package chaos

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Side audits model a handler that reads the queue's database and then writes another SQLite
// database attached to its connection, such as a notifier recording what it sent. When another
// connection commits to the main database in between, the processed mark fails on a stale snapshot
// even though the transaction wrote. The queue must then retry the handler, never mark the job in a
// fresh transaction: that would drop the attached write. Each side-audited task must leave exactly
// one side row, and other tasks none.

// sideAudited reports whether task is side audited. One task in sixteen is: on SQLite, every commit
// to the main database fails each side audit whose read preceded it, so they are retried often, and
// more of them would starve every other writer under the shortest lock timeouts.
func sideAudited(c Config, task int) bool {
	return c.SideDB != "" && (task+int(c.Seed%16))%16 == 0
}

// createSideDB creates the side database and its table.
func createSideDB(ctx context.Context, c Config) error {
	db, err := sql.Open("sqlite3", "file:"+c.SideDB+"?_journal_mode=WAL")
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE chaos_side_effects (task INTEGER NOT NULL)`)
	return errors.Join(err, db.Close())
}

// openSideDB opens the main database for a worker's side-audit queue, with the side database
// attached to every connection.
func openSideDB(c Config) *sql.DB {
	db := sql.OpenDB(attachingConnector{dsn: c.DSN, side: c.SideDB})
	db.SetMaxOpenConns(c.Pool)
	db.SetMaxIdleConns(c.Pool)
	return db
}

type attachingConnector struct{ dsn, side string }

func (a attachingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := a.Driver().Open(a.dsn)
	if err != nil {
		return nil, err
	}
	if _, err := conn.(*observedConn).ExecContext(ctx, `ATTACH DATABASE ? AS side`, []driver.NamedValue{{Ordinal: 1, Value: a.side}}); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	return conn, nil
}

func (attachingConnector) Driver() driver.Driver {
	return observedDriver{base: &sqlite3.SQLiteDriver{}}
}

// verifySideEffects checks that each task's side audit committed its write exactly once. A task in
// interrupted may have none: the worker of its last side audit was killed after the write, and no
// commit of that write returned. Thus, the kill possibly came between the commits of the main and
// the side database (see commitWatch). No queue can keep the two together across a crash in WAL mode.
func verifySideEffects(ctx context.Context, c Config, interrupted map[int]bool) error {
	if c.SideDB == "" {
		return nil
	}
	db, err := sql.Open("sqlite3", "file:"+c.SideDB)
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT task, COUNT(*) FROM chaos_side_effects GROUP BY task`)
	if err != nil {
		return err
	}
	defer rows.Close()
	counts := make([]int, len(c.Tasks))
	for rows.Next() {
		var task, count int
		if err := rows.Scan(&task, &count); err != nil {
			return err
		}
		if task < 0 || task >= len(c.Tasks) {
			return fmt.Errorf("side audit wrote unknown task %d", task)
		}
		counts[task] = count
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for task, count := range counts {
		if want := map[bool]int{true: 1}[sideAudited(c, task)]; count != want && !(count == 0 && interrupted[task]) {
			return fmt.Errorf("side audit of task %d committed %d side writes; want %d", task, count, want)
		}
	}
	return nil
}

// checkAuditFailure checks a recorded failure of an audit or side audit in a fault-free run.
// Audits only read, so only timeouts may fail them. Side audits read and then write: like any such
// SQLite handler, they may also fail on a lock wait that timed out, or on a stale snapshot when
// their transaction is marked processed, and are retried.
func checkAuditFailure(kind string, jobID int64, reason string) error {
	if strings.Contains(reason, context.DeadlineExceeded.Error()) || strings.Contains(reason, context.Canceled.Error()) {
		return nil
	}
	if kind == sideAuditJobType && strings.Contains(reason, "database is locked") {
		return nil
	}
	return fmt.Errorf("%s job %d failed without faults for a reason other than a timeout: %s", kind, jobID, reason)
}

// commitWatch finds side audits whose worker may have been killed during their commit. The commit
// writes the main and then the side database: SQLite does not commit attached databases atomically
// in WAL mode, so a kill between the two leaves the job processed without its side row. Events are
// observed in order of arrival, which is not their order in time.
//
// The handler emits side_audit before the queue commits. The driver wrapper of the worker emits
// side_committed for the task when that commit returns (see observedTx). A worker runs many
// goroutines at the same time, so its other events do not show that a commit returned.
type commitWatch struct {
	lastSideAudit map[int]Event // By task.
	committed     map[int]bool  // By task: a commit of its side write returned.
	killedAt      map[int]time.Time
}

func newCommitWatch() *commitWatch {
	return &commitWatch{lastSideAudit: make(map[int]Event), committed: make(map[int]bool), killedAt: make(map[int]time.Time)}
}

func (w *commitWatch) observe(e Event) {
	switch e.Kind {
	case "kill", "kill_at_write", "kill_at_stop":
		w.killedAt[e.Worker] = e.At
	case "side_committed":
		w.committed[e.Task] = true
	case "side_audit":
		if !e.At.Before(w.lastSideAudit[e.Task].At) {
			w.lastSideAudit[e.Task] = e
		}
	}
}

// interrupted returns the tasks with no returned commit of a side write, whose last side audit's
// worker was killed within window after it: the commit possibly did not finish. A task with a
// returned commit is never in the result. If its side row is missing, the queue lost the write.
func (w *commitWatch) interrupted(window time.Duration) map[int]bool {
	tasks := make(map[int]bool)
	for task, e := range w.lastSideAudit {
		killed, ok := w.killedAt[e.Worker]
		if ok && !w.committed[task] && !killed.Before(e.At) && killed.Sub(e.At) <= window {
			tasks[task] = true
		}
	}
	return tasks
}
