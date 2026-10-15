package sqlq

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

// The queue's schema is created and changed by goose migrations, one directory per database under
// migrations/. 00001_initial.sql holds the whole schema as it was when migrations began; each later
// change is a new numbered file. Never edit an applied migration.
//
//go:embed migrations
var migrationFiles embed.FS

// schemaMigrationsTable records which migrations have been applied.
const schemaMigrationsTable = "sqlq_schema_migrations"

// postgresMigrationLockID is the PostgreSQL advisory lock taken while migrating: the CRC-64 (ECMA)
// of schemaMigrationsTable, halved to be positive. It is the queue's own, so that applications using
// goose for their schema don't wait on it.
const postgresMigrationLockID = 7446757463214981083

// migrate applies the migrations in migrations/dir that db lacks. A run that finds nothing pending
// takes no lock. If applying fails because another process applied the same migrations first
// (SQLite has no lock across processes), there is nothing left to do and migrate succeeds.
func migrate(ctx context.Context, db *sql.DB, dialect database.Dialect, dir string, opts ...goose.ProviderOption) error {
	fsys, err := fs.Sub(migrationFiles, "migrations/"+dir)
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(dialect, db, fsys, append([]goose.ProviderOption{
		goose.WithTableName(schemaMigrationsTable),
		// Applications may register their own Go migrations with goose.
		goose.WithDisableGlobalRegistry(true),
	}, opts...)...)
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}
	if pending, err := provider.HasPending(ctx); err == nil && !pending {
		return nil
	}
	if _, err := provider.Up(ctx); err != nil {
		if pending, pendingErr := provider.HasPending(ctx); pendingErr == nil && !pending {
			return nil
		}
		return fmt.Errorf("failed to migrate: %w", err)
	}
	return nil
}
