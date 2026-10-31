package chaos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// proxy impairs real PostgreSQL TCP traffic. Control/verification connections
// bypass the proxy.
type proxy struct {
	listener      net.Listener
	target        string
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	connections   map[net.Conn]bool
	uploadDelay   time.Duration
	downloadDelay time.Duration
	partition     bool
	wg            sync.WaitGroup
}

func newProxy(ctx context.Context, dsn string) (*proxy, string, error) {
	pc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, "", err
	}
	if net.ParseIP(pc.Host) == nil && pc.Host != "localhost" {
		return nil, "", fmt.Errorf("chaos proxy requires a numeric TCP host or localhost")
	}
	// Keep authentication intact, but use plain local TCP. SSL cannot
	// retain hostname verification when the destination is replaced by a proxy.
	if pc.TLSConfig != nil {
		return nil, "", fmt.Errorf("chaos proxy requires sslmode=disable")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", err
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &proxy{listener: ln, target: net.JoinHostPort(pc.Host, fmt.Sprint(pc.Port)), ctx: ctx, cancel: cancel, connections: make(map[net.Conn]bool)}
	addr := ln.Addr().(*net.TCPAddr)
	// ParseConfig's ConnString preserves its original input, not modified fields.
	workerDSN := dsn + fmt.Sprintf(" host=127.0.0.1 port=%d sslmode=disable", addr.Port)
	// URL DSNs cannot have keyword parameters appended; translate through URL.
	if len(dsn) >= 11 && (dsn[:11] == "postgres://" || len(dsn) >= 13 && dsn[:13] == "postgresql://") {
		workerDSN, err = proxyURL(dsn, addr.Port)
		if err != nil {
			_ = ln.Close()
			cancel()
			return nil, "", err
		}
	}
	p.wg.Go(func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			blocked := p.partition
			p.mu.Unlock()
			if blocked {
				_ = client.Close()
				continue
			}
			p.wg.Go(func() { p.serve(client) })
		}
	})
	return p, workerDSN, nil
}

func (p *proxy) serve(client net.Conn) {
	defer client.Close()
	server, err := (&net.Dialer{Timeout: time.Second}).DialContext(p.ctx, "tcp", p.target)
	if err != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.partition || p.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.connections[client], p.connections[server] = true, true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, client); delete(p.connections, server); p.mu.Unlock() }()
	done := make(chan struct{})
	go func() { p.copy(server, client, true); _ = server.Close(); _ = client.Close(); close(done) }()
	p.copy(client, server, false)
	_ = server.Close()
	_ = client.Close()
	<-done
}

func (p *proxy) copy(dst net.Conn, src net.Conn, upload bool) {
	buf := make([]byte, 4096)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			delay := p.downloadDelay
			if upload {
				delay = p.uploadDelay
			}
			p.mu.Unlock()
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-p.ctx.Done():
					timer.Stop()
					return
				}
			}
			if _, writeErr := io.Copy(dst, bytes.NewReader(buf[:n])); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *proxy) set(uploadDelay, downloadDelay time.Duration, partition, reset bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uploadDelay, p.downloadDelay, p.partition = uploadDelay, downloadDelay, partition
	if partition || reset {
		for conn := range p.connections {
			_ = conn.Close()
		}
	}
}
func (p *proxy) close() {
	p.cancel()
	_ = p.listener.Close()
	p.set(0, 0, true, true)
	p.wg.Wait()
}
