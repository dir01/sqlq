package chaos

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestProxyDelayPartitionAndHealing(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		for {
			conn, err := backend.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	p, _, err := newProxy(t.Context(), "postgres://postgres@"+backend.Addr().String()+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	dial := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp", p.listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	p.set(20*time.Millisecond, 30*time.Millisecond, false, false)
	conn := dial()
	defer conn.Close()
	start := time.Now()
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hello" || time.Since(start) < 50*time.Millisecond {
		t.Fatalf("proxy did not preserve bytes/delay: %q %s", buf, time.Since(start))
	}
	p.set(0, 0, true, true)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("partition did not close established connection")
	}
	blocked := dial()
	defer blocked.Close()
	if _, err := blocked.Read(buf); err == nil {
		t.Fatal("partition accepted traffic")
	}
	p.set(0, 0, false, true)
	healed := dial()
	defer healed.Close()
	if _, err := healed.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(healed, buf); err != nil {
		t.Fatal(err)
	}
}
