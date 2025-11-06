package mq

import (
	"context"
	"testing"
	"time"
)

type dummyConn struct {
	closed bool
}

func (d *dummyConn) Close() error {
	d.closed = true
	return nil
}

func TestConnectionPoolAcquireRelease(t *testing.T) {
	ctx := context.Background()
	factoryCount := 0
	pool, err := NewConnectionPool(func(ctx context.Context) (*dummyConn, error) {
		factoryCount++
		return &dummyConn{}, nil
	}, PoolOptions{MaxSize: 2, IdleTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewConnectionPool: %v", err)
	}
	defer pool.Close() // nolint:errcheck

	conn1, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Get conn1: %v", err)
	}

	if err := pool.Release(conn1); err != nil {
		t.Fatalf("Release conn1: %v", err)
	}

	conn2, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Get conn2: %v", err)
	}
	if conn1 != conn2 {
		t.Fatalf("expected pooled connection to be reused")
	}

	time.Sleep(2 * time.Millisecond)

	if err := pool.Release(conn2); err != nil {
		t.Fatalf("Release conn2: %v", err)
	}

	time.Sleep(75 * time.Millisecond)

	conn3, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Get conn3: %v", err)
	}

	if conn2 == conn3 && factoryCount > 1 {
		t.Fatalf("expected idle connection to be expired")
	}

	if err := pool.Release(conn3); err != nil {
		t.Fatalf("Release conn3: %v", err)
	}
}
