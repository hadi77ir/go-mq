package mq

import (
	"context"
	"errors"
	"sync"
	"time"
)

// PoolConn is the minimal interface required for pooled connections.
type PoolConn interface {
	Close() error
}

// PoolOptions configure ConnectionPool behavior.
type PoolOptions struct {
	MaxSize     int
	IdleTimeout time.Duration
}

// ConnectionFactory creates new connections that satisfy PoolConn.
type ConnectionFactory[T PoolConn] func(ctx context.Context) (T, error)

// ConnectionPool is a simple generic pool used by broker implementations.
type ConnectionPool[T PoolConn] struct {
	factory ConnectionFactory[T]
	opts    PoolOptions

	mu        sync.Mutex
	idle      chan pooledItem[T]
	total     int
	closed    bool
	closeOnce sync.Once
}

type pooledItem[T PoolConn] struct {
	conn     T
	lastUsed time.Time
}

// NewConnectionPool constructs a pool with the provided factory and options.
func NewConnectionPool[T PoolConn](factory ConnectionFactory[T], opts PoolOptions) (*ConnectionPool[T], error) {
	if opts.MaxSize <= 0 {
		return nil, errors.New("pool: MaxSize must be greater than zero")
	}

	idleSize := opts.MaxSize
	if idleSize < 1 {
		idleSize = 1
	}

	return &ConnectionPool[T]{
		factory: factory,
		opts:    opts,
		idle:    make(chan pooledItem[T], idleSize),
	}, nil
}

// Get acquires a pooled connection creating a new one if necessary.
func (p *ConnectionPool[T]) Get(ctx context.Context) (T, error) {
	var zero T

	for {
		select {
		case item, ok := <-p.idle:
			if !ok {
				return zero, errors.New("pool: closed")
			}
			if p.opts.IdleTimeout > 0 && time.Since(item.lastUsed) > p.opts.IdleTimeout {
				_ = item.conn.Close()
				p.decrementTotal()
				continue
			}
			return item.conn, nil
		default:
		}

		conn, err := p.tryCreate(ctx)
		if err != nil {
			if errors.Is(err, errPoolAtCapacity) {
				select {
				case item, ok := <-p.idle:
					if !ok {
						return zero, errors.New("pool: closed")
					}
					return item.conn, nil
				case <-ctx.Done():
					return zero, ctx.Err()
				}
			}
			return zero, err
		}
		return conn, nil
	}
}

var errPoolAtCapacity = errors.New("pool: at capacity")

func (p *ConnectionPool[T]) tryCreate(ctx context.Context) (T, error) {
	var zero T

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return zero, errors.New("pool: closed")
	}

	if p.total >= p.opts.MaxSize {
		return zero, errPoolAtCapacity
	}

	conn, err := p.factory(ctx)
	if err != nil {
		return zero, err
	}

	p.total++
	return conn, nil
}

// Release returns the connection to the pool. If the pool is closed, the connection is closed.
func (p *ConnectionPool[T]) Release(conn T) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return conn.Close()
	}

	select {
	case p.idle <- pooledItem[T]{conn: conn, lastUsed: time.Now()}:
		return nil
	default:
		p.total--
		return conn.Close()
	}
}

// Close drains and closes the pool.
func (p *ConnectionPool[T]) Close() error {
	var closeErr error

	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.idle)
		p.mu.Unlock()

		for item := range p.idle {
			if err := item.conn.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
	})

	return closeErr
}

func (p *ConnectionPool[T]) decrementTotal() {
	p.mu.Lock()
	p.total--
	p.mu.Unlock()
}
