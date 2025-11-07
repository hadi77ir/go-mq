package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hadi77ir/go-mq"
)

func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	policy := mq.RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     20 * time.Millisecond,
		Multiplier:     2,
	}

	attempts := 0
	err := mq.Retry(context.Background(), policy, func(context.Context) error {
		attempts++
		if attempts < 3 {
			return mq.ErrNoConnection
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected retry to eventually succeed, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetryStopsOnConfigurationError(t *testing.T) {
	policy := mq.RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}

	attempts := 0
	err := mq.Retry(context.Background(), policy, func(context.Context) error {
		attempts++
		return errors.Join(mq.ErrConfiguration, errors.New("invalid"))
	})
	if err == nil || !errors.Is(err, mq.ErrConfiguration) {
		t.Fatalf("expected configuration error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected no retries for configuration issues, got %d attempts", attempts)
	}
}

func TestRetryHonorsMaxAttempts(t *testing.T) {
	policy := mq.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}

	attempts := 0
	err := mq.Retry(context.Background(), policy, func(context.Context) error {
		attempts++
		return mq.ErrNoConnection
	})
	if err == nil || !errors.Is(err, mq.ErrNoConnection) {
		t.Fatalf("expected no connection error, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}
