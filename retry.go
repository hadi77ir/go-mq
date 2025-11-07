package mq

import (
	"context"
	"errors"
	"math"
	"time"
)

// RetryPolicy configures exponential backoff retries for broker operations.
type RetryPolicy struct {
	// MaxAttempts limits how many attempts are made before failing. Zero or negative means infinite retries.
	MaxAttempts int
	// InitialBackoff is the wait duration before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff.
	MaxBackoff time.Duration
	// Multiplier scales the backoff after each attempt (>= 1).
	Multiplier float64
}

// normalized applies defaults ensuring the policy is usable.
func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts < 0 {
		p.MaxAttempts = 0
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 200 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 5 * time.Second
	}
	if p.Multiplier <= 1 {
		p.Multiplier = 2
	}
	return p
}

// Normalized returns the policy with defaults applied. Exported for adapter configuration.
func (p RetryPolicy) Normalized() RetryPolicy {
	return p.normalized()
}

func (p RetryPolicy) delay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	backoff := float64(p.InitialBackoff) * math.Pow(p.Multiplier, float64(attempt))
	if backoff > float64(p.MaxBackoff) {
		return p.MaxBackoff
	}
	return time.Duration(backoff)
}

// Retry executes the provided operation until it succeeds, the context is canceled,
// or the policy's attempt limit is reached. Non-retryable errors (`ErrConfiguration`,
// `ErrNotSupported`) return immediately.
func Retry(ctx context.Context, policy RetryPolicy, operation func(ctx context.Context) error) error {
	policy = policy.normalized()
	for attempt := 0; ; attempt++ {
		err := operation(ctx)
		if err == nil {
			return nil
		}

		if !isRetryableError(err) {
			return err
		}

		// Respect attempt budget.
		if policy.MaxAttempts > 0 && attempt+1 >= policy.MaxAttempts {
			return err
		}

		delay := policy.delay(attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func isRetryableError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrConfiguration) || errors.Is(err, ErrNotSupported) {
		return false
	}
	return true
}
