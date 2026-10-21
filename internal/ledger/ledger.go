// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// Store owns the atomic decisions required for at-least-once delivery.
// Implementations must coordinate all writers that share the same ledger.
type Store interface {
	// Award deduplicates by payment ID and applies the monthly cap atomically.
	// A successful zero-point award still records the payment as processed.
	Award(ctx context.Context, requested Entry, monthlyCap int64) error
	// Reverse removes the original award exactly once, using its stored metadata.
	// It returns false only when the payment has no award.
	Reverse(ctx context.Context, paymentID string) (awardFound bool, err error)
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		if err := s.faults(op); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Award makes deduplication, cap calculation and insertion one ledger operation.
func (s *MemoryStore) Award(ctx context.Context, requested Entry, monthlyCap int64) error {
	// Simulated I/O and failures must not serialize unrelated members.
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	var used int64
	for _, e := range s.entries {
		if e.Kind != KindAward {
			continue
		}
		if e.PaymentID == requested.PaymentID {
			return nil
		}
		if e.MemberID == requested.MemberID && e.Month == requested.Month {
			used += e.Points
		}
	}
	requested.Kind = KindAward
	requested.Points = min(requested.Points, max(monthlyCap-used, 0))
	s.entries = append(s.entries, requested)
	return nil
}

// Reverse appends at most one reversal of the actual, possibly capped award.
func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) (bool, error) {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	var original Entry
	var found, reversed bool
	for _, e := range s.entries {
		if e.PaymentID != paymentID {
			continue
		}
		switch e.Kind {
		case KindAward:
			original, found = e, true
		case KindReversal:
			reversed = true
		}
	}
	if !found {
		return false, nil
	}
	if reversed {
		return true, nil
	}
	s.entries = append(s.entries, Entry{
		MemberID:  original.MemberID,
		PaymentID: original.PaymentID,
		Kind:      KindReversal,
		Points:    -original.Points,
		Month:     original.Month,
	})
	return true, nil
}

func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID && e.Month == month && e.Kind == KindAward {
			total += e.Points
		}
	}
	return total, nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
