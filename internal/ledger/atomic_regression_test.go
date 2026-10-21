package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAtomicAward_ConcurrentPaymentIdentityAndCap(t *testing.T) {
	t.Run("same payment is persisted once", func(t *testing.T) {
		store := NewMemoryStore(WithLatency(2 * time.Millisecond))
		requested := Entry{MemberID: "member", PaymentID: "payment", Kind: KindAward, Points: 1500, Month: "2026-10"}
		var calls []func() error
		for replica := 0; replica < 3; replica++ {
			for delivery := 0; delivery < 10; delivery++ {
				calls = append(calls, func() error {
					return store.Award(context.Background(), requested, 10_000)
				})
			}
		}
		runAtomicCalls(t, calls...)
		assertAtomicEntries(t, store, []Entry{requested})
		assertAtomicBalance(t, store, "member", 1500)
	})

	t.Run("three replicas share the same monthly cap", func(t *testing.T) {
		store := NewMemoryStore(WithLatency(2 * time.Millisecond))
		var calls []func() error
		for replica := 0; replica < 3; replica++ {
			for payment := 0; payment < 10; payment++ {
				requested := Entry{MemberID: "member", PaymentID: fmt.Sprintf("payment-%d", payment), Kind: KindAward, Points: 1000, Month: "2026-10"}
				calls = append(calls, func() error {
					return store.Award(context.Background(), requested, 2500)
				})
			}
		}
		runAtomicCalls(t, calls...)

		entries := atomicEntries(store)
		if len(entries) != 10 {
			t.Fatalf("persisted %d entries, want one for each of 10 payments, including zero awards", len(entries))
		}
		seen := make(map[string]bool)
		counts := make(map[int64]int)
		for _, entry := range entries {
			if seen[entry.PaymentID] {
				t.Errorf("payment %q was persisted more than once", entry.PaymentID)
			}
			seen[entry.PaymentID] = true
			if entry.MemberID != "member" || entry.Month != "2026-10" || entry.Kind != KindAward {
				t.Errorf("unexpected award metadata: %+v", entry)
			}
			counts[entry.Points]++
		}
		if counts[1000] != 2 || counts[500] != 1 || counts[0] != 7 || len(counts) != 3 {
			t.Errorf("award distribution = %v, want two full awards, one partial award, and seven zero awards", counts)
		}
		assertAtomicBalance(t, store, "member", 2500)
	})
}

func TestAtomicReverse_ConcurrentRedeliveryPreservesOriginalAwardAndConsumedCap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	seed := Entry{MemberID: "original-member", PaymentID: "seed", Kind: KindAward, Points: 9500, Month: "2026-09"}
	if err := store.Append(ctx, seed); err != nil {
		t.Fatal(err)
	}
	requested := Entry{MemberID: seed.MemberID, PaymentID: "partially-awarded", Kind: KindAward, Points: 1000, Month: seed.Month}
	if err := store.Award(ctx, requested, 10_000); err != nil {
		t.Fatal(err)
	}
	award := requested
	award.Points = 500
	assertAtomicEntries(t, store, []Entry{seed, award})

	var calls []func() error
	for delivery := 0; delivery < 30; delivery++ {
		calls = append(calls, func() error {
			found, err := store.Reverse(ctx, award.PaymentID)
			if err == nil && !found {
				return errors.New("existing award was reported missing")
			}
			return err
		})
	}
	runAtomicCalls(t, calls...)
	reversal := award
	reversal.Kind = KindReversal
	reversal.Points = -500
	assertAtomicEntries(t, store, []Entry{seed, award, reversal})
	assertAtomicBalance(t, store, seed.MemberID, 9500)

	thisMonth := Entry{MemberID: seed.MemberID, PaymentID: "after-reversal", Kind: KindAward, Points: 600, Month: seed.Month}
	if err := store.Award(ctx, thisMonth, 10_000); err != nil {
		t.Fatal(err)
	}
	thisMonth.Points = 0
	nextMonth := Entry{MemberID: seed.MemberID, PaymentID: "next-month", Kind: KindAward, Points: 600, Month: "2026-10"}
	if err := store.Award(ctx, nextMonth, 10_000); err != nil {
		t.Fatal(err)
	}
	assertAtomicEntries(t, store, []Entry{seed, award, reversal, thisMonth, nextMonth})
	assertAtomicBalance(t, store, seed.MemberID, 10_100)
	used, err := store.AwardedInMonth(ctx, seed.MemberID, seed.Month)
	if err != nil {
		t.Fatal(err)
	}
	if used != 10_000 {
		t.Errorf("consumed original-month cap = %d, want 10000 despite reversal", used)
	}
}

func TestAtomicMutations_FailuresAndCancellationHaveNoEffects(t *testing.T) {
	entry := Entry{MemberID: "member", PaymentID: "payment", Kind: KindAward, Points: 1200, Month: "2026-10"}
	operations := []struct {
		name string
		seed bool
		run  func(context.Context, *MemoryStore) error
	}{
		{"award", false, func(ctx context.Context, store *MemoryStore) error {
			return store.Award(ctx, entry, 10_000)
		}},
		{"reverse", true, func(ctx context.Context, store *MemoryStore) error {
			_, err := store.Reverse(ctx, entry.PaymentID)
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			t.Run("append fault is retryable without partial effects", func(t *testing.T) {
				var failNext atomic.Bool
				store := NewMemoryStore(WithFaults(func(op string) error {
					if op == "append" && failNext.CompareAndSwap(true, false) {
						return ErrUnavailable
					}
					return nil
				}))
				if operation.seed {
					if err := store.Append(context.Background(), entry); err != nil {
						t.Fatal(err)
					}
				}
				before := atomicEntries(store)
				failNext.Store(true)
				if err := operation.run(context.Background(), store); !errors.Is(err, ErrUnavailable) {
					t.Errorf("first attempt error = %v, want ErrUnavailable", err)
				}
				assertAtomicEntries(t, store, before)
				if err := operation.run(context.Background(), store); err != nil {
					t.Fatal(err)
				}
				want := []Entry{entry}
				if operation.seed {
					reversal := entry
					reversal.Kind = KindReversal
					reversal.Points = -entry.Points
					want = append(want, reversal)
				}
				assertAtomicEntries(t, store, want)
			})

			t.Run("already canceled context with zero latency", func(t *testing.T) {
				store := NewMemoryStore()
				if operation.seed {
					if err := store.Append(context.Background(), entry); err != nil {
						t.Fatal(err)
					}
				}
				before := atomicEntries(store)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := operation.run(ctx, store); !errors.Is(err, context.Canceled) {
					t.Errorf("error = %v, want context.Canceled", err)
				}
				assertAtomicEntries(t, store, before)
			})

			t.Run("cancellation while mutation lock is held", func(t *testing.T) {
				store := NewMemoryStore()
				if operation.seed {
					if err := store.Append(context.Background(), entry); err != nil {
						t.Fatal(err)
					}
				}
				before := atomicEntries(store)
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &atomicObservedContext{Context: base, checked: make(chan struct{})}
				store.mu.Lock()
				done := make(chan error, 1)
				go func() { done <- operation.run(ctx, store) }()
				select {
				case <-ctx.checked:
				case <-time.After(2 * time.Second):
					t.Error("round-trip context checks did not complete while mutation lock was held")
				}
				cancel()
				store.mu.Unlock()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("error after cancellation = %v, want context.Canceled", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("mutation did not return after lock was released")
				}
				assertAtomicEntries(t, store, before)
			})
		})
	}
}

// The second Err call captures the final round-trip check before signaling.
// Cancellation after this signal can only be caught by a subsequent check.
type atomicObservedContext struct {
	context.Context
	checks  atomic.Int32
	checked chan struct{}
}

func (ctx *atomicObservedContext) Err() error {
	err := ctx.Context.Err()
	if ctx.checks.Add(1) == 2 {
		close(ctx.checked)
	}
	return err
}

func TestAtomicAward_ZeroAwardStillReservesPaymentIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		requested int64
		cap       int64
	}{
		{"zero cap", 1000, 0},
		{"zero requested points", 0, 10_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryStore()
			requested := Entry{MemberID: "original-member", PaymentID: "payment", Kind: KindAward, Points: test.requested, Month: "2026-09"}
			if err := store.Award(ctx, requested, test.cap); err != nil {
				t.Fatal(err)
			}
			altered := requested
			altered.MemberID = "different-member"
			altered.Month = "2026-10"
			altered.Points = 500
			if err := store.Award(ctx, altered, 10_000); err != nil {
				t.Fatal(err)
			}
			original := requested
			original.Points = 0
			assertAtomicEntries(t, store, []Entry{original})
			for delivery := 0; delivery < 2; delivery++ {
				found, err := store.Reverse(ctx, requested.PaymentID)
				if err != nil || !found {
					t.Fatalf("reverse zero award = (%t, %v), want existing award without error", found, err)
				}
			}
			reversal := original
			reversal.Kind = KindReversal
			assertAtomicEntries(t, store, []Entry{original, reversal})
			assertAtomicBalance(t, store, original.MemberID, 0)
			assertAtomicBalance(t, store, altered.MemberID, 0)
		})
	}
}

func TestAtomicReverse_UnknownPaymentDoesNotCreateTombstone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	found, err := store.Reverse(ctx, "late-payment")
	if err != nil || found {
		t.Fatalf("unknown reversal = (%t, %v), want (false, nil)", found, err)
	}
	assertAtomicEntries(t, store, nil)
	award := Entry{MemberID: "member", PaymentID: "late-payment", Kind: KindAward, Points: 100, Month: "2026-10"}
	if err := store.Award(ctx, award, 10_000); err != nil {
		t.Fatal(err)
	}
	assertAtomicEntries(t, store, []Entry{award})
	assertAtomicBalance(t, store, award.MemberID, 100)
	found, err = store.Reverse(ctx, award.PaymentID)
	if err != nil || !found {
		t.Fatalf("reversal after settlement = (%t, %v), want (true, nil)", found, err)
	}
	reversal := award
	reversal.Kind = KindReversal
	reversal.Points = -award.Points
	assertAtomicEntries(t, store, []Entry{award, reversal})
	assertAtomicBalance(t, store, award.MemberID, 0)
}

func runAtomicCalls(t *testing.T, calls ...func() error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, len(calls))
	var ready, done sync.WaitGroup
	ready.Add(len(calls))
	done.Add(len(calls))
	for _, call := range calls {
		go func(call func() error) {
			defer done.Done()
			ready.Done()
			<-start
			errs <- call()
		}(call)
	}
	ready.Wait()
	close(start)
	done.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent operation failed: %v", err)
		}
	}
}

func atomicEntries(store *MemoryStore) []Entry {
	store.mu.Lock()
	defer store.mu.Unlock()
	return slices.Clone(store.entries)
}

func assertAtomicEntries(t *testing.T, store *MemoryStore, want []Entry) {
	t.Helper()
	if got := atomicEntries(store); !slices.Equal(got, want) {
		t.Errorf("entries = %+v, want %+v", got, want)
	}
}

func assertAtomicBalance(t *testing.T, store *MemoryStore, memberID string, want int64) {
	t.Helper()
	got, err := store.Balance(context.Background(), memberID)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("member %q balance = %d, want %d", memberID, got, want)
	}
}
