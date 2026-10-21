package rewards_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

// All messages are prepared before concurrent delivery: newMessage may call
// testing.Fatal, and outcome checks belong in the test goroutine after join.
func TestRegression_ConcurrentRedeliveryAcrossThreeReplicas(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(2 * time.Millisecond))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}

	for _, kind := range []string{events.TypeSettled, events.TypeReversed} {
		messages := make([]*fakeMessage, 30)
		calls := make([]func(), len(messages))
		for i := range messages {
			ev := settled("republished_payment", "member", 1800)
			ev.Type = kind
			ev.EventID = fmt.Sprintf("%s_delivery_%d", kind, i)
			messages[i] = newMessage(t, ev)
			handler, message := replicas[i%len(replicas)], messages[i]
			calls[i] = func() { handler.Handle(context.Background(), message) }
		}
		concurrently(calls...)
		for _, message := range messages {
			regressionWantOutcome(t, message, acked)
		}
		want := int64(1800)
		if kind == events.TypeReversed {
			want = 0
		}
		regressionWantBalance(t, store, "member", want)
	}
}

func TestRegression_GlobalPaymentIdentityAndOriginalReversalMetadata(t *testing.T) {
	store := ledger.NewMemoryStore()
	handler := newHandler(store)
	deliver(t, handler, settled("first", "original_member", 9500))
	deliver(t, handler, settled("capped", "original_member", 1000))

	duplicate := settled("capped", "other_member", 9000)
	duplicate.EventID = "different_settlement_event"
	duplicate.OccurredAt = oct.AddDate(0, 1, 0)
	deliver(t, handler, duplicate)
	regressionWantBalance(t, store, "original_member", 10_000)
	regressionWantBalance(t, store, "other_member", 0)
	regressionWantAwarded(t, store, "other_member", "2026-11", 0)

	reversal := reversed("capped", "other_member", 9000)
	reversal.OccurredAt = oct.AddDate(0, 1, 0)
	deliver(t, handler, reversal)
	reversal.EventID = "another_reversal_event"
	reversal.MemberID = "third_member"
	reversal.AmountCents = 1
	reversal.OccurredAt = oct.AddDate(0, 2, 0)
	deliver(t, handler, reversal)
	regressionWantBalance(t, store, "original_member", 9500)
	regressionWantBalance(t, store, "other_member", 0)
	regressionWantBalance(t, store, "third_member", 0)
	regressionWantAwarded(t, store, "original_member", "2026-10", 10_000)

	// Neither a conflicting duplicate nor another member's cap can consume this
	// member's monthly allowance.
	deliver(t, handler, settled("independent", "other_member", 10_000))
	regressionWantBalance(t, store, "other_member", 10_000)
}

func TestRegression_ZeroPointSettlementsRemainDeduplicated(t *testing.T) {
	t.Run("fractional dollar", func(t *testing.T) {
		store := ledger.NewMemoryStore()
		handler := newHandler(store)
		fractional := settled("fractional", "member", 1)
		fractional.AmountCents = 99
		deliver(t, handler, fractional)
		regressionWantBalance(t, store, "member", 0)

		duplicate := fractional
		duplicate.EventID = "fractional_republished"
		duplicate.AmountCents = 10_000 * 100
		duplicate.OccurredAt = oct.AddDate(0, 1, 0)
		deliver(t, handler, duplicate)
		deliver(t, handler, reversed("fractional", "member", 10_000))
		regressionWantBalance(t, store, "member", 0)
		regressionWantAwarded(t, store, "member", "2026-11", 0)

		deliver(t, handler, settled("full_allowance", "member", 10_000))
		regressionWantBalance(t, store, "member", 10_000)
	})

	t.Run("exhausted cap and reversal do not restore allowance", func(t *testing.T) {
		store := ledger.NewMemoryStore()
		handler := newHandler(store)
		deliver(t, handler, settled("fills_cap", "member", 10_000))
		deliver(t, handler, settled("zero_award", "member", 1000))
		deliver(t, handler, reversed("fills_cap", "member", 10_000))
		regressionWantBalance(t, store, "member", 0)

		// A different payment in the original month must still earn zero.
		deliver(t, handler, settled("after_reversal", "member", 1000))
		regressionWantBalance(t, store, "member", 0)
		regressionWantAwarded(t, store, "member", "2026-10", 10_000)

		duplicate := settled("zero_award", "member", 1000)
		duplicate.EventID = "zero_republished_next_month"
		duplicate.OccurredAt = oct.AddDate(0, 1, 0)
		deliver(t, handler, duplicate)
		deliver(t, handler, reversed("zero_award", "member", 1000))
		regressionWantBalance(t, store, "member", 0)
		regressionWantAwarded(t, store, "member", "2026-11", 0)

		nextMonth := settled("fresh_next_month", "member", 10_000)
		nextMonth.OccurredAt = oct.AddDate(0, 1, 0)
		deliver(t, handler, nextMonth)
		regressionWantBalance(t, store, "member", 10_000)
	})
}

func TestRegression_WrappedLedgerFailureNacksAndRetryAppliesOnce(t *testing.T) {
	for _, kind := range []string{events.TypeSettled, events.TypeReversed} {
		t.Run(kind, func(t *testing.T) {
			var failNext atomic.Bool
			store := ledger.NewMemoryStore(ledger.WithFaults(func(operation string) error {
				if operation == "append" && failNext.Swap(false) {
					return fmt.Errorf("temporary connection failure: %w", ledger.ErrUnavailable)
				}
				return nil
			}))
			handler := newHandler(store)
			ev := settled("retry_payment", "member", 1200)
			beforeFailure := int64(0)
			if kind == events.TypeReversed {
				deliver(t, handler, ev)
				ev = reversed("retry_payment", "member", 1200)
				beforeFailure = 1200
			}

			failNext.Store(true)
			failed := newMessage(t, ev)
			handler.Handle(context.Background(), failed)
			regressionWantOutcome(t, failed, nacked)
			regressionWantBalance(t, store, "member", beforeFailure)
			regressionWantAwarded(t, store, "member", "2026-10", beforeFailure)

			ev.EventID = "retried_with_another_event_id"
			deliver(t, handler, ev)
			ev.EventID = "republished_after_success"
			deliver(t, handler, ev)
			want := int64(1200)
			if kind == events.TypeReversed {
				want = 0
			}
			regressionWantBalance(t, store, "member", want)
			regressionWantAwarded(t, store, "member", "2026-10", 1200)
		})
	}
}

func TestRegression_CanceledLedgerOperationNacksWithoutMutation(t *testing.T) {
	for _, kind := range []string{events.TypeSettled, events.TypeReversed} {
		t.Run(kind, func(t *testing.T) {
			store := ledger.NewMemoryStore()
			handler := newHandler(store)
			ev := settled("canceled_payment", "member", 1200)
			beforeCancellation := int64(0)
			if kind == events.TypeReversed {
				deliver(t, handler, ev)
				ev = reversed("canceled_payment", "member", 1200)
				beforeCancellation = 1200
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			message := newMessage(t, ev)
			handler.Handle(ctx, message)
			regressionWantOutcome(t, message, nacked)
			regressionWantBalance(t, store, "member", beforeCancellation)
			regressionWantAwarded(t, store, "member", "2026-10", beforeCancellation)

			deliver(t, handler, ev)
			want := int64(1200)
			if kind == events.TypeReversed {
				want = 0
			}
			regressionWantBalance(t, store, "member", want)
		})
	}
}

func TestRegression_DeadLetterSuccessPreservesPayloadAndReason(t *testing.T) {
	invalid := []struct {
		name string
		data string
	}{
		{"malformed JSON", `{"type":`},
		{"missing member", `{"type":"payment.settled","payment_id":"p","amount_cents":100,"occurred_at":"2026-10-05T12:00:00Z"}`},
		{"unknown type", `{"type":"payment.unknown","payment_id":"p","member_id":"m","amount_cents":100,"occurred_at":"2026-10-05T12:00:00Z"}`},
		{"nonpositive amount", `{"type":"payment.settled","payment_id":"p","member_id":"m","amount_cents":0,"occurred_at":"2026-10-05T12:00:00Z"}`},
		{"missing time", `{"type":"payment.settled","payment_id":"p","member_id":"m","amount_cents":100}`},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			store := &regressionStoreProbe{Store: ledger.NewMemoryStore()}
			payload := []byte(test.data)
			_, invalidErr := events.Decode(payload)
			if !errors.Is(invalidErr, events.ErrInvalid) {
				t.Fatalf("test payload is not invalid: %v", invalidErr)
			}
			var published []byte
			var attributes map[string]string
			var calls int
			publisher := regressionPublisher(func(_ context.Context, data []byte, attrs map[string]string) error {
				calls++
				published = bytes.Clone(data)
				attributes = attrs
				return nil
			})
			handler := rewards.NewHandler(store, rewards.DefaultPolicy, publisher, newHandlerLogger())
			message := &fakeMessage{id: "invalid", data: payload, attempt: 1}
			handler.Handle(context.Background(), message)
			regressionWantOutcome(t, message, acked)
			if calls != 1 || !bytes.Equal(published, payload) {
				t.Fatalf("dead-letter calls = %d, payload = %q; want one call with %q", calls, published, payload)
			}
			if attributes["reason"] != invalidErr.Error() {
				t.Fatalf("dead-letter reason = %q, want %q", attributes["reason"], invalidErr.Error())
			}
			if got := store.calls.Load(); got != 0 {
				t.Fatalf("invalid delivery invoked the ledger %d times", got)
			}
		})
	}
}

func TestRegression_DeadLetterFailureNacksAndCanBeRetried(t *testing.T) {
	store := &regressionStoreProbe{Store: ledger.NewMemoryStore()}
	var calls int
	publisher := regressionPublisher(func(_ context.Context, _ []byte, _ map[string]string) error {
		calls++
		if calls == 1 {
			return errors.New("dead-letter service unavailable")
		}
		return nil
	})
	handler := rewards.NewHandler(store, rewards.DefaultPolicy, publisher, newHandlerLogger())
	payload := []byte("invalid JSON")
	for attempt, want := range []outcome{nacked, acked} {
		message := &fakeMessage{id: "invalid", data: payload, attempt: attempt + 1}
		handler.Handle(context.Background(), message)
		regressionWantOutcome(t, message, want)
	}
	if calls != 2 {
		t.Fatalf("dead-letter calls = %d, want 2", calls)
	}
	if got := store.calls.Load(); got != 0 {
		t.Fatalf("invalid delivery invoked the ledger %d times", got)
	}
}

func TestRegression_CancellationDuringDeadLetterPublishNacks(t *testing.T) {
	store := &regressionStoreProbe{Store: ledger.NewMemoryStore()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	done := make(chan struct{})
	var publisherError error
	publisher := regressionPublisher(func(publishContext context.Context, _ []byte, _ map[string]string) error {
		close(entered)
		<-publishContext.Done()
		publisherError = publishContext.Err()
		return publisherError
	})
	handler := rewards.NewHandler(store, rewards.DefaultPolicy, publisher, newHandlerLogger())
	message := &fakeMessage{id: "invalid", data: []byte("invalid JSON"), attempt: 1}
	go func() {
		defer close(done)
		handler.Handle(ctx, message)
	}()
	regressionWait(t, entered, "dead-letter publisher to start")
	cancel()
	regressionWait(t, done, "canceled delivery to finish")
	regressionWantOutcome(t, message, nacked)
	if !errors.Is(publisherError, context.Canceled) {
		t.Fatalf("publisher error = %v, want context.Canceled", publisherError)
	}
	if got := store.calls.Load(); got != 0 {
		t.Fatalf("invalid delivery invoked the ledger %d times", got)
	}
}

func TestRegression_UnknownReversalLogsAndDoesNotCreateTombstone(t *testing.T) {
	store := ledger.NewMemoryStore()
	var logs bytes.Buffer
	var deadLetters int
	publisher := regressionPublisher(func(context.Context, []byte, map[string]string) error {
		deadLetters++
		return nil
	})
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	handler := rewards.NewHandler(store, rewards.DefaultPolicy, publisher, logger)
	message := newMessage(t, reversed("late_payment", "member", 1200))
	handler.Handle(context.Background(), message)
	regressionWantOutcome(t, message, acked)
	regressionWantBalance(t, store, "member", 0)
	if deadLetters != 0 {
		t.Fatalf("unknown reversal was dead-lettered %d times", deadLetters)
	}
	if text := logs.String(); !strings.Contains(text, "late_payment") || !strings.Contains(strings.ToLower(text), "unknown") {
		t.Fatalf("unknown reversal log does not identify the unknown payment: %q", text)
	}

	// An unknown reversal is acknowledged without reserving the payment ID or
	// recording a completed reversal for a settlement that has not arrived.
	deliver(t, handler, settled("late_payment", "member", 1200))
	regressionWantBalance(t, store, "member", 1200)
	deliver(t, handler, reversed("late_payment", "member", 1200))
	regressionWantBalance(t, store, "member", 0)
}

func TestRegression_UTCOffsetsYearAndWholeDollarTruncation(t *testing.T) {
	store := ledger.NewMemoryStore()
	handler := newHandler(store)
	payments := []struct {
		id    string
		at    string
		cents int64
	}{
		{"january_offset", "2026-02-01T00:30:00+03:00", 9000 * 100},
		{"january_cap", "2026-01-31T23:45:00Z", 3000 * 100},
		{"february_offset", "2026-01-31T23:30:00-03:00", 9000 * 100},
		{"february_cap", "2026-02-01T02:45:00Z", 3000 * 100},
		{"next_year_fraction", "2027-01-01T00:00:00Z", 199},
	}
	for _, payment := range payments {
		at, err := time.Parse(time.RFC3339, payment.at)
		if err != nil {
			t.Fatal(err)
		}
		ev := settled(payment.id, "member", 1)
		ev.AmountCents = payment.cents
		ev.OccurredAt = at
		deliver(t, handler, ev)
	}
	regressionWantBalance(t, store, "member", 20_001)
	regressionWantAwarded(t, store, "member", "2026-01", 10_000)
	regressionWantAwarded(t, store, "member", "2026-02", 10_000)
	regressionWantAwarded(t, store, "member", "2027-01", 1)
}

type regressionPublisher func(context.Context, []byte, map[string]string) error

func (publisher regressionPublisher) Publish(ctx context.Context, data []byte, attributes map[string]string) error {
	return publisher(ctx, data, attributes)
}

type regressionStoreProbe struct {
	ledger.Store
	calls atomic.Int32
}

func (store *regressionStoreProbe) Award(ctx context.Context, entry ledger.Entry, cap int64) error {
	store.calls.Add(1)
	return store.Store.Award(ctx, entry, cap)
}

func (store *regressionStoreProbe) Reverse(ctx context.Context, paymentID string) (bool, error) {
	store.calls.Add(1)
	return store.Store.Reverse(ctx, paymentID)
}

func (store *regressionStoreProbe) Balance(ctx context.Context, memberID string) (int64, error) {
	store.calls.Add(1)
	return store.Store.Balance(ctx, memberID)
}

func regressionWantOutcome(t *testing.T, message *fakeMessage, want outcome) {
	t.Helper()
	if got := message.outcome(t); got != want {
		t.Fatalf("message %s outcome = %v, want %v", message.ID(), got, want)
	}
}

func regressionWantBalance(t *testing.T, store ledger.Store, member string, want int64) {
	t.Helper()
	if got := balance(t, store, member); got != want {
		t.Fatalf("member %s balance = %d, want %d", member, got, want)
	}
}

func regressionWantAwarded(t *testing.T, store *ledger.MemoryStore, member, month string, want int64) {
	t.Helper()
	got, err := store.AwardedInMonth(context.Background(), member, month)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("member %s awarded in %s = %d, want %d", member, month, got, want)
	}
}

func regressionWait(t *testing.T, ready <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestRegression_ConcurrentRedeliveryAfterLedgerFailures(t *testing.T) {
	for _, kind := range []string{events.TypeSettled, events.TypeReversed} {
		t.Run(kind, func(t *testing.T) {
			var failures atomic.Int32
			store := ledger.NewMemoryStore(ledger.WithLatency(2*time.Millisecond), ledger.WithFaults(func(operation string) error {
				if operation == "append" && failures.Add(-1) >= 0 {
					return ledger.ErrUnavailable
				}
				return nil
			}))
			replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}
			if kind == events.TypeReversed {
				deliver(t, replicas[0], settled("faulted_payment", "member", 1200))
			}
			failures.Store(6)
			messages := make([]*fakeMessage, 30)
			calls := make([]func(), len(messages))
			for i := range messages {
				ev := settled("faulted_payment", "member", 1200)
				ev.Type = kind
				ev.EventID = fmt.Sprintf("%s_faulted_delivery_%d", kind, i)
				messages[i] = newMessage(t, ev)
				handler, message := replicas[i%len(replicas)], messages[i]
				calls[i] = func() { handler.Handle(context.Background(), message) }
			}
			concurrently(calls...)
			var retries []*fakeMessage
			var redeliveries []func()
			for i, message := range messages {
				if message.outcome(t) != nacked {
					continue
				}
				retry := &fakeMessage{id: message.id, data: message.data, attempt: 2}
				retries = append(retries, retry)
				handler := replicas[(i+1)%len(replicas)]
				redeliveries = append(redeliveries, func() { handler.Handle(context.Background(), retry) })
			}
			if len(retries) != 6 {
				t.Fatalf("nacked deliveries = %d, want 6 failed ledger operations", len(retries))
			}
			concurrently(redeliveries...)
			for _, retry := range retries {
				regressionWantOutcome(t, retry, acked)
			}
			want := int64(1200)
			if kind == events.TypeReversed {
				want = 0
			}
			regressionWantBalance(t, store, "member", want)
			regressionWantAwarded(t, store, "member", "2026-10", 1200)
		})
	}
}
