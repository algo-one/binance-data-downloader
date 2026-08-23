package vision

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// What is worth testing here, and what is not.
//
// The token bucket itself belongs to golang.org/x/time/rate. Asserting that it
// refills correctly would be testing the Go team's code with our test suite,
// which is effort spent to learn nothing. What is ours is the *policy*: the
// numbers in limiter.go, the defaults [NewLimiter] substitutes, and the claim
// that those numbers stay inside the quota Binance publishes.
//
// The schedule test below straddles the line deliberately. It asserts on
// x/time/rate's behaviour, but what it is really pinning is that our chosen
// rate and burst produce the pacing the file comment claims — so a later edit
// to either constant fails here with the arithmetic spelled out, rather than
// silently changing how fast this library hits Binance.

// TestLimiterSpendsBurstThenPaces pins the shape of the configured policy: a
// program that has just started owes nothing, so its first calls go out at
// once, and only then does the sustained rate bind.
//
// # What synctest is doing here
//
// Inside a bubble the whole time package runs on a fake clock, private to this
// test and starting at midnight 2000-01-01. rate.Limiter's internal time.Now()
// and its timers are therefore virtual without the limiter knowing, and the
// clock jumps forward only when every goroutine in the bubble is blocked on
// something inside it.
//
// Two things follow, and both matter. The test asserts on *exact* durations —
// 50ms, not "between 40 and 70" — because fake time advances by precisely the
// amount the code asked to wait. And it costs no real time at all: this would
// otherwise sit out 350 ms of genuine sleeping, and the equivalent test for a
// realistic pipeline would sit out minutes.
//
// It is also the reason this package does not inject a clock into the limiter
// the way [Policy] does for retries. That injection exists to make delays
// assertable; a bubble makes them assertable without it.
func TestLimiterSpendsBurstThenPaces(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// The shipped defaults: 40 weight/second with a 20-weight bucket. At
		// KlinesWeight each, that is ten free calls and then one every 50 ms.
		lim := NewLimiter(DefaultWeightPerSecond, DefaultBurst)

		start := time.Now()

		for i := range DefaultBurst / KlinesWeight {
			if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
				t.Fatalf("burst call %d: %v", i+1, err)
			}
		}

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("spending the burst took %s, want no waiting at all", elapsed)
		}

		// The next call finds the bucket empty and waits for two tokens at
		// forty a second.
		if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
			t.Fatalf("first paced call: %v", err)
		}

		if elapsed, want := time.Since(start), 50*time.Millisecond; elapsed != want {
			t.Errorf("the first paced call landed at %s, want %s", elapsed, want)
		}

		// And every one after it is spaced identically, which is the property
		// the quota actually cares about.
		for i := range 5 {
			if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
				t.Fatalf("paced call %d: %v", i+2, err)
			}
		}

		if elapsed, want := time.Since(start), 300*time.Millisecond; elapsed != want {
			t.Errorf("six paced calls took %s, want %s", elapsed, want)
		}
	})
}

// TestLimiterStaysInsideTheQuota checks the numbers against the quota they were
// chosen for. It is arithmetic rather than behaviour, and it is here because
// the numbers are the part of this file a future edit is most likely to get
// wrong — raising the rate to make a download faster, without noticing what it
// is a fraction of.
func TestLimiterStaysInsideTheQuota(t *testing.T) {
	t.Parallel()

	if perMinute := DefaultWeightPerSecond * 60; perMinute > WeightLimitPerMinute {
		t.Errorf("the default rate spends %d weight a minute, past the published limit of %d",
			perMinute, WeightLimitPerMinute)
	}

	// The headroom is deliberate: the quota is per IP, so anything else on the
	// machine draws from the same budget. Half is the line this project drew;
	// crossing it should be a decision, which is what failing here makes it.
	if perMinute, half := DefaultWeightPerSecond*60, WeightLimitPerMinute/2; perMinute > half {
		t.Errorf("the default rate spends %d weight a minute, past the %d this library allows itself",
			perMinute, half)
	}

	// A burst smaller than one call's weight refuses every request outright,
	// which is a limiter that cannot be used rather than one that is strict.
	if DefaultBurst < KlinesWeight {
		t.Errorf("burst %d cannot admit a single klines call, which costs %d", DefaultBurst, KlinesWeight)
	}
}

// TestLimiterDefaultsAreUsable is the guard on a zero-valued construction.
//
// rate.NewLimiter has no such guard of its own, and both of its zero values
// fail silently in the worst way: a zero rate refills nothing, so the first
// call past the burst blocks forever, and a zero burst rejects every call
// outright. Neither is distinguishable from a hung network in a log.
func TestLimiterDefaultsAreUsable(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		lim := NewLimiter(0, 0)

		if got := float64(lim.Limit()); got != DefaultWeightPerSecond {
			t.Errorf("rate = %v, want the default %v", got, DefaultWeightPerSecond)
		}

		if got := lim.Burst(); got != DefaultBurst {
			t.Errorf("burst = %v, want the default %v", got, DefaultBurst)
		}

		// Constructed is not the same as usable. Spending past the burst is
		// what proves the bucket actually refills rather than merely reporting
		// a plausible rate.
		for i := range (DefaultBurst / KlinesWeight) + 2 {
			if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
				t.Fatalf("call %d: %v", i+1, err)
			}
		}
	})
}

// TestLimiterRefusesUnsatisfiableWeight covers weight the bucket can never
// hold, however long it refills. Blocking forever there would be a deadlock
// wearing a rate limiter's clothes, and in a log it looks like a hung network.
func TestLimiterRefusesUnsatisfiableWeight(t *testing.T) {
	t.Parallel()

	lim := NewLimiter(DefaultWeightPerSecond, DefaultBurst)

	if err := lim.WaitN(t.Context(), DefaultBurst+1); err == nil {
		t.Fatal("WaitN accepted a weight larger than the burst, want an error")
	}
}

// TestLimiterStopsOnCancellation checks that a caller queued behind the bucket
// leaves when its context ends, rather than sitting out the full delay. A
// cancelled backtest should stop, and a limiter is one of the few places in
// this package that blocks for long enough for the difference to show.
func TestLimiterStopsOnCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// One call's worth of bucket, refilling once an hour: the second call
		// has a long wait ahead of it.
		lim := NewLimiter(1.0/3600, KlinesWeight)

		if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
			t.Fatalf("first call: %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())

		// Cancel from another goroutine in the same bubble, so the wait is
		// genuinely entered and then interrupted. synctest advances the clock
		// only when every goroutine is blocked on something inside the bubble,
		// which is exactly what makes this deterministic instead of a race
		// between the cancel and the timer.
		go cancel()

		err := lim.WaitN(ctx, KlinesWeight)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitN = %v, want context.Canceled", err)
		}
	})
}

// TestBurstFor covers the rule that pairs a burst with a rate.
//
// The rule earns a test rather than a comment because it has a trap at one end:
// a burst below KlinesWeight makes WaitN return an error instead of waiting, so
// a caller asking for a very low rate would get a limiter that refuses every
// call rather than a slow one. That is the failure the floor exists for, and it
// only appears at rates nobody types by accident.
func TestBurstFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rate float64
		want int
	}{
		{
			// The shipped pair, reproduced by the rule rather than declared
			// beside it. If DefaultBurst and DefaultWeightPerSecond ever drift
			// apart, this is what says so.
			name: "the default rate yields the default burst",
			rate: DefaultWeightPerSecond,
			want: DefaultBurst,
		},
		{
			name: "half the default rate yields half the burst",
			rate: DefaultWeightPerSecond / 2,
			want: DefaultBurst / 2,
		},
		{
			name: "the full quota",
			rate: WeightLimitPerMinute / 60,
			want: 50,
		},
		{
			// Half of 3 is 1, which is below one call's weight.
			name: "a rate whose half cannot admit one call is floored",
			rate: 3,
			want: KlinesWeight,
		},
		{
			name: "a rate far below one call's weight is still usable",
			rate: 0.5,
			want: KlinesWeight,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := BurstFor(tc.rate)
			if got != tc.want {
				t.Errorf("BurstFor(%v) = %d, want %d", tc.rate, got, tc.want)
			}

			// Whatever the arithmetic said, the result has to admit a call.
			// Stated separately from the table so that a new case cannot be
			// added without this holding for it too.
			if got < KlinesWeight {
				t.Errorf("BurstFor(%v) = %d, which cannot admit a klines call costing %d",
					tc.rate, got, KlinesWeight)
			}
		})
	}
}

// TestBurstForIsUsableAtALowRate is the same claim as a measurement.
//
// BurstFor returning a plausible number is not the same as the limiter it
// builds being able to serve a request. This spends past the burst at a rate
// low enough that the floor is what produced it, inside a bubble so the waiting
// costs no real time.
func TestBurstForIsUsableAtALowRate(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		const slow = 1 // one weight per second: half a klines call

		lim := NewLimiter(slow, BurstFor(slow))

		start := time.Now()

		// Three calls: the first drains the burst, the next two wait for it to
		// refill at one weight a second, so two calls of KlinesWeight each.
		for i := range 3 {
			if err := lim.WaitN(t.Context(), KlinesWeight); err != nil {
				t.Fatalf("call %d: %v", i+1, err)
			}
		}

		// Exact, not approximate. That is what the bubble buys.
		if got, want := time.Since(start), 4*time.Second; got != want {
			t.Errorf("three calls took %v, want %v", got, want)
		}
	})
}
