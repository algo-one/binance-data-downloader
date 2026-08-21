package binancedata

import (
	"errors"
	"testing"
	"time"
)

func utc(year int, month time.Month, day int, hms ...int) time.Time {
	var h, m, s int

	if len(hms) > 0 {
		h = hms[0]
	}

	if len(hms) > 1 {
		m = hms[1]
	}

	if len(hms) > 2 {
		s = hms[2]
	}

	return time.Date(year, month, day, h, m, s, 0, time.UTC)
}

// upTo turns a half-open boundary into the closed [Request.End] that covers
// exactly the same candles: the last instant before it.
//
// It exists so the tables below can go on naming the round boundary they mean —
// "everything up to the 16th" — while asking for it in the convention Request
// actually uses. Written out at each site as t.Add(-time.Nanosecond), the
// intent would be buried in arithmetic 40 times over, and one of those forty
// would eventually say -time.Millisecond.
//
// Note what does NOT use it: [plan.Chunk] bounds, [decodeSpec] bounds and
// vision.KlineQuery bounds are all still half-open, because those are internal
// seams. Only a Request is closed.
func upTo(t time.Time) time.Time {
	return t.Add(-time.Nanosecond)
}

// validRequest is the baseline the tables below mutate one field at a time, so
// that each case states only what makes it interesting.
func validRequest() Request {
	return Request{
		Symbol:   "BTC/USDT",
		Interval: Interval1h,
		Market:   MarketSpot,
		Start:    utc(2024, 1, 1),
		End:      upTo(utc(2024, 2, 1)),
	}
}

func TestRequestValidate(t *testing.T) {
	t.Parallel()

	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*Request)
		wantErr bool
	}{
		{
			name:   "the baseline is valid",
			mutate: func(*Request) {},
		},
		{
			name:   "a zero end is legal and means now",
			mutate: func(r *Request) { r.End = time.Time{} },
		},
		{
			name:   "symbols may be written unseparated",
			mutate: func(r *Request) { r.Symbol = "btcusdt" },
		},
		{
			name:   "monthly-only intervals are accepted",
			mutate: func(r *Request) { r.Interval = Interval1mo },
		},
		{
			name:    "an unset interval is rejected",
			mutate:  func(r *Request) { r.Interval = 0 },
			wantErr: true,
		},
		{
			name:    "an unset market is rejected",
			mutate:  func(r *Request) { r.Market = 0 },
			wantErr: true,
		},
		{
			name:    "an empty symbol is rejected",
			mutate:  func(r *Request) { r.Symbol = "" },
			wantErr: true,
		},
		{
			name:    "a malformed symbol is rejected",
			mutate:  func(r *Request) { r.Symbol = "BTC USDT" },
			wantErr: true,
		},
		{
			name:    "a zero start is rejected",
			mutate:  func(r *Request) { r.Start = time.Time{} },
			wantErr: true,
		},
		{
			name:    "a non-UTC start is rejected",
			mutate:  func(r *Request) { r.Start = time.Date(2024, 1, 1, 0, 0, 0, 0, newYork) },
			wantErr: true,
		},
		{
			name:    "a non-UTC end is rejected",
			mutate:  func(r *Request) { r.End = time.Date(2024, 2, 1, 0, 0, 0, 0, newYork) },
			wantErr: true,
		},
		{
			name:    "an end before the start is rejected",
			mutate:  func(r *Request) { r.End = utc(2023, 1, 1) },
			wantErr: true,
		},
		{
			// A closed range [t, t] holds one instant, and asks for the one
			// candle that opens there. Under the half-open rule this spelling
			// was empty by definition and had to be rejected; now it is a
			// legitimate single-candle request, and refusing it would refuse
			// something a caller can reasonably want.
			name:   "an end equal to the start is a single-instant request",
			mutate: func(r *Request) { r.End = r.Start },
		},
		{
			// One nanosecond the other way is still an error, which is what
			// pins the comparison down to the exact boundary rather than
			// "roughly the right direction".
			name:    "an end one nanosecond before the start is rejected",
			mutate:  func(r *Request) { r.End = r.Start.Add(-time.Nanosecond) },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := validRequest()
			tt.mutate(&req)

			err := req.Validate()

			switch {
			case tt.wantErr && err == nil:
				t.Error("want an error, got nil")
			case tt.wantErr && !errors.Is(err, ErrInvalidRequest):
				t.Errorf("error %v does not wrap ErrInvalidRequest", err)
			case !tt.wantErr && err != nil:
				t.Errorf("want no error, got %v", err)
			}
		})
	}
}

// TestResolveEndIsPerCall is the regression test for the first bug in the
// ported Python implementation, and the one whose symptom was hardest to spot.
//
// That code wrote `def register(..., end_date=datetime.now(UTC))`. Python
// evaluates a default argument once, when the function is defined — so the
// "current" end date is frozen at import time. A process that runs for a week
// keeps asking for data up to the day it started, returning less and less of
// what was requested, and never says so.
//
// The fix is not a better default; it is having nothing to freeze. A zero End
// is a value meaning "now", and now arrives as a parameter at the moment of the
// call. This test builds one Request and resolves it twice against two clocks,
// which is exactly the shape the Python bug failed.
func TestResolveEndIsPerCall(t *testing.T) {
	t.Parallel()

	// One request value, reused — the analogue of a default argument bound
	// once and used forever.
	req := Request{
		Symbol:   "BTCUSDT",
		Interval: Interval1h,
		Market:   MarketSpot,
		Start:    utc(2024, 1, 1),
	}

	monday := utc(2026, 8, 10, 9, 30)
	sunday := utc(2026, 8, 16, 9, 30)

	first, err := req.resolve(monday)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	second, err := req.resolve(sunday)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if !first.End.Equal(monday) {
		t.Errorf("first resolution ended at %s, want %s", first.End, monday)
	}

	if !second.End.Equal(sunday) {
		t.Errorf("second resolution ended at %s, want %s", second.End, sunday)
	}

	if first.End.Equal(second.End) {
		t.Error("both resolutions produced the same end: the clock reading was captured, not resolved per call")
	}

	// The original is untouched. resolve has a value receiver, so it works on
	// a copy; if it did not, the first call would have written an End into req
	// and the second would have found one already there — reintroducing the
	// bug through the back door.
	if !req.End.IsZero() {
		t.Error("resolve mutated the caller's request, so a later call would reuse the stale end")
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()

	now := utc(2026, 8, 18, 12, 0)

	t.Run("normalises the symbol", func(t *testing.T) {
		t.Parallel()

		req := validRequest()
		req.Symbol = "btc-usdt"

		got, err := req.resolve(now)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}

		if got.Symbol != "BTCUSDT" {
			t.Errorf("symbol = %q, want %q", got.Symbol, "BTCUSDT")
		}
	})

	t.Run("keeps an explicit end", func(t *testing.T) {
		t.Parallel()

		req := validRequest()

		got, err := req.resolve(now)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}

		if !got.End.Equal(req.End) {
			t.Errorf("end = %s, want the explicit %s", got.End, req.End)
		}
	})

	t.Run("rejects a start in the future", func(t *testing.T) {
		t.Parallel()

		req := validRequest()
		req.Start = utc(2027, 1, 1)
		req.End = time.Time{}

		_, err := req.resolve(now)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("got %v, want an error wrapping ErrInvalidRequest", err)
		}
	})

	t.Run("propagates validation errors", func(t *testing.T) {
		t.Parallel()

		req := validRequest()
		req.Interval = 0

		if _, err := req.resolve(now); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("got %v, want an error wrapping ErrInvalidRequest", err)
		}
	})

	t.Run("converts a non-UTC clock rather than failing", func(t *testing.T) {
		t.Parallel()

		req := validRequest()
		req.End = time.Time{}

		got, err := req.resolve(time.Date(2026, 8, 18, 12, 0, 0, 0, time.FixedZone("CET", 3600)))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}

		// The clock is our wiring, not the caller's input, so a non-UTC one is
		// converted rather than rejected — but the result must still be UTC,
		// because a Request is used as a map key in FetchAll and a Location
		// pointer is part of what == compares there.
		if got.End.Location() != time.UTC {
			t.Errorf("end location = %s, want UTC", got.End.Location())
		}
	})
}

// TestRequestIsUsableAsAMapKey pins the property FetchAll's
// map[Request][]Kline signature depends on.
//
// time.Time equality under == compares the wall clock, the monotonic reading
// and the *Location pointer — not the instant. Requiring UTC is what makes two
// requests for the same range land on the same map entry, and this test is
// where that stops being an argument and becomes a guarantee.
func TestRequestIsUsableAsAMapKey(t *testing.T) {
	t.Parallel()

	// Two times naming the same instant, built by different routes: one
	// constructed directly, one parsed. Both are UTC, so both must be ==.
	parsed, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("parsing: %v", err)
	}

	a := validRequest()
	b := validRequest()
	b.Start = parsed

	seen := map[Request]int{}
	seen[a]++
	seen[b]++

	if len(seen) != 1 {
		t.Errorf("the same request produced %d map entries, want 1", len(seen))
	}

	if seen[a] != 2 {
		t.Errorf("count = %d, want 2", seen[a])
	}
}

// TestEndExclusiveIsExactlyOneNanosecondPast covers the single point where the
// library's two range conventions meet.
//
// Everything a caller writes is closed, [Start, End]; everything below the
// public API is half-open, [Start, End). One function converts between them,
// and the size of the step it takes is the whole argument for doing it this
// way — so the step is asserted rather than assumed.
func TestEndExclusiveIsExactlyOneNanosecondPast(t *testing.T) {
	t.Parallel()

	end := utc(2024, 2, 1)

	req := validRequest()
	req.End = end

	got := req.endExclusive()

	if want := end.Add(time.Nanosecond); !got.Equal(want) {
		t.Errorf("endExclusive() = %s, want %s",
			got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}

	// A candle opening exactly at End is inside the exclusive bound. This is
	// the property the conversion exists for, stated as the comparison the
	// code below it actually performs.
	if !end.Before(got) {
		t.Errorf("a candle opening at End (%s) falls outside the exclusive bound %s",
			end.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano))
	}

	// And the step is smaller than any resolution Binance publishes in. The
	// archives carry milliseconds, and microseconds since 2025, so a whole
	// microsecond past End must already be outside the bound. If the step ever
	// grew to a microsecond or more, a real candle could sit inside it and the
	// range would silently gain one.
	if end.Add(time.Microsecond).Before(got) {
		t.Errorf("the exclusive bound %s is at least a microsecond past End (%s), "+
			"which is wide enough to admit a real candle",
			got.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano))
	}
}
