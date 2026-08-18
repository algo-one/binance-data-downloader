package binancedata

import (
	"errors"
	"testing"
	"time"
)

// TestParseInterval covers the spellings that must be accepted and, more
// importantly, the ones that must not be. The 1m/1M pair is the case this whole
// test file exists for.
func TestParseInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    Interval
		wantErr bool
	}{
		{name: "archive spelling", input: "1h", want: Interval1h},
		{name: "shortest interval", input: "1s", want: Interval1s},
		{name: "longest fixed interval", input: "1w", want: Interval1w},

		// The two spellings of the monthly interval. Both are legitimate
		// input: "1mo" is what appears in archive file names, "1M" is what the
		// REST API documents, and users copy from both.
		{name: "monthly archive spelling", input: "1mo", want: Interval1mo},
		{name: "monthly REST spelling", input: "1M", want: Interval1mo},

		// The reason ParseInterval must never fold case. If "1M" were
		// lower-cased first it would parse as one minute, and a caller asking
		// for monthly candles would silently receive minute candles.
		{name: "lowercase m is a minute, not a month", input: "1m", want: Interval1m},

		{name: "empty string", input: "", wantErr: true},
		{name: "unknown interval", input: "7h", wantErr: true},
		{name: "uppercased archive spelling is not accepted", input: "1MO", wantErr: true},
		{name: "uppercased second is not accepted", input: "1S", wantErr: true},
		{name: "uppercased hour is not accepted", input: "1H", wantErr: true},
		{name: "surrounding whitespace is not trimmed", input: " 1h", wantErr: true},
		{name: "duration syntax is not accepted", input: "1h0m0s", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseInterval(tt.input)

			if tt.wantErr {
				// Every rejection must be recognisable as ErrInvalidRequest
				// through the wrapping, not merely be "some error".
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("ParseInterval(%q) error = %v, want one wrapping ErrInvalidRequest", tt.input, err)
				}

				// A failed parse must not hand back a usable value alongside
				// the error, or a caller who checks the error late still gets
				// plausible-looking data.
				if got.IsValid() {
					t.Errorf("ParseInterval(%q) = %v with an error; want the invalid zero value", tt.input, got)
				}

				return
			}

			if err != nil {
				t.Fatalf("ParseInterval(%q) unexpected error: %v", tt.input, err)
			}

			if got != tt.want {
				t.Errorf("ParseInterval(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestIntervalRoundTrip asserts that every interval survives a trip out to each
// of its two spellings and back. Written as a loop over Intervals() rather than
// as a table, so that adding a seventeenth interval is automatically covered
// instead of silently untested.
func TestIntervalRoundTrip(t *testing.T) {
	t.Parallel()

	for _, interval := range Intervals() {
		t.Run(interval.String(), func(t *testing.T) {
			t.Parallel()

			archive := interval.String()
			if archive == "" {
				t.Fatalf("%v has an empty archive spelling", uint8(interval))
			}

			got, err := ParseInterval(archive)
			if err != nil {
				t.Fatalf("ParseInterval(%q) from String(): %v", archive, err)
			}
			if got != interval {
				t.Errorf("ParseInterval(%q) = %v, want %v", archive, got, interval)
			}

			rest := interval.RESTParam()
			if rest == "" {
				t.Fatalf("%v has an empty REST spelling", interval)
			}

			got, err = ParseInterval(rest)
			if err != nil {
				t.Fatalf("ParseInterval(%q) from RESTParam(): %v", rest, err)
			}
			if got != interval {
				t.Errorf("ParseInterval(%q) = %v, want %v", rest, got, interval)
			}
		})
	}
}

// TestIntervalSpellings pins the one interval whose two spellings differ, and
// asserts that it is the only one. If a future Binance change makes a second
// interval diverge, this test says so rather than letting one code path quietly
// use the wrong string.
func TestIntervalSpellings(t *testing.T) {
	t.Parallel()

	if got, want := Interval1mo.String(), "1mo"; got != want {
		t.Errorf("Interval1mo.String() = %q, want %q", got, want)
	}
	if got, want := Interval1mo.RESTParam(), "1M"; got != want {
		t.Errorf("Interval1mo.RESTParam() = %q, want %q", got, want)
	}

	for _, interval := range Intervals() {
		if interval == Interval1mo {
			continue
		}

		if interval.String() != interval.RESTParam() {
			t.Errorf("%v: spellings diverge unexpectedly: archive %q, REST %q",
				interval, interval.String(), interval.RESTParam())
		}
	}
}

// TestIntervalSpellingsAreUnique guards the parse index against two intervals
// claiming the same string, which would make one of them unreachable.
func TestIntervalSpellingsAreUnique(t *testing.T) {
	t.Parallel()

	seen := make(map[string]Interval)
	for _, interval := range Intervals() {
		for _, spelling := range []string{interval.String(), interval.RESTParam()} {
			if other, ok := seen[spelling]; ok && other != interval {
				t.Errorf("spelling %q claimed by both %v and %v", spelling, other, interval)
			}
			seen[spelling] = interval
		}
	}

	// Sixteen intervals, of which exactly one contributes a second distinct
	// spelling.
	if got, want := len(seen), len(Intervals())+1; got != want {
		t.Errorf("index holds %d spellings, want %d", got, want)
	}
}

// TestIntervalArchiveAvailability pins the two tables from
// docs/architecture.md. These are facts about what Binance publishes, and
// getting one wrong produces a 404 that is indistinguishable from "this symbol
// had not been listed yet" — so they are asserted explicitly rather than
// derived from the same table the code reads.
func TestIntervalArchiveAvailability(t *testing.T) {
	t.Parallel()

	wantDaily := map[Interval]bool{
		Interval1s: true, Interval1m: true, Interval3m: true, Interval5m: true,
		Interval15m: true, Interval30m: true, Interval1h: true, Interval2h: true,
		Interval4h: true, Interval6h: true, Interval8h: true, Interval12h: true,
		Interval1d: true,
	}

	// 1s is present here, unlike in the Python source's table. Verified by
	// HEAD against the live archives on 2026-08-18.
	wantMonthly := map[Interval]bool{
		Interval1s: true,
		Interval1m: true, Interval3m: true, Interval5m: true, Interval15m: true,
		Interval30m: true, Interval1h: true, Interval2h: true, Interval4h: true,
		Interval6h: true, Interval8h: true, Interval12h: true, Interval1d: true,
		Interval3d: true, Interval1w: true, Interval1mo: true,
	}

	for _, interval := range Intervals() {
		if got := interval.HasDailyArchives(); got != wantDaily[interval] {
			t.Errorf("%v.HasDailyArchives() = %t, want %t", interval, got, wantDaily[interval])
		}
		if got := interval.HasMonthlyArchives(); got != wantMonthly[interval] {
			t.Errorf("%v.HasMonthlyArchives() = %t, want %t", interval, got, wantMonthly[interval])
		}

		// An interval published at neither granularity would be a planning
		// dead end: nothing could ever be downloaded for it in bulk.
		if !interval.HasDailyArchives() && !interval.HasMonthlyArchives() {
			t.Errorf("%v is published at neither granularity", interval)
		}
	}

	// The one asymmetry, stated directly so the intent survives a careless
	// edit to the table above: candles longer than a day have no daily
	// archive. Everything else, 1s included, is published at both.
	for _, interval := range []Interval{Interval3d, Interval1w, Interval1mo} {
		if interval.HasDailyArchives() {
			t.Errorf("%v must be monthly-only", interval)
		}
	}
	if !Interval1s.HasMonthlyArchives() {
		t.Error("1s has monthly archives; Binance publishes them despite the Python source's table")
	}
}

// TestIntervalDuration checks the fixed lengths and, more importantly, that the
// calendar-based interval refuses to claim one.
func TestIntervalDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		interval Interval
		want     time.Duration
		wantOK   bool
	}{
		{interval: Interval1s, want: time.Second, wantOK: true},
		{interval: Interval1m, want: time.Minute, wantOK: true},
		{interval: Interval15m, want: 15 * time.Minute, wantOK: true},
		{interval: Interval1h, want: time.Hour, wantOK: true},
		{interval: Interval12h, want: 12 * time.Hour, wantOK: true},
		{interval: Interval1d, want: 24 * time.Hour, wantOK: true},
		{interval: Interval3d, want: 72 * time.Hour, wantOK: true},
		{interval: Interval1w, want: 7 * 24 * time.Hour, wantOK: true},

		// A month is 28, 29, 30 or 31 days. Reporting no fixed duration is the
		// correct answer, and returning an approximation would be worse than
		// returning nothing.
		{interval: Interval1mo, want: 0, wantOK: false},

		{interval: Interval(0), want: 0, wantOK: false},
		{interval: Interval(200), want: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.interval.String(), func(t *testing.T) {
			t.Parallel()

			got, ok := tt.interval.Duration()
			if ok != tt.wantOK {
				t.Fatalf("%v.Duration() ok = %t, want %t", tt.interval, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("%v.Duration() = %v, want %v", tt.interval, got, tt.want)
			}
		})
	}
}

// TestIntervalIsValid covers the values a caller can construct that the
// constants do not name.
func TestIntervalIsValid(t *testing.T) {
	t.Parallel()

	// The zero value is the case that matters: it is what an unset struct
	// field holds, and it must not be mistaken for a real interval.
	var unset Interval
	if unset.IsValid() {
		t.Error("the zero value of Interval must not be valid")
	}

	if Interval(200).IsValid() {
		t.Error("Interval(200) must not be valid")
	}

	for _, interval := range Intervals() {
		if !interval.IsValid() {
			t.Errorf("%v from Intervals() reports itself invalid", interval)
		}
	}
}

// TestIntervalStringInvalid checks that a broken value prints something a
// reader can act on rather than an empty string that leaves a hole in a log
// line.
func TestIntervalStringInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		interval Interval
		want     string
	}{
		{interval: Interval(0), want: "Interval(0)"},
		{interval: Interval(200), want: "Interval(200)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()

			if got := tt.interval.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
			if got := tt.interval.RESTParam(); got != "" {
				t.Errorf("RESTParam() = %q, want the empty string", got)
			}
		})
	}
}

// TestIntervals checks the ordering contract and that the returned slice is a
// copy. The copy matters: a caller sorting or truncating the result must not be
// able to damage the package for every other caller in the process.
func TestIntervals(t *testing.T) {
	t.Parallel()

	all := Intervals()

	if got, want := len(all), 16; got != want {
		t.Fatalf("Intervals() returned %d intervals, want %d", got, want)
	}

	if all[0] != Interval1s {
		t.Errorf("Intervals()[0] = %v, want %v", all[0], Interval1s)
	}
	if last := all[len(all)-1]; last != Interval1mo {
		t.Errorf("last interval = %v, want %v", last, Interval1mo)
	}

	for i := 1; i < len(all); i++ {
		if all[i] <= all[i-1] {
			t.Errorf("Intervals() is not ascending at index %d: %v then %v", i, all[i-1], all[i])
		}
	}

	// Mutate the returned slice, then ask for a fresh one and check the damage
	// did not stick.
	all[0] = Interval1mo
	if again := Intervals(); again[0] != Interval1s {
		t.Error("Intervals() shares state between calls; a caller mutated the package")
	}
}

// TestIntervalTextMarshalling exercises the encoding.TextMarshaler pair, which
// is what makes JSON fields and flag.TextVar work in later stages.
func TestIntervalTextMarshalling(t *testing.T) {
	t.Parallel()

	t.Run("round trip through text", func(t *testing.T) {
		t.Parallel()

		for _, interval := range Intervals() {
			text, err := interval.MarshalText()
			if err != nil {
				t.Fatalf("%v.MarshalText(): %v", interval, err)
			}

			// The unmarshalling target is a variable, because UnmarshalText
			// has a pointer receiver and writes through it.
			var got Interval
			if err := got.UnmarshalText(text); err != nil {
				t.Fatalf("UnmarshalText(%q): %v", text, err)
			}

			if got != interval {
				t.Errorf("round trip of %v through %q produced %v", interval, text, got)
			}
		}
	})

	t.Run("marshalling an invalid interval fails", func(t *testing.T) {
		t.Parallel()

		var unset Interval
		if _, err := unset.MarshalText(); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("MarshalText() on the zero value: error = %v, want one wrapping ErrInvalidRequest", err)
		}
	})

	t.Run("unmarshalling rejects unknown text and leaves the target alone", func(t *testing.T) {
		t.Parallel()

		got := Interval1h
		if err := got.UnmarshalText([]byte("1decade")); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("UnmarshalText(\"1decade\"): error = %v, want one wrapping ErrInvalidRequest", err)
		}

		// A failed unmarshal must not half-write the target.
		if got != Interval1h {
			t.Errorf("failed UnmarshalText overwrote the target: got %v, want %v", got, Interval1h)
		}
	})

	t.Run("unmarshalling accepts the REST spelling", func(t *testing.T) {
		t.Parallel()

		var got Interval
		if err := got.UnmarshalText([]byte("1M")); err != nil {
			t.Fatalf("UnmarshalText(\"1M\"): %v", err)
		}
		if got != Interval1mo {
			t.Errorf("UnmarshalText(\"1M\") = %v, want %v", got, Interval1mo)
		}
	})
}
