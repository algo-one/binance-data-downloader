package plan

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// utc is shorthand for building a UTC instant in a test table. Tests in this
// package name dozens of instants, and time.Date(2024, 1, 1, 0, 0, 0, 0,
// time.UTC) six times on one line is unreadable.
//
// The variadic tail takes hour, minute, second — so utc(2024, 1, 15) is
// midnight and utc(2024, 1, 15, 12) is noon. Go has no default arguments;
// variadic parameters plus a switch is the usual stand-in, and it is worth the
// four lines when it makes a table legible.
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

// monthly, daily and rest build expected chunks with less ceremony than a
// struct literal, so an expected plan reads as a list of periods.
func monthly(y int, m time.Month) Chunk {
	start := utc(y, m, 1)

	return Chunk{KindMonthlyArchive, start, start.AddDate(0, 1, 0)}
}

func daily(y int, m time.Month, d int) Chunk {
	start := utc(y, m, d)

	return Chunk{KindDailyArchive, start, start.AddDate(0, 0, 1)}
}

func rest(start, end time.Time) Chunk {
	return Chunk{KindRESTRange, start, end}
}

// dailyRange builds the consecutive daily chunks from day through last
// inclusive, which is what a partial month expands to.
func dailyRange(y int, m time.Month, day, last int) []Chunk {
	var out []Chunk
	for d := day; d <= last; d++ {
		out = append(out, daily(y, m, d))
	}

	return out
}

// assertChunks compares two plans and reports the first difference in terms of
// periods rather than struct fields. A failing plan test is read by a human
// trying to see which day went missing, so the output is built for that.
func assertChunks(t *testing.T, got, want []Chunk) {
	t.Helper()

	if len(got) != len(want) {
		t.Errorf("got %d chunks, want %d", len(got), len(want))
	}

	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			t.Errorf("chunk %d: got %s, want %s", i, got[i], want[i])
		}
	}

	// Print both plans in full only once something is already wrong. Failed
	// returns true if the test has recorded a failure, which makes it the hook
	// for "add context to a failure I already know about".
	if t.Failed() {
		t.Logf("full plan:\n got: %s\nwant: %s", format(got), format(want))
	}
}

func format(chunks []Chunk) string {
	if len(chunks) == 0 {
		return "(none)"
	}

	out := ""
	for _, c := range chunks {
		out += "\n  " + c.String()
	}

	return out
}

// A frozen frontier standing in for "archives are published well past every
// date these tests use". Nothing in this package reads a clock, so this is an
// ordinary constant rather than a mocked time source.
var farFuture = utc(2026, 8, 17)

func TestExpand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec Spec
		want []Chunk
	}{
		{
			name: "whole months use monthly archives",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2024, 4, 1),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: []Chunk{monthly(2024, 1), monthly(2024, 2), monthly(2024, 3)},
		},
		{
			// Expand is availability-blind, so a partial month always becomes
			// days here. Whether the month was the cheaper fetch after all is
			// [Consolidate]'s question, because only the listing can answer it.
			name: "partial first month falls back to dailies",
			spec: Spec{
				Start: utc(2024, 1, 15), End: utc(2024, 3, 1),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: append(dailyRange(2024, 1, 15, 31), monthly(2024, 2)),
		},
		{
			name: "partial last month falls back to dailies",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2024, 2, 5),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: append([]Chunk{monthly(2024, 1)}, dailyRange(2024, 2, 1, 4)...),
		},
		{
			name: "a start mid-day is covered by that whole day",
			spec: Spec{
				Start: utc(2024, 1, 15, 13, 30), End: utc(2024, 1, 18),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: dailyRange(2024, 1, 15, 17),
		},
		{
			name: "leap day is not special",
			spec: Spec{
				Start: utc(2024, 2, 26), End: utc(2024, 3, 2),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: append(dailyRange(2024, 2, 26, 29), daily(2024, 3, 1)),
		},
		{
			name: "monthly-only interval uses whole months even for a partial range",
			spec: Spec{
				Start: utc(2024, 2, 15), End: utc(2024, 4, 10),
				ArchivesThrough: farFuture, HasDaily: false, HasMonthly: true,
			},
			want: []Chunk{monthly(2024, 2), monthly(2024, 3), monthly(2024, 4)},
		},
		{
			name: "the tail beyond the archives goes to REST",
			spec: Spec{
				Start: utc(2026, 8, 1), End: utc(2026, 8, 18),
				ArchivesThrough: utc(2026, 8, 17), HasDaily: true, HasMonthly: true,
			},
			want: append(dailyRange(2026, 8, 1, 16), rest(utc(2026, 8, 17), utc(2026, 8, 18))),
		},
		{
			name: "no archives at all means the whole range is REST",
			spec: Spec{
				Start: utc(2026, 8, 1), End: utc(2026, 8, 18),
				ArchivesThrough: time.Time{}, HasDaily: true, HasMonthly: true,
			},
			want: []Chunk{rest(utc(2026, 8, 1), utc(2026, 8, 18))},
		},
		{
			name: "a range entirely after the archives is all REST",
			spec: Spec{
				Start: utc(2026, 8, 17), End: utc(2026, 8, 18),
				ArchivesThrough: utc(2026, 8, 17), HasDaily: true, HasMonthly: true,
			},
			want: []Chunk{rest(utc(2026, 8, 17), utc(2026, 8, 18))},
		},
		{
			name: "a single day",
			spec: Spec{
				Start: utc(2024, 6, 10), End: utc(2024, 6, 11),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: []Chunk{daily(2024, 6, 10)},
		},
		{
			name: "a whole year",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2025, 1, 1),
				ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
			},
			want: []Chunk{
				monthly(2024, 1), monthly(2024, 2), monthly(2024, 3), monthly(2024, 4),
				monthly(2024, 5), monthly(2024, 6), monthly(2024, 7), monthly(2024, 8),
				monthly(2024, 9), monthly(2024, 10), monthly(2024, 11), monthly(2024, 12),
			},
		},
		{
			name: "monthly-only interval past the frontier still gets a REST tail",
			spec: Spec{
				Start: utc(2026, 6, 1), End: utc(2026, 8, 18),
				ArchivesThrough: utc(2026, 8, 1), HasDaily: false, HasMonthly: true,
			},
			want: []Chunk{
				monthly(2026, 6), monthly(2026, 7),
				rest(utc(2026, 8, 1), utc(2026, 8, 18)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Expand(tt.spec)
			if err != nil {
				t.Fatalf("Expand: unexpected error: %v", err)
			}

			assertChunks(t, got, tt.want)
		})
	}
}

// TestExpandDecember20ToJanuary3 is the regression test for the second bug in
// the ported Python implementation, and the one that cost the most data.
//
// That code decided whether a range sat inside the "current month" by comparing
// dates shifted with relativedelta, without first normalising either side to
// the 1st. A range from 2024-12-20 to 2025-01-03 therefore compared as a single
// month, took the whole-month path, and returned December while silently
// dropping the first days of January.
//
// The fix is structural rather than a patched comparison: month boundaries in
// [Expand] are only ever produced by monthStart, so they are the 1st by
// construction and there is no shifted date to get wrong. This test pins the
// behaviour that structure produces.
func TestExpandDecember20ToJanuary3(t *testing.T) {
	t.Parallel()

	spec := Spec{
		Start:           utc(2024, 12, 20),
		End:             utc(2025, 1, 3),
		ArchivesThrough: farFuture,
		HasDaily:        true,
		HasMonthly:      true,
	}

	got, err := Expand(spec)
	if err != nil {
		t.Fatalf("Expand: unexpected error: %v", err)
	}

	want := append(dailyRange(2024, 12, 20, 31), dailyRange(2025, 1, 1, 2)...)
	assertChunks(t, got, want)

	// State the property the bug violated, separately from the exact plan, so
	// that a future change to the chunking strategy cannot quietly reintroduce
	// it by making the table above "expected to change".
	last := got[len(got)-1]
	if last.End.Before(spec.End) {
		t.Errorf("plan stops at %s, before the requested end %s: the January days were dropped",
			last.End.Format(time.RFC3339), spec.End.Format(time.RFC3339))
	}
}

// TestExpandCoversEveryRange is the regression test for the third ported bug: a
// code path that fetched days 1..N-2 and left the last two days to nobody.
//
// Rather than pin one range, it sweeps a few hundred of them across month
// boundaries, leap days, year ends and the archive frontier, and asserts the
// invariant directly on each result. [Expand] already checks this internally,
// so a failure here means both the arithmetic and its own assertion are wrong;
// checking independently is what makes the assertion worth having.
func TestExpandCoversEveryRange(t *testing.T) {
	t.Parallel()

	intervals := []struct {
		name              string
		hasDaily, monthly bool
	}{
		{"daily and monthly", true, true},
		{"monthly only", false, true},
	}

	frontiers := []struct {
		name string
		at   time.Time
	}{
		{"archives far ahead", farFuture},
		{"frontier inside the range", utc(2024, 3, 15)},
		{"no archives", time.Time{}},
	}

	// Every start offset from a fixed origin, in days, crossing two month
	// boundaries and a leap day; every length from one day to ten weeks.
	origin := utc(2024, 1, 28)

	for _, iv := range intervals {
		for _, fr := range frontiers {
			t.Run(iv.name+"/"+fr.name, func(t *testing.T) {
				t.Parallel()

				for startDay := 0; startDay < 40; startDay++ {
					for _, lengthDays := range []int{1, 2, 13, 29, 31, 70} {
						start := origin.AddDate(0, 0, startDay)
						end := start.AddDate(0, 0, lengthDays)

						spec := Spec{
							Start: start, End: end,
							ArchivesThrough: fr.at,
							HasDaily:        iv.hasDaily, HasMonthly: iv.monthly,
						}

						chunks, err := Expand(spec)
						if err != nil {
							t.Fatalf("Expand(%s..%s): %v",
								start.Format(time.DateOnly), end.Format(time.DateOnly), err)
						}

						checkCovers(t, chunks, start, end)
					}
				}
			})
		}
	}
}

// checkCovers re-derives the coverage invariant independently of the
// implementation: sorted, contiguous, non-empty, spanning the whole range.
func checkCovers(t *testing.T, chunks []Chunk, start, end time.Time) {
	t.Helper()

	if len(chunks) == 0 {
		t.Fatalf("[%s,%s): no chunks", start.Format(time.DateOnly), end.Format(time.DateOnly))
	}

	if chunks[0].Start.After(start) {
		t.Errorf("[%s,%s): first chunk %s starts late",
			start.Format(time.DateOnly), end.Format(time.DateOnly), chunks[0])
	}

	for i, c := range chunks {
		if !c.Start.Before(c.End) {
			t.Errorf("[%s,%s): chunk %d is empty: %s",
				start.Format(time.DateOnly), end.Format(time.DateOnly), i, c)
		}

		if i > 0 && !c.Start.Equal(chunks[i-1].End) {
			t.Errorf("[%s,%s): gap or overlap between %s and %s",
				start.Format(time.DateOnly), end.Format(time.DateOnly), chunks[i-1], c)
		}
	}

	if last := chunks[len(chunks)-1]; last.End.Before(end) {
		t.Errorf("[%s,%s): last chunk %s ends early",
			start.Format(time.DateOnly), end.Format(time.DateOnly), last)
	}
}

func TestExpandRejectsBadSpecs(t *testing.T) {
	t.Parallel()

	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}

	tests := []struct {
		name string
		spec Spec
	}{
		{
			name: "zero start",
			spec: Spec{End: utc(2024, 1, 2), ArchivesThrough: farFuture, HasMonthly: true},
		},
		{
			name: "end before start",
			spec: Spec{
				Start: utc(2024, 2, 1), End: utc(2024, 1, 1),
				ArchivesThrough: farFuture, HasMonthly: true,
			},
		},
		{
			name: "empty range",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2024, 1, 1),
				ArchivesThrough: farFuture, HasMonthly: true,
			},
		},
		{
			name: "start not UTC",
			spec: Spec{
				Start: time.Date(2024, 1, 1, 0, 0, 0, 0, newYork), End: utc(2024, 2, 1),
				ArchivesThrough: farFuture, HasMonthly: true,
			},
		},
		{
			name: "frontier not midnight",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2024, 2, 1),
				ArchivesThrough: utc(2024, 1, 15, 6), HasMonthly: true,
			},
		},
		{
			name: "no archive granularity but a frontier is set",
			spec: Spec{
				Start: utc(2024, 1, 1), End: utc(2024, 2, 1),
				ArchivesThrough: farFuture,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := Expand(tt.spec)
			if !errors.Is(err, ErrInvalidSpec) {
				t.Errorf("got error %v, want one wrapping ErrInvalidSpec", err)
			}
		})
	}
}

func TestSubstitute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		chunk    Chunk
		hasDaily bool
		want     []Chunk
		wantErr  error
	}{
		{
			name:     "a missing month becomes its days",
			chunk:    monthly(2024, 2),
			hasDaily: true,
			want:     dailyRange(2024, 2, 1, 29), // 2024 is a leap year
		},
		{
			// The case that decided the fallback design. BTCUSDT's 1mo archive
			// for March 2024 does not exist while February and April both do,
			// verified against the live bucket on 2026-08-18. 1mo has no daily
			// archives, so any rule that stopped at "fall back to dailies"
			// would return nothing at all for that month.
			name:     "a missing month with no dailies becomes a REST range",
			chunk:    monthly(2024, 3),
			hasDaily: false,
			want:     []Chunk{rest(utc(2024, 3, 1), utc(2024, 4, 1))},
		},
		{
			name:     "a missing day becomes a REST range",
			chunk:    daily(2024, 5, 7),
			hasDaily: true,
			want:     []Chunk{rest(utc(2024, 5, 7), utc(2024, 5, 8))},
		},
		{
			// Expand only ever emits month-aligned monthly chunks, so nothing
			// in this library can reach this case today — which is exactly why
			// it is worth pinning. Midnight is the only time of day Binance
			// names a daily archive after, so a substitution that starts at
			// 07:00 asks for files that do not exist, and every one of them
			// 404s into a REST range that the archives could have served.
			//
			// The days it produces necessarily overshoot the chunk they
			// replace at both ends, for the same reason every other chunk in
			// this package may: an archive is a whole day and cannot be cut.
			name:     "a month that is not day-aligned still expands into whole days",
			chunk:    Chunk{KindMonthlyArchive, utc(2024, 2, 10, 7), utc(2024, 2, 13, 0, 30)},
			hasDaily: true,
			want:     dailyRange(2024, 2, 10, 13),
		},
		{
			name:     "a REST range has nothing left to fall back to",
			chunk:    rest(utc(2024, 5, 7), utc(2024, 5, 8)),
			hasDaily: true,
			wantErr:  ErrCoverageGap,
		},
		{
			name:    "a chunk with no kind is rejected",
			chunk:   Chunk{Start: utc(2024, 5, 7), End: utc(2024, 5, 8)},
			wantErr: ErrInvalidSpec,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Substitute(tt.chunk, tt.hasDaily)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("got error %v, want one wrapping %v", err, tt.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("Substitute: unexpected error: %v", err)
			}

			assertChunks(t, got, tt.want)

			// Whatever the substitution, it must cover at least what it
			// replaced — otherwise splicing it into a plan opens the gap the
			// coverage check was built to catch. At *least*, not exactly:
			// daily archives are whole days, so replacing a chunk that starts
			// mid-day means starting at the midnight before it.
			if got[0].Start.After(tt.chunk.Start) || got[len(got)-1].End.Before(tt.chunk.End) {
				t.Errorf("substitution spans [%s,%s), which does not cover the replaced chunk [%s,%s)",
					got[0].Start.Format(time.RFC3339), got[len(got)-1].End.Format(time.RFC3339),
					tt.chunk.Start.Format(time.RFC3339), tt.chunk.End.Format(time.RFC3339))
			}

			// And it must be contiguous within itself, since the whole point
			// of a substitution is that it can be spliced in where the chunk
			// it replaces used to be.
			for i := 1; i < len(got); i++ {
				if !got[i].Start.Equal(got[i-1].End) {
					t.Errorf("substitute chunk %d (%s) does not continue from %d (%s)",
						i, got[i], i-1, got[i-1])
				}
			}
		})
	}
}

// TestSubstituteKeepsPlansContiguous splices a substitution into a real plan and
// re-checks the invariant, which is the way substitutions are actually used.
func TestSubstituteKeepsPlansContiguous(t *testing.T) {
	t.Parallel()

	spec := Spec{
		Start: utc(2024, 1, 1), End: utc(2024, 4, 1),
		ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
	}

	chunks, err := Expand(spec)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}

	// Replace February, as though its monthly archive had 404'd.
	replacement, err := Substitute(chunks[1], spec.HasDaily)
	if err != nil {
		t.Fatalf("Substitute: %v", err)
	}

	spliced := make([]Chunk, 0, len(chunks)+len(replacement))
	spliced = append(spliced, chunks[0])
	spliced = append(spliced, replacement...)
	spliced = append(spliced, chunks[2:]...)

	checkCovers(t, spliced, spec.Start, spec.End)
}

func TestKindString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind Kind
		want string
	}{
		{KindMonthlyArchive, "monthly"},
		{KindDailyArchive, "daily"},
		{KindRESTRange, "rest"},
		{Kind(0), "Kind(0)"},
	}

	for _, tt := range tests {
		if got := tt.kind.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", uint8(tt.kind), got, tt.want)
		}
	}
}

// ExampleExpand doubles as documentation: `go test` runs it and compares stdout
// against the Output comment, so the example on pkg.go.dev cannot drift out of
// date the way a hand-written snippet in a doc comment can.
func ExampleExpand() {
	chunks, err := Expand(Spec{
		Start:           time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		End:             time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC),
		ArchivesThrough: time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC),
		HasDaily:        true,
		HasMonthly:      true,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(len(chunks), "chunks")
	fmt.Println("first:", chunks[0])
	fmt.Println("last: ", chunks[len(chunks)-1])

	// Output:
	// 21 chunks
	// first: daily[2026-07-28T00:00:00Z,2026-07-29T00:00:00Z)
	// last:  rest[2026-08-17T00:00:00Z,2026-08-18T00:00:00Z)
}

// alwaysPublished and neverPublished are the two trivial listings, for cases
// where the interesting variable is the plan rather than what exists.
func alwaysPublished(time.Time) bool { return true }
func neverPublished(time.Time) bool  { return false }

func TestConsolidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		in     []Chunk
		exists func(time.Time) bool
		want   []Chunk
	}{
		{
			// 16 of January's 31 days is over half, so one download beats
			// thirty-two.
			name:   "a run worth more than half the month becomes the month",
			in:     dailyRange(2024, 1, 16, 31),
			exists: alwaysPublished,
			want:   []Chunk{monthly(2024, 1)},
		},
		{
			// One day fewer is 15 of 31, and the threshold does not flip. The
			// two cases are deliberately adjacent so the boundary is pinned
			// rather than merely exercised.
			name:   "a run worth less than half the month stays as days",
			in:     dailyRange(2024, 1, 17, 31),
			exists: alwaysPublished,
			want:   dailyRange(2024, 1, 17, 31),
		},
		{
			// The case that moved this out of Expand. The month is worth
			// consolidating onto and does not exist, so consolidating would
			// mean a 404 followed by a fan-out across the *whole* month —
			// strictly worse than the days that were actually wanted.
			name:   "a month the listing does not have is never consolidated onto",
			in:     dailyRange(2024, 2, 5, 29),
			exists: neverPublished,
			want:   dailyRange(2024, 2, 5, 29),
		},
		{
			name:   "with no listing at all nothing is consolidated",
			in:     dailyRange(2024, 1, 16, 31),
			exists: nil,
			want:   dailyRange(2024, 1, 16, 31),
		},
		{
			// Runs are maximal *within* a month. Consolidating across the
			// boundary would produce a chunk naming an archive that does not
			// exist, since Binance publishes one file per calendar month.
			name:   "a run spanning a month boundary is split at it",
			in:     append(dailyRange(2024, 1, 16, 31), dailyRange(2024, 2, 1, 29)...),
			exists: alwaysPublished,
			want:   []Chunk{monthly(2024, 1), monthly(2024, 2)},
		},
		{
			// A hole in the run means the month does not cover the same
			// ground, so replacing it would claim coverage of the missing day.
			name:   "a run with a hole in it is left alone",
			in:     append(dailyRange(2024, 1, 1, 10), dailyRange(2024, 1, 12, 31)...),
			exists: alwaysPublished,
			want:   append(dailyRange(2024, 1, 1, 10), dailyRange(2024, 1, 12, 31)...),
		},
		{
			name: "chunks that are not daily archives pass through",
			in: []Chunk{
				monthly(2024, 1),
				rest(utc(2024, 2, 1), utc(2024, 2, 5)),
			},
			exists: alwaysPublished,
			want: []Chunk{
				monthly(2024, 1),
				rest(utc(2024, 2, 1), utc(2024, 2, 5)),
			},
		},
		{
			// The tail of a real plan: days up to the archive frontier, then
			// the REST range past it. Only the days are eligible.
			name:   "a daily run followed by a REST tail",
			in:     append(dailyRange(2026, 8, 1, 16), rest(utc(2026, 8, 17), utc(2026, 8, 18))),
			exists: alwaysPublished,
			want: []Chunk{
				monthly(2026, 8),
				rest(utc(2026, 8, 17), utc(2026, 8, 18)),
			},
		},
		{
			name:   "nothing to do",
			in:     nil,
			exists: alwaysPublished,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Consolidate(tt.in, tt.exists)

			assertChunks(t, got, tt.want)

			// Whatever it did, the result must still cover everything the
			// input covered. Consolidation may widen a plan; it may never
			// narrow one.
			if len(tt.in) > 0 {
				if got[0].Start.After(tt.in[0].Start) {
					t.Errorf("consolidated plan starts at %s, after the input's %s",
						got[0].Start.Format(time.RFC3339), tt.in[0].Start.Format(time.RFC3339))
				}

				if got[len(got)-1].End.Before(tt.in[len(tt.in)-1].End) {
					t.Errorf("consolidated plan ends at %s, before the input's %s",
						got[len(got)-1].End.Format(time.RFC3339),
						tt.in[len(tt.in)-1].End.Format(time.RFC3339))
				}
			}
		})
	}
}

// TestConsolidateThenVerifyCoverage runs the real sequence — Expand, then
// Consolidate — and re-checks the invariant Expand guarantees, because
// consolidation is the one step that can legitimately widen a plan and a widened
// plan must still be a covering one.
func TestConsolidateThenVerifyCoverage(t *testing.T) {
	t.Parallel()

	spec := Spec{
		Start: utc(2024, 1, 16), End: utc(2024, 3, 20),
		ArchivesThrough: farFuture, HasDaily: true, HasMonthly: true,
	}

	chunks, err := Expand(spec)
	if err != nil {
		t.Fatalf("Expand: %v", err)
	}

	got := Consolidate(chunks, alwaysPublished)

	// January is wanted from the 16th (16 of 31, so consolidated), February
	// whole, March to the 20th (19 of 31, so consolidated too).
	assertChunks(t, got, []Chunk{monthly(2024, 1), monthly(2024, 2), monthly(2024, 3)})

	checkCovers(t, got, spec.Start, spec.End)
}
