// Package plan turns a time range into the list of downloads that covers it.
//
// # Why this is its own package
//
// Every calendar rule in this library lives here, and nothing else does. The
// package imports "errors", "fmt" and "time" — that is the complete list, and
// it is enforced by the compiler rather than by discipline. A package that does
// not import net/http cannot make a network request by accident, however
// carelessly someone edits it later.
//
// That guarantee is worth a small amount of awkwardness. The domain types
// (binancedata.Interval and friends) live in the root package, and the root
// package imports this one, so this package cannot import the root back — Go
// forbids import cycles outright. The workaround is [Spec], which carries the
// two or three facts about an interval this package actually needs as plain
// booleans. It is a little clumsy to fill in, and in exchange the calendar
// logic is testable with no clock, no network, and no fixtures.
//
// # What a plan is
//
// Binance publishes historical klines in three places, and a range of any
// length usually needs all three:
//
//	monthly archive   one ZIP per calendar month     the bulk of any range
//	daily archive     one ZIP per day                whole months not yet published
//	REST range        paginated API calls            the last day or two
//
// [Expand] does the decomposition assuming every archive it names exists.
// Whether they actually exist is a question for the network, so it is asked
// later; [Substitute] is the pure rule for what to do with the answer.
//
// # The invariant that matters
//
// The chunks [Expand] returns are sorted, contiguous, and cover the whole
// requested range. Contiguous means each chunk begins exactly where the
// previous one ended — not a millisecond later — so every instant in the range
// belongs to exactly one chunk. Chunks may extend *past* the requested range at
// either end, because archives are whole days and whole months and cannot be
// cut in half; the reduce step trims the result.
//
// [Expand] checks this itself before returning, on every call. The check is a
// single pass over a handful of chunks, so it costs nothing measurable, and it
// converts the one failure mode nobody would notice — a missing day in the
// middle of a range, returned with no error — into a loud one. The
// implementation this library replaces had two such gaps.
package plan

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidSpec reports that a [Spec] describes something impossible: a range
// that ends before it starts, times that are not UTC, bounds that are not
// aligned to midnight.
//
// A caller of this package cannot cause this — the root package validates the
// user's request long before building a Spec — so in practice it means the
// wiring between the two is wrong. It is a real error rather than a panic
// because a library should not take a process down over its own bug.
var ErrInvalidSpec = errors.New("invalid plan spec")

// ErrCoverageGap reports that [Expand] produced chunks which do not cover the
// requested range without gaps or overlaps.
//
// This is an assertion failure, not a condition any input should produce. It
// exists because the alternative to checking is not "no bug", it is "a silent
// bug": a range missing its last two days looks exactly like a range that had
// no data for its last two days. If this error ever escapes, the arithmetic in
// this file is wrong and the stack trace points at it.
var ErrCoverageGap = errors.New("plan does not cover the requested range")

// Kind is which of the three sources a chunk will be fetched from.
//
// Like the enumerations in the root package it counts from 1, so that the zero
// value of a Chunk is detectably unset rather than silently meaning "monthly".
type Kind uint8

// The three chunk sources.
const (
	KindMonthlyArchive Kind = iota + 1 // one ZIP covering a calendar month
	KindDailyArchive                   // one ZIP covering a single day
	KindRESTRange                      // paginated calls to the REST API
)

// String implements fmt.Stringer, so chunks print readably in test failures and
// log lines. Test output is a user interface too.
func (k Kind) String() string {
	switch k {
	case KindMonthlyArchive:
		return "monthly"
	case KindDailyArchive:
		return "daily"
	case KindRESTRange:
		return "rest"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// Chunk is one unit of work: a source and the half-open range it covers.
//
// For an archive chunk the range is the archive's own extent — a whole calendar
// month or a whole day — regardless of how much of it the request wanted. A
// request for two days in the middle of January produces two daily chunks, but
// a request for a single day at an interval that has no daily archives produces
// one monthly chunk spanning all of January. Downloading a whole month to serve
// one day of it is not waste in a cache-backed library; it is the next eleven
// requests already answered.
type Chunk struct {
	Kind  Kind
	Start time.Time // inclusive
	End   time.Time // exclusive
}

// String implements fmt.Stringer. The format is chosen for reading a diff of
// two plans in a failing test, which is the only place it appears.
func (c Chunk) String() string {
	return fmt.Sprintf("%s[%s,%s)", c.Kind,
		c.Start.Format(time.RFC3339), c.End.Format(time.RFC3339))
}

// Spec is everything [Expand] needs, in types this package can hold without
// importing the root package. See the package comment for why that constraint
// exists.
type Spec struct {
	// Start and End are the half-open range being requested, both UTC. They
	// come from a resolved binancedata.Request, so End has already had "now"
	// substituted for the caller's zero value — this package never asks what
	// time it is.
	Start time.Time
	End   time.Time

	// ArchivesThrough is the first instant no bulk archive covers: everything
	// at or after it must come from the REST API. It must be midnight UTC,
	// because archives are whole days and whole months.
	//
	// This is a fact about what Binance has published, discovered by listing
	// the bucket, and it is a parameter rather than a calculation on purpose.
	// The obvious shortcut is to assume archives lag real time by a day and
	// subtract; that is a guess, and a guess that is wrong for one day drops
	// that day without saying so. Asking the bucket what exists costs one HTTP
	// request and removes the question.
	//
	// The zero value means no archives exist at all, and the whole range
	// resolves to REST. That is the right answer for a symbol listed this
	// week, and it falls out of the arithmetic rather than needing a case.
	ArchivesThrough time.Time

	// HasDaily and HasMonthly are binancedata.Interval.HasDailyArchives and
	// HasMonthlyArchives for the interval being requested. Passing the two
	// answers rather than the Interval is what keeps this package free of the
	// import cycle described in the package comment.
	//
	// Only three intervals differ: 3d, 1w and 1mo have monthly archives but no
	// daily ones, their candles being longer than a day.
	HasDaily   bool
	HasMonthly bool
}

// validate rejects a Spec this package cannot honour. Every check is on the
// wiring rather than on user input, which is why the messages name struct
// fields rather than request parameters.
func (s Spec) validate() error {
	if s.Start.IsZero() {
		return fmt.Errorf("start is zero: %w", ErrInvalidSpec)
	}

	if !s.Start.Before(s.End) {
		return fmt.Errorf("start %s is not before end %s: %w",
			s.Start.Format(time.RFC3339), s.End.Format(time.RFC3339), ErrInvalidSpec)
	}

	for _, f := range []struct {
		name string
		t    time.Time
	}{{"start", s.Start}, {"end", s.End}, {"archivesThrough", s.ArchivesThrough}} {
		if f.t.Location() != time.UTC {
			return fmt.Errorf("%s is %s, not UTC: %w", f.name, f.t.Location(), ErrInvalidSpec)
		}
	}

	if !s.HasDaily && !s.HasMonthly && !s.ArchivesThrough.IsZero() {
		return fmt.Errorf("interval has neither daily nor monthly archives, "+
			"but archivesThrough is set to %s: %w", s.ArchivesThrough.Format(time.RFC3339), ErrInvalidSpec)
	}

	// Archives are whole days and whole months, so a boundary in the middle of
	// a day describes something Binance does not publish. Catching it here
	// beats discovering it as an off-by-a-few-hours gap much later.
	if !s.ArchivesThrough.IsZero() && !s.ArchivesThrough.Equal(dayStart(s.ArchivesThrough)) {
		return fmt.Errorf("archivesThrough %s is not midnight UTC: %w",
			s.ArchivesThrough.Format(time.RFC3339), ErrInvalidSpec)
	}

	return nil
}

// Expand decomposes a range into the chunks that cover it, assuming every
// archive it names exists.
//
// The returned chunks are sorted, contiguous, and cover [Start, End) — see the
// package comment for what that guarantees and why it is checked rather than
// asserted in prose. Chunks may begin before Start or end after End, because
// archives are indivisible.
//
// Availability is not consulted here, because consulting it needs the network
// and this package does not have one. The caller probes the chunks it gets back
// and hands any that turn out to be missing to [Substitute].
func Expand(s Spec) ([]Chunk, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	// The archive portion of the range is whatever part of it precedes the
	// point where archives run out. If ArchivesThrough is zero — no archives
	// published — this is an empty range and the loop below does not run.
	archiveEnd := minTime(s.End, s.ArchivesThrough)

	var chunks []Chunk

	// cur is the first instant not yet covered. Each iteration emits one chunk
	// and advances cur to that chunk's end, which is what makes the result
	// contiguous by construction rather than by careful arithmetic.
	for cur := s.Start; cur.Before(archiveEnd); {
		// Both boundaries are computed from the 1st of the month, never by
		// adding or subtracting from cur. That is the entire fix for the
		// second bug in the ported implementation: it compared a request date
		// against a month-shifted date without normalising either to the 1st,
		// so a range like 2024-12-20 to 2025-01-03 was judged to be inside a
		// single month and its final days were dropped in silence.
		mStart := monthStart(cur)
		mEnd := mStart.AddDate(0, 1, 0)

		// A monthly archive is usable when the whole month falls inside the
		// part of the range archives cover. A month that is only partly
		// requested, or only partly published, is not — it goes to dailies.
		wholeMonthCovered := !mStart.Before(s.Start) && !mEnd.After(archiveEnd)

		switch {
		case s.HasMonthly && wholeMonthCovered:
			chunks = append(chunks, Chunk{KindMonthlyArchive, mStart, mEnd})
			cur = mEnd

		case !s.HasDaily:
			// 3d, 1w and 1mo: there are no daily archives to fall back to, so
			// the monthly archive covers the partial month too and the reduce
			// step trims it. This chunk deliberately extends beyond the
			// requested range at one or both ends.
			chunks = append(chunks, Chunk{KindMonthlyArchive, mStart, mEnd})
			cur = mEnd

		default:
			// A partial month at an interval that has daily archives: emit one
			// chunk per day, from the day containing cur up to whichever comes
			// first, the end of the month or the end of the archive range.
			dayLimit := minTime(mEnd, archiveEnd)
			for d := dayStart(cur); d.Before(dayLimit); d = d.AddDate(0, 0, 1) {
				chunks = append(chunks, Chunk{KindDailyArchive, d, d.AddDate(0, 0, 1)})
			}

			cur = dayLimit
		}
	}

	// Everything the archives do not reach comes from the REST API. The start
	// of that range is the end of the last archive chunk rather than
	// archiveEnd, because the last archive chunk may overshoot — a monthly-only
	// interval emits whole months. Deriving it from what was actually emitted
	// is what keeps the seam airtight regardless of which branch above ran.
	restStart := s.Start
	if n := len(chunks); n > 0 {
		restStart = chunks[n-1].End
	}

	if restStart.Before(s.End) {
		chunks = append(chunks, Chunk{KindRESTRange, restStart, s.End})
	}

	if err := verifyCoverage(chunks, s.Start, s.End); err != nil {
		return nil, err
	}

	return chunks, nil
}

// Substitute returns the chunks to try in place of one that turned out not to
// exist, or an error if there is nothing left to try.
//
// Absence is normal. Binance has published archives with holes in them — the
// 1mo archive for BTCUSDT is missing March 2024 while February and April are
// both present, verified against the live bucket on 2026-08-18 — and no amount
// of date arithmetic predicts that. The fallback ladder is:
//
//	monthly archive ──► daily archives ──► REST range
//	                    (skipped when the interval has none)
//
// The returned chunks cover exactly the same range as the one passed in, so a
// caller can splice them in place without re-checking coverage.
func Substitute(c Chunk, hasDaily bool) ([]Chunk, error) {
	switch c.Kind {
	case KindMonthlyArchive:
		if !hasDaily {
			// 3d, 1w and 1mo have no daily archives, so a missing month can
			// only be filled from the API. This is the case that decided the
			// design: any rule that stopped at "fall back to dailies" would
			// return nothing at all for 1mo March 2024.
			return []Chunk{{KindRESTRange, c.Start, c.End}}, nil
		}

		var days []Chunk
		for d := c.Start; d.Before(c.End); d = d.AddDate(0, 0, 1) {
			days = append(days, Chunk{KindDailyArchive, d, d.AddDate(0, 0, 1)})
		}

		return days, nil

	case KindDailyArchive:
		return []Chunk{{KindRESTRange, c.Start, c.End}}, nil

	case KindRESTRange:
		// The bottom of the ladder. A REST range that comes back empty is a
		// genuine "Binance does not have this", and the caller turns it into
		// binancedata.ErrNotAvailable rather than an empty slice.
		return nil, fmt.Errorf("%s: no source left to try: %w", c, ErrCoverageGap)

	default:
		return nil, fmt.Errorf("chunk with unset kind: %s: %w", c, ErrInvalidSpec)
	}
}

// verifyCoverage checks the invariant documented on the package: sorted,
// non-empty, contiguous, and spanning at least [start, end).
//
// Each of the four checks below corresponds to a way a range can lose data
// without anyone noticing, which is why this runs in production and not only
// under test.
func verifyCoverage(chunks []Chunk, start, end time.Time) error {
	if len(chunks) == 0 {
		return fmt.Errorf("no chunks for range [%s,%s): %w",
			start.Format(time.RFC3339), end.Format(time.RFC3339), ErrCoverageGap)
	}

	// The first chunk must begin at or before the requested start, or the
	// leading edge of the range is missing.
	if chunks[0].Start.After(start) {
		return fmt.Errorf("first chunk %s begins after requested start %s: %w",
			chunks[0], start.Format(time.RFC3339), ErrCoverageGap)
	}

	for i, c := range chunks {
		// An empty or reversed chunk would download nothing while appearing to
		// account for its range.
		if !c.Start.Before(c.End) {
			return fmt.Errorf("chunk %d is empty: %s: %w", i, c, ErrCoverageGap)
		}

		// Equality, not "close enough". A gap of one nanosecond between two
		// chunks is still a gap, and at 1s candles it is a missing candle.
		if i > 0 && !c.Start.Equal(chunks[i-1].End) {
			return fmt.Errorf("chunk %d (%s) does not continue from chunk %d (%s): %w",
				i, c, i-1, chunks[i-1], ErrCoverageGap)
		}
	}

	// The trailing edge is the one the ported implementation lost twice, in two
	// different code paths, so it gets its own check and its own test.
	if last := chunks[len(chunks)-1]; last.End.Before(end) {
		return fmt.Errorf("last chunk %s ends before requested end %s: %w",
			last, end.Format(time.RFC3339), ErrCoverageGap)
	}

	return nil
}

// monthStart returns midnight UTC on the 1st of t's month.
//
// time.Date normalises out-of-range fields rather than rejecting them, which is
// what makes AddDate(0, 1, 0) on a normalised 1st safe: there is no 31st to
// slide into the following month. Anchoring here rather than at the call sites
// is what makes the month arithmetic in Expand correct by construction.
func monthStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// dayStart returns midnight UTC on t's day.
//
// Deliberately not t.Truncate(24 * time.Hour): Truncate works on the absolute
// duration since the zero time and knows nothing about calendars, so it agrees
// with this function only by coincidence of UTC having no daylight saving. The
// coincidence holds today and is not worth depending on.
func dayStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// minTime returns the earlier of two instants.
//
// Go 1.21 added a builtin min, but it is constrained to ordered types and
// time.Time is a struct, so comparison goes through Before. Writing it out once
// keeps the intent visible at the call sites.
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}
