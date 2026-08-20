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
// month or a whole day — regardless of how much of it the request wanted, so a
// chunk routinely covers more than was asked for and the reduce step trims the
// result.
//
// # How much more
//
// A month that is wanted in full, or wanted at an interval with no daily
// archives, is always one monthly chunk. The interesting case is a month that
// is published in full and wanted in part, and there the answer is a trade
// rather than a principle:
//
//	                        requests        bytes
//	31 daily archives       62 (a zip and a sidecar each)   only the days wanted
//	1 monthly archive       2                               the whole month
//
// Neither end of that is right for both shapes of request. Twenty-five days of
// January as twenty-five daily downloads is fifty requests to avoid fetching
// six days nobody asked for, and it leaves the cache holding twenty-five files
// that the next request for January cannot use as a month. One day of January
// as a monthly download is 93 MB of 1s candles to serve 86,400 of them.
//
// So the rule is a threshold: take the month once at least half of it is
// wanted, and take days otherwise. Over-fetching really is cheap in a
// cache-backed library — it is the next eleven requests already answered — but
// "cheap" is a claim about the ratio between what was fetched and what was
// wanted, and that ratio is what the threshold bounds.
//
// The rule lives in [Consolidate] rather than in [Expand], because it is only
// sound when the monthly archive actually exists, and only the bucket listing
// knows that.
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
		// requested, or only partly published, is not — it goes to dailies,
		// and [Consolidate] decides afterwards whether the month was the
		// cheaper fetch after all.
		//
		// That split is deliberate and was not always here. The threshold used
		// to live in this loop, tested against ArchivesThrough — which is the
		// later of the monthly and daily frontiers, so it answers "is this
		// period before the frontier?" and not "does a monthly archive for it
		// exist?". Those differ for most of every month, because dailies lag
		// real time by a day and monthlies by up to a month plus that day, and
		// picking a whole-month chunk for a month Binance has not published
		// yet is the worst of both: the substitution then fans out the entire
		// month rather than the days that were wanted. Only the bucket listing
		// can answer the question the trade-off actually rests on, and this
		// package cannot see it — so the decision moved to where it can.
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

// Consolidate replaces runs of daily chunks with the monthly archive covering
// them, wherever that month exists and enough of it is wanted.
//
// monthExists is asked about the first instant of a month and must answer from
// the bucket listing. It is a parameter rather than a lookup because this
// package has no network and is not allowed one — see the package comment — so
// the caller supplies the one fact the trade-off turns on.
//
// # Why this is not part of Expand
//
// The threshold trades requests against bytes: one monthly download instead of
// up to sixty-two daily ones, at the cost of fetching days nobody asked for.
// That trade is only worth making if the monthly archive is there. If it is
// not, the chunk 404s and [Substitute] fans it back out into *every* day of the
// month rather than the days that were wanted — strictly worse than never
// having consolidated, and worse than the plan before the threshold existed.
//
// This used to be decided inside [Expand], against Spec.ArchivesThrough, which
// cannot answer the question: it is the later of the monthly and daily
// frontiers, so for most of every month it says "published" about a month whose
// archive Binance has not written yet. Daily archives lag real time by about a
// day and monthly ones by up to a month plus that day, so the window in which
// the two disagree is not an edge case — it is most of the time.
//
// # What it will not do
//
// Cross a month boundary, reorder anything, or touch a chunk that is not a
// daily archive. Runs are maximal within one calendar month, and the monthly
// chunk that replaces one covers at least the run's own span, so coverage is
// preserved — wider, never narrower, which is what [Chunk] documents as normal.
func Consolidate(chunks []Chunk, monthExists func(time.Time) bool) []Chunk {
	if monthExists == nil {
		// No listing, no upgrade. Returning the plan unchanged is the safe
		// direction: it fetches exactly what was asked for.
		return chunks
	}

	out := make([]Chunk, 0, len(chunks))

	for i := 0; i < len(chunks); {
		// Anything that is not a daily archive passes straight through.
		if chunks[i].Kind != KindDailyArchive {
			out = append(out, chunks[i])
			i++

			continue
		}

		// Take every consecutive daily chunk that falls inside this month, and
		// note whether they were contiguous.
		//
		// Contiguity is checked rather than assumed. Expand emits contiguous
		// days, so a gap cannot arrive from there — but if one ever did, the
		// month covering the days *around* the gap also covers the gap, and
		// emitting it beside the days that were kept would put two chunks over
		// the same dates. That is not a data error (the reduce step
		// deduplicates on open time) but it is a download of the same month
		// twice over, which is the opposite of what consolidating is for. A
		// month whose days do not form one run is therefore left alone
		// entirely.
		mStart := monthStart(chunks[i].Start)
		mEnd := mStart.AddDate(0, 1, 0)

		j, contiguous := i+1, true

		for j < len(chunks) && chunks[j].Kind == KindDailyArchive && chunks[j].Start.Before(mEnd) {
			if !chunks[j].Start.Equal(chunks[j-1].End) {
				contiguous = false
			}

			j++
		}

		if contiguous && monthExists(mStart) && worthWholeMonth(chunks[i].Start, chunks[j-1].End, mStart, mEnd) {
			out = append(out, Chunk{KindMonthlyArchive, mStart, mEnd})
		} else {
			out = append(out, chunks[i:j]...)
		}

		i = j
	}

	return out
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
// The returned chunks are contiguous and cover *at least* the range passed in,
// so a caller can splice them in where the chunk used to be. At least, rather
// than exactly, for the reason [Chunk] gives: a daily archive is a whole day
// and cannot be cut, so replacing a chunk that begins at 07:00 means beginning
// at the midnight before it. Substituting is therefore the one operation that
// can make a plan overlap itself, and the reduce step — which deduplicates on
// open time — is what absorbs that.
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

		// dayStart, not c.Start. Binance names a daily archive after a date
		// and nothing else, so every chunk of this kind must sit at midnight
		// UTC — a loop starting from c.Start verbatim yields 07:00 chunks for
		// a 07:00 input, which name archives that do not exist and 404 their
		// way to the REST API one day at a time. Expand only ever emits
		// month-aligned monthly chunks, so nothing reaches this with a ragged
		// start today; snapping here is what keeps that a fact about Expand
		// rather than a precondition this function silently depends on.
		var days []Chunk
		for d := dayStart(c.Start); d.Before(c.End); d = d.AddDate(0, 0, 1) {
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

// day is the length of a calendar day in UTC, which is the only calendar this
// package deals in. Named so that the divisions in [worthWholeMonth] read as
// counting days rather than as duration arithmetic.
//
// It is exactly 24 hours because these instants are UTC and UTC has no
// daylight saving. The same expression against a zoned time would be wrong
// twice a year, which is why every boundary in this package is built with
// time.Date in time.UTC rather than by adding durations.
const day = 24 * time.Hour

// worthWholeMonth reports whether a month wanted only in part is nonetheless
// cheaper to fetch as its monthly archive. [Consolidate] is the only caller.
//
// from and to bound the part still wanted; mStart and mEnd bound the month.
// The rule is half: 16 days of a 31-day January take the month, 15 take the
// days. See [Chunk] for the two costs being traded off, and note that both
// sides of the trade are bounded — the worst monthly over-fetch is just under
// 2× what was wanted, and the worst daily request count is 62.
//
// The division truncates, and deliberately: a partial day at either end is not
// a day's worth of archive, and rounding it up would let a range of a few
// hours either side of midnight count as two days. Truncating makes the
// threshold err towards daily archives, which is the cheaper mistake — it
// fetches too little rather than too much, and the next request warms the
// cache properly.
func worthWholeMonth(from, to, mStart, mEnd time.Time) bool {
	wanted := int(to.Sub(from) / day)
	total := int(mEnd.Sub(mStart) / day)

	return wanted*2 >= total
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
