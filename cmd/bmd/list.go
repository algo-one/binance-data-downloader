package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// list reports what Binance publishes for one symbol and interval.
//
// It answers "how far back does this pair go, and is anything missing" without
// downloading a single archive: two bucket listings, and nothing else.
func list(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("bmd list", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		symbol   = fs.String("symbol", "", "trading pair: BTC/USDT, BTC-USDT or BTCUSDT")
		interval = fs.String("interval", "", "candle interval: 1s, 1m, 1h, 1d, 1w, 1mo ...")
		market   = fs.String("market", "spot", "market to read: spot")
		since    = fs.String("since", "", "only look from this date onwards (YYYY-MM-DD or RFC 3339, UTC)")
		showAll  = fs.Bool("archives", false, "print every archive rather than a summary")

		common commonFlags
	)

	common.registerVerbose(fs)

	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, `bmd list - show what Binance publishes for a symbol and interval

Usage:
  bmd list -symbol BTC/USDT -interval 1h [-since 2024-01-01]

Nothing is downloaded. -since makes this faster too: the tool jumps straight
to that date in Binance's file list, instead of reading through every year
before it. Asking about one year costs one network request. Asking about a
pair's whole history can cost seven.

Flags:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() > 0 {
		return usagef("unexpected argument %q; every value is given with a flag", fs.Arg(0))
	}

	normalized, iv, err := parseSymbolInterval(*symbol, *interval)
	if err != nil {
		return err
	}

	m, err := parseMarket(*market)
	if err != nil {
		return err
	}

	query := binancedata.AvailabilityQuery{Symbol: normalized, Interval: iv, Market: m}

	if *since != "" {
		if query.Since, err = parseStart(*since); err != nil {
			// parseStart names the flag it was given, which is "start".
			return usagef("-since %q: want YYYY-MM-DD or an RFC 3339 timestamp", *since)
		}
	}

	opts, err := common.options(fs, stderr)
	if err != nil {
		return err
	}

	l, err := newLoader(opts...)
	if err != nil {
		return err
	}

	available, err := l.Available(ctx, query)
	if err != nil {
		return err
	}

	return writeAvailability(stdout, available, *showAll)
}

// writeAvailability renders the answer.
//
// text/tabwriter rather than fixed-width formatting: the columns hold dates and
// counts whose widths are known, but the labels are not, and a table that
// realigns itself cannot drift out of alignment when a label is reworded.
func writeAvailability(w io.Writer, a binancedata.Availability, showAll bool) error {
	if _, err := fmt.Fprintf(w, "%s %s %s\n\n", a.Symbol, a.Interval, a.Market); err != nil {
		return err
	}

	// Nothing at all is a real answer and deserves a sentence rather than an
	// empty table. It means the bucket was asked and said there is nothing
	// here — a pair that never traded, or one whose name is misspelt.
	if len(a.Monthly) == 0 && len(a.Daily) == 0 {
		_, err := fmt.Fprintln(w, "no archives published")

		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	writeSpan(tw, "monthly archives", a.Monthly, a.MonthlyGaps())
	writeSpan(tw, "daily archives", a.Daily, a.DailyGaps())

	// The frontier, which is the one number this whole command exists to make
	// visible: everything at or after it has to come from the REST API,
	// because no archive covers it yet.
	if !a.ArchivesThrough.IsZero() {
		_, _ = fmt.Fprintf(tw, "archives through\t\t%s\n", a.ArchivesThrough.Format(dateLayout))
	}

	if err := tw.Flush(); err != nil {
		return err
	}

	if showAll {
		return writeArchiveList(w, a)
	}

	return nil
}

// writeSpan prints one granularity's row: how many, over what range, and how
// many holes.
func writeSpan(w io.Writer, label string, published, gaps []time.Time) {
	if len(published) == 0 {
		return
	}

	first, last := published[0], published[len(published)-1]

	_, _ = fmt.Fprintf(w, "%s\t%d\t%s .. %s", label, len(published),
		first.Format(dateLayout), last.Format(dateLayout))

	// A gap count is not decoration. Binance's archives genuinely have holes —
	// BTCUSDT-1mo-2024-03 does not exist while its neighbours do — and a
	// summary that reported only the first and last date would describe that
	// range as complete.
	if len(gaps) > 0 {
		_, _ = fmt.Fprintf(w, "\t%d missing", len(gaps))
	}

	_, _ = fmt.Fprintln(w)
}

// writeArchiveList prints every period in order, marking the holes.
//
// The summary above says how many archives are missing; this is what says
// which. The two lists are merged chronologically rather than printed one after
// the other, because a hole matters in relation to its neighbours — "2024-03 is
// missing" is a fact, and seeing it sitting between 2024-02 and 2024-04 is the
// thing that makes it obvious.
func writeArchiveList(w io.Writer, a binancedata.Availability) error {
	sections := []struct {
		label     string
		published []time.Time
		gaps      []time.Time
	}{
		{"monthly", a.Monthly, a.MonthlyGaps()},
		{"daily", a.Daily, a.DailyGaps()},
	}

	for _, s := range sections {
		if len(s.published) == 0 {
			continue
		}

		if _, err := fmt.Fprintf(w, "\n%s\n", s.label); err != nil {
			return err
		}

		for _, e := range mergeByTime(s.published, s.gaps) {
			mark := ""
			if e.missing {
				mark = "  MISSING"
			}

			if _, err := fmt.Fprintf(w, "  %s%s\n", e.when.Format(dateLayout), mark); err != nil {
				return err
			}
		}
	}

	return nil
}

// archiveLine is one period and whether it is there.
type archiveLine struct {
	when    time.Time
	missing bool
}

// mergeByTime interleaves the published periods with the missing ones.
//
// Both inputs are already ascending — Availability sorts what the bucket
// returned, and the gaps are produced by walking that order — so this is the
// merge step of a merge sort and needs no comparison-based sort at all. The
// gaps also fall strictly between the first and last published period, which
// is what guarantees the result is a contiguous run.
func mergeByTime(published, gaps []time.Time) []archiveLine {
	out := make([]archiveLine, 0, len(published)+len(gaps))
	i, j := 0, 0

	for i < len(published) && j < len(gaps) {
		if published[i].Before(gaps[j]) {
			out = append(out, archiveLine{when: published[i]})
			i++

			continue
		}

		out = append(out, archiveLine{when: gaps[j], missing: true})
		j++
	}

	for ; i < len(published); i++ {
		out = append(out, archiveLine{when: published[i]})
	}

	for ; j < len(gaps); j++ {
		out = append(out, archiveLine{when: gaps[j], missing: true})
	}

	return out
}
