package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// availabilityWithAHole is the fixture every list test uses: January, February
// and April published, March missing.
//
// It is not an invented shape. BTCUSDT-1mo-2024-03.zip genuinely does not exist
// while 2024-02 and 2024-04 both do, and it is the single finding that rules
// out predicting availability from a calendar — which is what this command
// exists to make visible.
func availabilityWithAHole(t *testing.T) binancedata.Availability {
	t.Helper()

	return binancedata.Availability{
		Symbol:   "BTCUSDT",
		Interval: binancedata.Interval1mo,
		Market:   binancedata.MarketSpot,
		Monthly: []time.Time{
			mustDate(t, "2024-01-01"),
			mustDate(t, "2024-02-01"),
			mustDate(t, "2024-04-01"),
		},
		ArchivesThrough: mustDate(t, "2024-05-01"),
	}
}

// TestListReportsTheSummary checks the default output, whose whole job is to
// say "three archives, January to April, one missing" rather than "January to
// April" — the second is what a calendar would tell you, and it is wrong.
func TestListReportsTheSummary(t *testing.T) {
	f := &fakeLoader{available: availabilityWithAHole(t)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"list", "-symbol", "BTC/USDT", "-interval", "1mo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	checkGolden(t, "list-summary.txt", stdout.Bytes())

	// The query carried the normalised symbol, not the slashed spelling.
	if got := f.gotQuery.Symbol; got != "BTCUSDT" {
		t.Errorf("Symbol = %q, want the normalised BTCUSDT", got)
	}

	if got := f.gotQuery.Interval; got != binancedata.Interval1mo {
		t.Errorf("Interval = %v, want 1mo", got)
	}

	// No -since means no bound, which is what asks the bucket for the whole
	// history.
	if !f.gotQuery.Since.IsZero() {
		t.Errorf("Since = %s, want the zero time when -since is not given", f.gotQuery.Since)
	}
}

// TestListArchivesShowsEveryPeriod checks -archives, where the point is that
// the missing month appears *between* its neighbours rather than in a footnote.
func TestListArchivesShowsEveryPeriod(t *testing.T) {
	f := &fakeLoader{available: availabilityWithAHole(t)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"list", "-symbol", "BTCUSDT", "-interval", "1mo", "-archives",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	checkGolden(t, "list-archives.txt", stdout.Bytes())
}

// TestListOnASymbolThatNeverTradedSaysSo is the distinction the whole listing
// layer is built around, surfacing at the command line. S3 answers an unknown
// prefix with HTTP 200 and an empty document, so this is a fact rather than a
// failure — and it must read as one rather than as an empty table.
func TestListOnASymbolThatNeverTradedSaysSo(t *testing.T) {
	f := &fakeLoader{available: binancedata.Availability{
		Symbol:   "NOPEUSDT",
		Interval: binancedata.Interval1h,
		Market:   binancedata.MarketSpot,
	}}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"list", "-symbol", "NOPEUSDT", "-interval", "1h",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if !strings.Contains(stdout.String(), "no archives published") {
		t.Errorf("stdout = %q, want it to say plainly that there is nothing", stdout.String())
	}
}

// TestListSincePassesThrough checks that -since reaches the query, which is
// what makes it a seek rather than a filter — and therefore what makes it
// cheaper rather than merely shorter.
func TestListSincePassesThrough(t *testing.T) {
	f := &fakeLoader{available: availabilityWithAHole(t)}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"list", "-symbol", "BTCUSDT", "-interval", "1mo", "-since", "2024-02-01",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if got, want := f.gotQuery.Since, mustDate(t, "2024-02-01"); !got.Equal(want) {
		t.Errorf("Since = %s, want %s", got, want)
	}
}

// TestListReportsAFailedLookup keeps the outcome that must never be confused
// with an empty one. A listing that failed means we do not know what Binance
// has; reporting it as "nothing published" would be a lie with a nil error
// attached.
func TestListReportsAFailedLookup(t *testing.T) {
	f := &fakeLoader{availableErr: errors.New("listing the bucket: connection refused")}
	f.install(t)

	var stdout, stderr bytes.Buffer

	err := run(t.Context(), []string{
		"list", "-symbol", "BTCUSDT", "-interval", "1h",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("run returned nil, want the lookup's error")
	}

	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %v, want the underlying message", err)
	}

	// It is a failure, not a usage mistake: the command line was fine.
	if errors.Is(err, errUsage) {
		t.Error("a failed listing was reported as a usage error")
	}

	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want nothing when the lookup failed", stdout.String())
	}
}
