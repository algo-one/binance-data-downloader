package main

// Flag plumbing shared by the three commands: how a date on a command line
// becomes a time.Time, and how the flags every command has in common become
// options for binancedata.NewLoader.

import (
	"context"
	"flag"
	"io"
	"iter"
	"log/slog"
	"strings"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// dateLayout is the spelling this tool leads with, and the only one that gets
// the end-of-day treatment below.
const dateLayout = "2006-01-02"

// parseStart turns a -start value into the first instant of the range.
//
// A bare date means midnight UTC on that day, which is the only reading anybody
// expects, and an RFC 3339 timestamp is taken exactly as written.
func parseStart(value string) (time.Time, error) {
	t, _, err := parseInstant("start", value)

	return t, err
}

// parseEnd turns an -end value into the last instant of the range.
//
// This is where the CLI's inclusive -end and the library's inclusive End meet,
// and the interesting case is the bare date.
//
// binancedata.Request.End is inclusive of an *instant*: a candle is returned
// when its open time is at or before it. Handing 2024-03-31 straight through
// would therefore return, for hourly candles, the single candle that opened at
// midnight on the 31st — because that is the last open time at or before
// midnight. Nobody writing -end 2024-03-31 means one hour of the 31st.
//
// So a bare date is expanded to that day's last instant. Two consequences are
// worth seeing. The whole day is included at every interval, which is what was
// meant. And the exclusive bound the library derives from it — End plus one
// nanosecond — lands exactly on midnight of the next day, which is a chunk
// seam, so the plan is the same one the old half-open spelling produced and no
// extra archive is fetched for the boundary.
//
// A -end written as a full RFC 3339 timestamp is taken exactly as given. Some-
// body who wrote the time out has said which instant they mean.
func parseEnd(value string) (time.Time, error) {
	t, dateOnly, err := parseInstant("end", value)
	if err != nil {
		return time.Time{}, err
	}

	if dateOnly {
		// The last instant of the day, at nanosecond resolution. Adding a day
		// and subtracting one nanosecond rather than writing 23:59:59.999999999
		// out: AddDate handles the month and year rollovers, and there is no
		// literal for a reader to miscount the nines in.
		t = t.AddDate(0, 0, 1).Add(-time.Nanosecond)
	}

	return t, nil
}

// parseInstant accepts either spelling and reports which one it got.
//
// Both are parsed as UTC. time.Parse with a layout that carries no zone yields
// a time in UTC already, and RFC 3339 requires an explicit offset, which .UTC()
// then normalises — the library rejects any Start or End whose location is not
// time.UTC, so this conversion is not a convenience but a requirement.
func parseInstant(field, value string) (t time.Time, dateOnly bool, err error) {
	if value == "" {
		return time.Time{}, false, usagef("-%s is required", field)
	}

	if t, err := time.Parse(dateLayout, value); err == nil {
		return t.UTC(), true, nil
	}

	t, err = time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false, usagef(
			"-%s %q: want YYYY-MM-DD or an RFC 3339 timestamp such as 2024-01-15T12:00:00Z",
			field, value)
	}

	return t.UTC(), false, nil
}

// commonFlags holds the flags more than one command takes.
//
// They are registered one at a time rather than as a block, and that is the
// whole point of the four methods below. docs/architecture.md states the rule
// they exist to keep: an accepted-and-ignored setting is a defect, not a stub.
// A -concurrency on `bmd verify`, whose walk is sequential, or a -cache-dir on
// `bmd list`, which never opens the cache, would be a flag that a user can set,
// that the help text advertises, and that changes nothing — which is worse than
// not offering it, because it takes a debugging session to find out.
//
// So each command registers exactly what it honours:
//
//	download   -cache-dir  -concurrency  -quiet  -verbose
//	verify     -cache-dir                -quiet  -verbose
//	prune      -cache-dir                -quiet  -verbose
//	cache      -cache-dir                        -verbose
//	list                                         -verbose
//
// `cache` has no -quiet because it has nothing to suppress: its report is its
// output and goes to stdout, where -quiet would leave a command that printed
// nothing at all.
type commonFlags struct {
	cacheDir    string
	concurrency int
	quiet       bool
	verbose     bool
}

// registerCacheDir is for the commands that read or write the cache.
func (c *commonFlags) registerCacheDir(fs *flag.FlagSet) {
	fs.StringVar(&c.cacheDir, "cache-dir", "",
		"where the two-tier cache lives (default: the user cache directory)")
}

// registerConcurrency is for the commands that fetch in parallel, which is
// download alone: the listing behind `list` is two requests the library already
// runs concurrently, and the verification walk is sequential by nature.
func (c *commonFlags) registerConcurrency(fs *flag.FlagSet) {
	fs.IntVar(&c.concurrency, "concurrency", 0,
		"how many chunks to fetch at once (default: 8)")
}

// registerQuiet is for the commands with progress or a summary to suppress.
func (c *commonFlags) registerQuiet(fs *flag.FlagSet) {
	fs.BoolVar(&c.quiet, "quiet", false, "print nothing to stderr but errors")
}

// registerVerbose is for every command, because every command builds a loader
// and the loader logs what it decided.
func (c *commonFlags) registerVerbose(fs *flag.FlagSet) {
	fs.BoolVar(&c.verbose, "verbose", false, "log what the pipeline is doing to stderr")
}

// options turns the common flags into loader options.
//
// The zero values are left out rather than passed as zero. WithConcurrency
// rejects a non-positive count and WithCacheDir rejects an empty directory —
// correctly, since both are caller errors in a Go program — so "the flag was
// not given" has to be expressed by not calling the option at all.
//
// # Why this returns an error, and why it needs the FlagSet
//
// Because "left out" and "rejected" are different answers to different
// questions, and testing the value alone cannot tell them apart. A bare
// `if c.concurrency > 0` covers both: it skips the option when the flag was
// absent, which is right, and it *also* skips it when somebody typed
// -concurrency -4, which is not — that runs at the default 8 and says nothing,
// which is the accepted-and-ignored setting docs/architecture.md calls a defect
// rather than a stub.
//
// The distinction lives in the FlagSet rather than in the value. fs.Visit walks
// only the flags actually set on the command line, so it separates "-cache-dir
// was never given" from "-cache-dir was given as an empty string", which the
// string itself cannot. That second case is not hypothetical: it is what
// `bmd verify -cache-dir "$CACHE_DIR" -rm` does when CACHE_DIR is unset, and
// silently defaulting there points a deleting command at the user's real cache.
//
// Visit also only ever sees flags this command registered, so the switch below
// needs no guard for `bmd verify` having no -concurrency: it was never
// registered, so it can never have been set.
func (c *commonFlags) options(fs *flag.FlagSet, stderr io.Writer) ([]binancedata.Option, error) {
	// Visit takes a func with no error return and cannot be stopped early, so
	// the first failure is captured here and the rest of the walk is skipped.
	var bad error

	fs.Visit(func(f *flag.Flag) {
		if bad != nil {
			return
		}

		switch f.Name {
		case "cache-dir":
			if c.cacheDir == "" {
				bad = usagef(`-cache-dir is empty; omit it entirely for the default cache directory`)
			}

		case "concurrency":
			if c.concurrency < 1 {
				bad = usagef("-concurrency %d: must be at least 1", c.concurrency)
			}
		}
	})

	if bad != nil {
		return nil, bad
	}

	var opts []binancedata.Option

	if c.cacheDir != "" {
		opts = append(opts, binancedata.WithCacheDir(c.cacheDir))
	}

	if c.concurrency > 0 {
		opts = append(opts, binancedata.WithConcurrency(c.concurrency))
	}

	if c.verbose {
		opts = append(opts, binancedata.WithLogger(slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))))
	}

	return opts, nil
}

// newLoader builds the real loader. It is a variable rather than a function so
// that a test can replace it, which is the only way a test in package main can
// reach the pipeline at all: the options that aim a Loader at an httptest
// server are unexported, so only the library's own tests can use them.
//
// Everything below this line in the CLI therefore talks to one of the three
// small interfaces in this file, and the tests supply their own implementations.
var newLoader = func(opts ...binancedata.Option) (loader, error) {
	return binancedata.NewLoader(opts...)
}

// loader is every method of *binancedata.Loader that this CLI uses.
//
// Declaring the interface here, next to its consumers rather than beside the
// type that satisfies it, is the Go convention and the useful direction: the
// library does not know this interface exists, and adding a method to Loader
// cannot break it.
type loader interface {
	Stream(ctx context.Context, req binancedata.Request) iter.Seq2[binancedata.Kline, error]
	Available(ctx context.Context, q binancedata.AvailabilityQuery) (binancedata.Availability, error)
	VerifyCache(ctx context.Context) iter.Seq2[binancedata.CacheEntry, error]
	CacheUsage(ctx context.Context) (binancedata.CacheUsage, error)
	PruneArchives(ctx context.Context, opts binancedata.PruneOptions) iter.Seq2[binancedata.PruneResult, error]
}

// parseSymbolInterval is the pair of flags every command that names data takes.
//
// Validation is left to the library rather than repeated here — ParseInterval
// knows the sixteen intervals and both spellings of the monthly one, and
// NormalizeSymbol knows the three symbol formats — but the errors are rewritten
// as usage errors, because a bad flag value is a typing mistake and deserves
// the exit status that says so.
func parseSymbolInterval(symbol, interval string) (string, binancedata.Interval, error) {
	if strings.TrimSpace(symbol) == "" {
		return "", 0, usagef("-symbol is required, for example BTC/USDT")
	}

	normalized, err := binancedata.NormalizeSymbol(symbol)
	if err != nil {
		return "", 0, usagef("-symbol %q: %v", symbol, err)
	}

	iv, err := parseInterval(interval)
	if err != nil {
		return "", 0, err
	}

	return normalized, iv, nil
}

// parseInterval resolves the -interval flag.
//
// Split out of parseSymbolInterval because `bmd download` takes several symbols
// against one interval, so the two are no longer parsed together there.
// Validation is left to the library — ParseInterval knows the sixteen intervals
// and both spellings of the monthly one — but the error is rewritten as a usage
// error, because a bad flag value is a typing mistake and deserves the exit
// status that says so.
func parseInterval(value string) (binancedata.Interval, error) {
	if strings.TrimSpace(value) == "" {
		return 0, usagef("-interval is required, for example 1h")
	}

	iv, err := binancedata.ParseInterval(value)
	if err != nil {
		return 0, usagef("-interval %q: %v", value, err)
	}

	return iv, nil
}

// symbolList collects the -symbol flag, which may be given more than once and
// may hold a comma-separated list each time. These two are equivalent:
//
//	bmd download -symbol BTC/USDT,ETH/USDT
//	bmd download -symbol BTC/USDT -symbol ETH/USDT
//
// Both spellings exist because both are what people already have. A list is
// what somebody types by hand; repetition is what a shell loop building an
// argument slice produces. Supporting one and not the other would send whoever
// has the wrong one off to write a join or a split.
//
// A comma is safe as the separator because no symbol can contain one:
// NormalizeSymbol accepts letters, digits and a single "/" or "-" separator,
// and rejects everything else.
//
// # Why this is a flag.Value
//
// It is the only way stdlib flag lets one flag appear twice. flag.String keeps
// the last occurrence and silently discards the earlier ones, so
// `-symbol BTC/USDT -symbol ETH/USDT` would download ETHUSDT alone and say
// nothing about the symbol it dropped.
type symbolList []string

// String is flag.Value's renderer, used in the default shown by -h. It is
// deliberately the input spelling rather than Go syntax, so the help text reads
// like something you could type.
func (s *symbolList) String() string {
	if s == nil {
		return ""
	}

	return strings.Join(*s, ",")
}

// Set appends one occurrence's worth of symbols, splitting it on commas.
//
// An occurrence that yields nothing appends nothing and is not reported here.
// It is a usage error, and this is the wrong place to raise one: stdlib flag
// renders a Set failure with %v rather than %w, so an error returned here
// reaches the caller with its chain flattened, errors.Is cannot find errUsage in
// it, and report gives it exit status 1 where every other bad flag value gets 2.
// checkSymbolFlag below raises it instead, where the wrapping survives.
func (s *symbolList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}

	return nil
}

// checkSymbolFlag reports an absent -symbol and an empty one differently.
//
// The distinction lives in the FlagSet rather than in the value, exactly as it
// does in commonFlags.options: fs.Visit walks only the flags actually set on the
// command line, so it separates "-symbol was never given" from "-symbol was
// given as an empty string", which an empty slice cannot.
//
// It is worth telling apart. `-symbol "$SYMBOLS"` with the variable unset is a
// command that meant to name something, and answering it with "-symbol is
// required" points at a flag that was given, which is the kind of message that
// costs somebody ten minutes.
func checkSymbolFlag(fs *flag.FlagSet, symbols symbolList) error {
	if len(symbols) > 0 {
		return nil
	}

	given := false

	fs.Visit(func(f *flag.Flag) {
		if f.Name == "symbol" {
			given = true
		}
	})

	if given {
		return usagef("-symbol was given but names no symbol")
	}

	return usagef("-symbol is required, for example BTC/USDT")
}

// parseSymbols normalises every symbol and drops the duplicates.
//
// Deduplication is after normalisation and not before, because that is the only
// point at which duplicates are visible: BTC/USDT, BTC-USDT and BTCUSDT are one
// symbol written three ways, and they all become BTCUSDT here.
//
// It matters more than tidiness. Every symbol gets its own output file, and the
// name is generated from the normalised symbol — so a duplicate that survived
// this would have two downloads writing the same path, each through its own
// temporary file, with the second rename silently replacing the first's work.
func parseSymbols(symbols []string) ([]string, error) {
	// A backstop, and unreachable from the one caller there is today:
	// download() runs checkSymbolFlag before buildRequests, and that returns an
	// error for every empty case with a better message than this one — it can
	// tell "never given" from "given and names nothing", which a slice cannot.
	//
	// It stays because of what the alternative fails as. Without it an empty
	// slice produces zero requests, runDownload loops zero times, and the
	// command prints "0 of 0 symbols" and exits 0 — a silent success for a run
	// that downloaded nothing. That is the wrong way for a future second caller
	// to discover it forgot the check.
	if len(symbols) == 0 {
		return nil, usagef("-symbol is required, for example BTC/USDT")
	}

	var (
		out  = make([]string, 0, len(symbols))
		seen = make(map[string]bool, len(symbols))
	)

	for _, symbol := range symbols {
		normalized, err := binancedata.NormalizeSymbol(symbol)
		if err != nil {
			return nil, usagef("-symbol %q: %v", symbol, err)
		}

		if seen[normalized] {
			continue
		}

		seen[normalized] = true

		out = append(out, normalized)
	}

	return out, nil
}

// parseMarket resolves the -market flag.
func parseMarket(value string) (binancedata.Market, error) {
	m, err := binancedata.ParseMarket(value)
	if err != nil {
		return 0, usagef("-market %q: %v", value, err)
	}

	return m, nil
}
