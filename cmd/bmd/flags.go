package main

// Flag plumbing shared by the five commands: how a date on a command line
// becomes a time.Time, how a repeatable list flag collects its values, and how
// the flags every command has in common become options for
// binancedata.NewLoader.

import (
	"context"
	"flag"
	"io"
	"iter"
	"log/slog"
	"os"
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
//	evict      -cache-dir                -quiet  -verbose
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
		"where the two-tier cache lives (default: $BMD_CACHE_DIR, else the user cache directory)")
}

// cacheDirEnv names the cache directory when no -cache-dir flag is given.
//
// It carries the command's own name as a prefix, which is the convention that
// keeps environment variables apart: the environment is one flat namespace
// shared by every process in a shell, so a bare CACHE_DIR would be read by this
// tool and by anything else that happened to guess the same name.
//
// It is the CLI that reads this and not the library. binancedata is a package
// other programs import, and a package that reads the environment on its own
// can have a variable exported for some entirely unrelated reason silently
// redirect where its caller's program writes files. Code says where its cache
// lives by calling WithCacheDir. A person says it by exporting this.
const cacheDirEnv = "BMD_CACHE_DIR"

// lookupEnv reads one environment variable. It is a package-level variable
// rather than a direct call, for the same reason newLoader below is one: a test
// replaces it.
//
// The obvious alternative, testing.T.Setenv, edits the real process
// environment, and that is worse here than it first sounds. The default this
// setting falls back to is os.UserCacheDir, which itself reads XDG_CACHE_HOME
// on Linux — so a test that writes to the process environment is a test that
// can move the very default it is trying to assert against. Swapping the lookup
// leaves the real environment untouched.
//
// os.LookupEnv rather than os.Getenv, and the second return value is the whole
// reason: Getenv answers "" both for a variable that was never set and for one
// set to the empty string, and those two need different answers below.
var lookupEnv = os.LookupEnv

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
//
// The cache directory has a third source beyond the flag and the default — the
// BMD_CACHE_DIR environment variable — and resolveCacheDir below settles the
// three against each other. It runs after the Visit walk on purpose: the walk
// is what rejects -cache-dir "", and the environment may only speak for a flag
// that was genuinely absent.
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

	cacheDir, err := c.resolveCacheDir(fs)
	if err != nil {
		return nil, err
	}

	var opts []binancedata.Option

	if cacheDir != "" {
		opts = append(opts, binancedata.WithCacheDir(cacheDir))
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

// resolveCacheDir decides which cache directory this command should use.
//
// The order is the one every command-line tool uses, and it is worth stating
// because people rely on it without reading anything: the flag wins, then the
// environment variable, then the library's own default. The more specific
// instruction beats the less specific one — what was typed for this single run
// beats what was exported once into a shell.
//
// An empty return means "say nothing and let the library choose". WithCacheDir
// rejects an empty directory, correctly, so "no preference" cannot be expressed
// by handing the option a zero value; it is expressed by not appending the
// option at all, which is what the caller above does.
//
// # Why the FlagSet is consulted and not the value alone
//
// Two different questions are being answered here, and neither one is visible
// in c.cacheDir.
//
// The first is whether this command honours a cache directory at all. fs.Lookup
// returns nil for a flag that was never registered, and `bmd list` never
// registers -cache-dir because it opens no cache. Reading the environment there
// would hand that command a setting it advertises nowhere and acts on never —
// the accepted-and-ignored setting docs/architecture.md calls a defect rather
// than a stub. The environment is held to exactly the same rule as the flags,
// so `BMD_CACHE_DIR=/tmp/x bmd list` changes nothing, the way -cache-dir cannot.
//
// The second is whether the flag was given. options has already rejected
// -cache-dir "" by the time this runs, so an empty c.cacheDir can only mean the
// flag was absent — and absent is the single case in which the environment
// gets a turn.
func (c *commonFlags) resolveCacheDir(fs *flag.FlagSet) (string, error) {
	if c.cacheDir != "" {
		return c.cacheDir, nil
	}

	if fs.Lookup("cache-dir") == nil {
		return "", nil
	}

	dir, ok := lookupEnv(cacheDirEnv)
	if !ok {
		return "", nil
	}

	// Set but empty is a mistake, not a request for the default, and it is the
	// same mistake the -cache-dir "" guard above exists for wearing different
	// clothes: `export BMD_CACHE_DIR="$CACHE_DIR"` with CACHE_DIR unset exports
	// an empty string, and the shell reports nothing. Falling back quietly there
	// aims a deleting command — `bmd evict -all`, `bmd verify -rm` — at the
	// user's real cache while whoever wrote the export believes it is pointed at
	// a scratch directory.
	//
	// TrimSpace is used for the test and not for the value. A directory name may
	// legally begin or end with a space on Unix, so trimming what is returned
	// would silently rewrite somebody's path; trimming only the emptiness check
	// still catches BMD_CACHE_DIR=" ", which is that same shell mistake with a
	// stray quote in it.
	if strings.TrimSpace(dir) == "" {
		return "", usagef(
			"%s is set but empty; unset it entirely for the default cache directory", cacheDirEnv)
	}

	return dir, nil
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
	EvictCache(ctx context.Context, opts binancedata.EvictOptions) iter.Seq2[binancedata.EvictResult, error]
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

// parseInterval resolves one -interval value.
//
// Split out of parseSymbolInterval because `bmd download` takes lists of both
// symbols and intervals, so neither is parsed alongside the other there any
// more. Validation is left to the library — ParseInterval knows the sixteen
// intervals and both spellings of the monthly one — but the error is rewritten
// as a usage error, because a bad flag value is a typing mistake and deserves
// the exit status that says so.
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

// parseIntervals resolves every -interval value and drops the duplicates.
//
// It is parseSymbols' counterpart, and the deduplication is there for the same
// reason rather than for tidiness: an [binancedata.Interval] appears in the
// generated output name, so two flag values that mean one interval would give
// two downloads the same path, each writing its own temporary file, with the
// second rename silently replacing the first's work.
//
// The duplicates are not hypothetical here the way "BTC/USDT versus BTCUSDT"
// is. Binance itself spells the monthly interval two ways — "1mo" in an archive
// path and "1M" in a REST parameter — ParseInterval accepts both on purpose,
// and `-interval 1mo,1M` is one interval written the way each half of Binance
// writes it. Deduplicating after parsing is the only point at which that is
// visible; before it they are two different strings.
func parseIntervals(values []string) ([]binancedata.Interval, error) {
	// The same backstop parseSymbols carries, for the same reason: download()
	// runs checkListFlag first and has a better message, and without this an
	// empty slice would produce zero requests and a run that reports "0 of 0"
	// and exits 0 — a silent success for a command that downloaded nothing.
	if len(values) == 0 {
		return nil, usagef("-interval is required, for example 1h")
	}

	var (
		out  = make([]binancedata.Interval, 0, len(values))
		seen = make(map[binancedata.Interval]bool, len(values))
	)

	for _, value := range values {
		iv, err := parseInterval(value)
		if err != nil {
			return nil, err
		}

		if seen[iv] {
			continue
		}

		seen[iv] = true

		out = append(out, iv)
	}

	return out, nil
}

// listFlag collects a flag that may be given more than once and may hold a
// comma-separated list each time. `bmd download` registers two of them, -symbol
// and -interval, and all four of these spellings are equivalent:
//
//	bmd download -symbol BTC/USDT,ETH/USDT
//	bmd download -symbol BTC/USDT -symbol ETH/USDT
//	bmd download -interval 1h,1d
//	bmd download -interval 1h -interval 1d
//
// Both spellings exist because both are what people already have. A list is
// what somebody types by hand; repetition is what a shell loop building an
// argument slice produces. Supporting one and not the other would send whoever
// has the wrong one off to write a join or a split.
//
// A comma is safe as the separator for both flags because neither value can
// contain one: NormalizeSymbol accepts letters, digits and a single "/" or "-"
// separator and rejects everything else, and the sixteen interval spellings are
// a digit and a letter.
//
// # Why this is a flag.Value
//
// It is the only way stdlib flag lets one flag appear twice. flag.String keeps
// the last occurrence and silently discards the earlier ones, so
// `-symbol BTC/USDT -symbol ETH/USDT` would download ETHUSDT alone and say
// nothing about the symbol it dropped.
//
// # Why one type for both flags
//
// Because the alternative is two types differing in nothing, and a second copy
// of Set is a second place for the comma handling to drift. The values are
// strings here and are parsed by the flag's own parser afterwards — parseSymbols
// for one, parseIntervals for the other — so nothing about a symbol or an
// interval is baked in at this level. What the two flags do share is the shape
// of the mistake this guards: a flag given twice and silently honoured once.
type listFlag []string

// String is flag.Value's renderer, used in the default shown by -h. It is
// deliberately the input spelling rather than Go syntax, so the help text reads
// like something you could type.
func (s *listFlag) String() string {
	if s == nil {
		return ""
	}

	return strings.Join(*s, ",")
}

// Set appends one occurrence's worth of values, splitting it on commas.
//
// An occurrence that yields nothing appends nothing and is not reported here.
// It is a usage error, and this is the wrong place to raise one: stdlib flag
// renders a Set failure with %v rather than %w, so an error returned here
// reaches the caller with its chain flattened, errors.Is cannot find errUsage in
// it, and report gives it exit status 1 where every other bad flag value gets 2.
// checkListFlag below raises it instead, where the wrapping survives.
func (s *listFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*s = append(*s, part)
		}
	}

	return nil
}

// checkListFlag reports an absent list flag and an empty one differently.
//
// The distinction lives in the FlagSet rather than in the value, exactly as it
// does in commonFlags.options: fs.Visit walks only the flags actually set on the
// command line, so it separates "-symbol was never given" from "-symbol was
// given as an empty string", which an empty slice cannot.
//
// It is worth telling apart. `-symbol "$SYMBOLS"` with the variable unset is a
// command that meant to name something, and answering it with "-symbol is
// required" points at a flag that was given, which is the kind of message that
// costs somebody ten minutes. The same shell mistake reaches -interval through
// `-interval "$INTERVALS"`, which is why this takes the flag's name rather than
// naming -symbol itself.
func checkListFlag(fs *flag.FlagSet, name string, values listFlag, example string) error {
	if len(values) > 0 {
		return nil
	}

	given := false

	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			given = true
		}
	})

	if given {
		return usagef("-%s was given but names no %s", name, name)
	}

	return usagef("-%s is required, for example %s", name, example)
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
	// download() runs checkListFlag before buildRequests, and that returns an
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
