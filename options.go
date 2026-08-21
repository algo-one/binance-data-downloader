package binancedata

// This file holds everything a caller can configure about a [Loader], plus the
// [Progress] value the loader reports work through.
//
// # The functional options pattern
//
// Go has no keyword arguments, no default parameter values and no constructor
// overloading, so a type with six optional settings has a shape problem. The
// three usual answers, and why this one:
//
//	NewLoader(dir string, n int, c *http.Client, ...)   every caller writes
//	                                                    every parameter, and
//	                                                    adding one breaks them
//	                                                    all
//
//	NewLoader(cfg Config)                               a struct of settings.
//	                                                    Workable, but the zero
//	                                                    value of each field has
//	                                                    to be a legal default,
//	                                                    and there is nowhere to
//	                                                    put validation
//
//	NewLoader(opts ...Option)                           this one
//
// An [Option] is a function that mutates a private configuration struct. The
// caller writes only what they want to change, the defaults live in one place,
// adding an option breaks nobody, and — the part that matters most here — an
// option can *reject* what it was given, because it returns an error. The
// project's rule is that validation belongs in something that returns an error
// so it cannot silently fail to run; an option is exactly that.

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/time/rate"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// defaultConcurrency is how many chunks a loader fetches at once when nobody
// says otherwise.
//
// Eight is a compromise between two costs that pull in opposite directions.
// The work is dominated by network round trips, so more workers finish a long
// range sooner — the shared http.Client keeps 64 idle connections per host
// precisely so that a pool this size never re-handshakes. But a worker holds a
// whole decoded archive in memory, and archives are not small: a 1m month is
// about 14 MB of candles and a 1s month is about 810 MB, so eight workers on 1s
// data is already several gigabytes.
//
// Eight is therefore chosen for the common case (1m and coarser, where it costs
// at most ~110 MB) and documented so that anyone fetching 1s data knows to turn
// it down. See [WithConcurrency].
const defaultConcurrency = 8

// Option configures a [Loader]. See [NewLoader].
//
// The pointer receiver is unexported, so this type can be named by callers but
// only constructed by the With* functions in this file. That is deliberate: it
// keeps the configuration struct private, which means fields can be added,
// renamed or removed without it being a breaking change.
type Option func(*loaderConfig) error

// loaderConfig is the accumulated result of applying every [Option].
//
// The fields below the line are test seams rather than public settings. They
// exist because every network path in this package must be testable against an
// httptest.Server, and because time has to be injectable for the calendar rules
// to be assertable. They are unexported, so no consumer can reach them; the
// tests live in this same package and can.
type loaderConfig struct {
	cacheDir    string
	concurrency int
	client      *http.Client
	progress    func(Progress)
	logger      *slog.Logger

	// ---- test seams ----

	// listBaseURL, downloadBaseURL and apiBaseURL override the three hosts.
	// Empty means the real one; see internal/vision for each default.
	listBaseURL     string
	downloadBaseURL string
	apiBaseURL      string

	// now is the clock. Every calendar decision in a fetch is made against one
	// reading of it, taken at the top of the call.
	now func() time.Time

	// policy is the retry behaviour, and limiter the REST budget. Both are
	// zero by default, which internal/vision reads as "use the standard one".
	policy  vision.Policy
	limiter *rate.Limiter

	// minPause and maxPause bound how long a rate limit closes the gate for.
	// Defaulted from the constants in loader.go.
	minPause, maxPause time.Duration
}

// defaultLoaderConfig is the configuration a [Loader] built with no options
// gets. Every field here is a decision; see the constants and comments for
// which, and why.
func defaultLoaderConfig() loaderConfig {
	return loaderConfig{
		concurrency: defaultConcurrency,

		// slog.DiscardHandler, not nil. A nil *slog.Logger panics on use, so
		// defaulting to one would mean a nil check at every call site — and
		// the one that gets forgotten is a panic in a library. Discarding
		// costs a method call that returns false and nothing else.
		//
		// Discarding rather than writing to stderr is the other half of the
		// decision: a library that logs without being asked to is a library
		// that corrupts somebody's stdout-parsing script. slog.DiscardHandler
		// arrived in Go 1.24; this module's floor is 1.25.
		logger: slog.New(slog.DiscardHandler),

		// UTC at the source. Everything downstream requires UTC — Request
		// validation rejects anything else — so converting here means there is
		// one place it can be got wrong instead of a dozen.
		now: func() time.Time { return time.Now().UTC() },

		minPause: minPipelinePause,
		maxPause: maxPipelinePause,
	}
}

// validate checks the assembled configuration.
//
// Every individual option already validates its own argument, so this catches
// only what needs more than one field to see. Today that is nothing, and the
// function exists anyway: it is the hook a future cross-field rule goes into,
// and an empty one is cheaper to add now than a call site is to remember later.
func (c loaderConfig) validate() error {
	if c.concurrency < 1 {
		// Unreachable through WithConcurrency, which rejects it. Reachable if
		// a default is ever changed carelessly.
		return fmt.Errorf("loader: concurrency %d: must be at least 1: %w", c.concurrency, ErrInvalidRequest)
	}

	return nil
}

// WithCacheDir sets the directory holding cached archives and their derived
// Parquet files.
//
// The default is a "bmd" directory inside the operating system's own cache
// location — ~/Library/Caches on macOS, $XDG_CACHE_HOME or ~/.cache on Linux,
// %LocalAppData% on Windows. That is the right default because everything in
// it is re-downloadable by definition and none of it should ever be backed up.
//
// The directory is created on first write, not here and not by [NewLoader]: a
// loader that only ever gets cache hits, or whose requests all fail
// validation, leaves nothing behind.
//
// An empty dir is rejected rather than treated as "use the default". The two
// spellings would be indistinguishable at the call site, and a configuration
// file with a missing key would silently write to somewhere its author did not
// choose.
func WithCacheDir(dir string) Option {
	return func(c *loaderConfig) error {
		if dir == "" {
			return fmt.Errorf("loader: cache dir is empty "+
				"(omit WithCacheDir entirely for the default): %w", ErrInvalidRequest)
		}

		c.cacheDir = dir

		return nil
	}
}

// WithConcurrency sets how many chunks are fetched at once. The default is
// [defaultConcurrency].
//
// One number governs the whole loader, not one per call: [Loader.FetchAll]
// running twenty requests uses the same budget as a single [Loader.Fetch], so
// the setting means what it says regardless of how the work arrives.
//
// Turn it *down* for 1s data. Each worker holds one decoded archive, and a
// month of 1s candles is around 810 MB — see [defaultConcurrency].
func WithConcurrency(n int) Option {
	return func(c *loaderConfig) error {
		if n < 1 {
			return fmt.Errorf("loader: concurrency %d: must be at least 1: %w", n, ErrInvalidRequest)
		}

		c.concurrency = n

		return nil
	}
}

// WithHTTPClient supplies the [http.Client] used for every request.
//
// The default is a package-wide client built by [vision.NewHTTPClient], and it
// is worth knowing what you are replacing before you replace it. It clones
// http.DefaultTransport — keeping proxy support and HTTP/2 negotiation, which
// a hand-built &http.Transport{} silently drops — and raises
// MaxIdleConnsPerHost from its default of 2 to 64, because every request this
// library makes goes to one of three hosts. Passing http.DefaultClient here
// means two idle connections per host, and a worker pool that re-handshakes
// TCP and TLS for most of its downloads.
//
// It also deliberately sets no Client.Timeout. A timeout bounds the entire
// exchange including the body read, so any value large enough for a 93 MB
// archive on a slow link is far too large to catch a hung connection. Use the
// context instead: it is per call and the caller owns it.
func WithHTTPClient(client *http.Client) Option {
	return func(c *loaderConfig) error {
		if client == nil {
			return fmt.Errorf("loader: http client is nil "+
				"(omit WithHTTPClient entirely for the default): %w", ErrInvalidRequest)
		}

		c.client = client

		return nil
	}
}

// WithProgress registers a function called once for each chunk of work that
// finishes. See [Progress] for what it is told and what it is not.
//
// Calls are serialised: the loader holds a mutex across the call, so fn is
// never entered by two goroutines at once and does not need to be safe for
// concurrent use. That is a promise worth making explicitly, because the
// obvious implementation — call it from each worker — would quietly require
// every caller to write a mutex of their own, and most would not.
//
// The cost of that promise is that fn is on the critical path: a slow callback
// stalls the pool. Keep it to a counter, a progress bar or a channel send.
func WithProgress(fn func(Progress)) Option {
	return func(c *loaderConfig) error {
		if fn == nil {
			return fmt.Errorf("loader: progress function is nil: %w", ErrInvalidRequest)
		}

		c.progress = fn

		return nil
	}
}

// WithLogger sets the structured logger. The default discards everything.
//
// The loader logs at two levels and nothing else writes: Debug for each
// decision that is invisible from the outside — a chunk the bucket listing said
// was missing, an archive that 404'd anyway, a fallback down the ladder — and
// Warn for the pipeline pausing on a rate limit. Errors are returned rather
// than logged, since a library that logs an error and returns it has reported
// it twice.
func WithLogger(l *slog.Logger) Option {
	return func(c *loaderConfig) error {
		if l == nil {
			return fmt.Errorf("loader: logger is nil "+
				"(omit WithLogger entirely to discard log output): %w", ErrInvalidRequest)
		}

		c.logger = l

		return nil
	}
}

// The options below are unexported, and every one of them exists so that a test
// can point this package at an httptest.Server or freeze its clock.
//
// They are options rather than fields poked directly onto a Loader for one
// reason: a Loader is assembled in [NewLoader] from a completed configuration,
// so a field written afterwards would either be ignored or would have to be
// re-read on every call. Going through the same door as the public settings
// means the test exercises the real construction path.

// withOfflineHosts points all three transports at a port nothing listens on.
//
// It is for tests that build a Loader to inspect it or to exercise pure logic,
// and never fetch anything. Those need no server — but leaving the hosts unset
// means the real Binance endpoints, and CLAUDE.md's rule that no test may touch
// Binance is worth keeping true by construction rather than by everyone
// remembering not to add a Fetch. If one is ever added, it fails at once
// instead of reaching out.
func withOfflineHosts() Option {
	const unreachable = "http://127.0.0.1:1"

	return withTestHosts(unreachable, unreachable, unreachable)
}

// withTestHosts aims the three transports at test servers. An empty string
// leaves that host at its real default.
func withTestHosts(listing, download, api string) Option {
	return func(c *loaderConfig) error {
		c.listBaseURL, c.downloadBaseURL, c.apiBaseURL = listing, download, api

		return nil
	}
}

// withClock replaces the source of "now".
//
// It governs *calendar* decisions only — where a range ends, whether a candle
// has closed, which month a listing seeks to. Delays and pauses read the real
// clock and are tested inside a testing/synctest bubble instead; see the note
// on the loader's gate for why the two are not the same question.
func withClock(now func() time.Time) Option {
	return func(c *loaderConfig) error {
		c.now = now

		return nil
	}
}

// withPolicy replaces the retry policy, so that a test can keep production's
// four attempts without spending production's 3.5 seconds of backoff.
func withPolicy(p vision.Policy) Option {
	return func(c *loaderConfig) error {
		c.policy = p

		return nil
	}
}

// withPauseBounds replaces the clamp a Retry-After is squeezed into.
//
// The shipped floor is a second, so a test of the retry loop would otherwise
// sit out several of them per run. Production values are the constants in
// loader.go; this changes the bounds, never the rule that they are applied.
func withPauseBounds(minPause, maxPause time.Duration) Option {
	return func(c *loaderConfig) error {
		c.minPause, c.maxPause = minPause, maxPause

		return nil
	}
}

// withLimiter replaces the REST rate limiter. Tests that are not about pacing
// pass an enormous one, so that an assertion about pagination does not also
// depend on a token-bucket calculation.
func withLimiter(lim *rate.Limiter) Option {
	return func(c *loaderConfig) error {
		c.limiter = lim

		return nil
	}
}

// Source says where a chunk of candles came from.
//
// It mirrors the internal planner's own enumeration rather than exposing it.
// internal/plan cannot appear in this package's public API — consumers cannot
// import an internal package, so a public function returning plan.Kind would
// name a type they are unable to write down — and the indirection is cheap: one
// type and one switch, in exchange for the planner staying free to change.
type Source uint8

// The three places a candle can come from. Numbered from 1 so that the zero
// value is invalid rather than a plausible default, as with [Interval] and
// [Market].
const (
	SourceMonthlyArchive Source = iota + 1 // one ZIP covering a calendar month
	SourceDailyArchive                     // one ZIP covering a single day
	SourceRESTAPI                          // paginated calls to the REST API
)

// String implements [fmt.Stringer].
func (s Source) String() string {
	switch s {
	case SourceMonthlyArchive:
		return "monthly archive"
	case SourceDailyArchive:
		return "daily archive"
	case SourceRESTAPI:
		return "rest api"
	default:
		return fmt.Sprintf("Source(%d)", uint8(s))
	}
}

// Progress describes one finished chunk of work, and is what [WithProgress]
// receives.
//
// # What it does not say
//
// There is no "cache hit" field, and its absence is a design decision rather
// than an oversight. The cache's entire surface within this package is one
// method that takes an archive and returns its candles; which tier answered,
// whether anything was downloaded and whether the Parquet file had to be
// rebuilt are deliberately invisible above that line, and adding them to this
// struct would mean widening that surface for the sake of a progress bar. See
// docs/caching.md.
//
// What is here is the shape of the work — how many units, how far through, and
// how much each one yielded — which is what a progress display and a
// diagnostic log actually need.
type Progress struct {
	// Request is the request this work belongs to, in resolved form: the
	// symbol normalised and End filled in if it was left zero. In FetchAll
	// this is what identifies which of the requests a callback is about.
	Request Request

	// Source is where the candles came from.
	//
	// It names what the *plan* asked for. If the archive turned out to be
	// missing and the candles were recovered from further down the ladder,
	// this still says what was planned, and the substitution is reported to
	// the logger instead. One event per planned unit of work is what keeps
	// Done and Total meaningful.
	Source Source

	// Start and End are the half-open range the chunk covered — Start
	// included, End excluded. For an archive this is the archive's own
	// extent, which is routinely wider than the part of it the request wanted.
	//
	// Note the mismatch with the Request above, and that it is deliberate.
	// A [Request] is closed: the caller's End is included. A chunk is
	// half-open, because chunks are the pieces a range is cut into and
	// half-open pieces join without arithmetic — see [Request] on where each
	// convention lives. These fields describe the pieces, so they use the
	// pieces' convention, and a display that prints them alongside the
	// request's own range should say so rather than let a reader assume the
	// last candle of the chunk opened at End.
	Start, End time.Time

	// Klines is how many candles the chunk produced, before merging and
	// trimming. Zero is normal at the leading edge of a symbol's history and
	// for a range that is still forming; it is not normal in the middle of a
	// published month, and the loader turns that case into an error rather
	// than a quiet gap.
	Klines int

	// Done and Total count chunks, not bytes: Done is how many have finished
	// including this one, Total how many the plan holds. Total is fixed before
	// any work starts, so Done/Total is a genuine fraction rather than an
	// estimate that moves.
	Total int
	Done  int

	// Err is the error the chunk failed with, or nil.
	//
	// A failure is reported here *and* returned from the call, because the two
	// answer different questions: this one says which unit of work failed
	// while the run was still going, and the returned error is what the caller
	// acts on. A progress callback is not a substitute for checking the error.
	Err error
}
