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

// Option configures a [Loader]. See [NewLoader] and the With* functions below.
//
// An Option is something you receive from a With* function and hand to
// [NewLoader]. There is nothing else to know about it, and nothing outside this
// package can build one — which is the whole reason it is an interface.
//
// # Why an interface, when an option is plainly a function
//
// The obvious spelling is a named function type, and that is what this was
// until Stage 9:
//
//	type Option func(*loaderConfig) error
//
// It compiles to the same thing and callers write the same code. What it also
// does is publish a signature naming loaderConfig — a type no other package can
// see, let alone write down — so the generated documentation renders the
// declaration as an instruction the reader is unable to follow. The private
// configuration struct leaks out through the one place it was supposed to stay
// behind.
//
// An interface whose only method is unexported renders instead as a named type
// with its contents filtered out, which is honest rather than teasing. It also
// makes the type closed by construction: apply is unexported, so no other
// package can satisfy Option even by accident, and this package stays free to
// add a method to it later without breaking a single caller.
//
// This is the same reasoning that kept the domain types out of internal/ in
// Stage 2. An identifier the documentation names but the reader cannot reach is
// worse than one it never mentions.
type Option interface {
	// apply mutates the configuration, or rejects what it was given.
	//
	// The pointer is why an option can be a mutation rather than a
	// copy-and-return: every option in a call to NewLoader writes into the
	// same struct, in the order written.
	apply(*loaderConfig) error
}

// optionFunc adapts a plain function to [Option].
//
// This is the standard Go move for giving an interface a function
// implementation — http.HandlerFunc is the same trick, and in the standard
// library it is very nearly the only one — and it is why each With* function
// below is still a one-line closure. Attaching a method to a *named function
// type* is what satisfies the interface; the closure itself carries the
// argument that was captured.
//
// sort.IntSlice and sort.StringSlice are the idea one step removed: methods
// hung on a named non-struct type so that it satisfies sort.Interface. They are
// slice types rather than func types, so they are a relative of this pattern
// rather than an instance of it.
//
// It is unexported on purpose. Exporting it would hand back the ability to
// write an arbitrary Option, and with it a pointer to loaderConfig, undoing
// everything the interface was for.
type optionFunc func(*loaderConfig) error

// apply implements [Option] by calling the function itself.
func (f optionFunc) apply(c *loaderConfig) error { return f(c) }

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
//
// No environment variable is consulted here, and that is deliberate rather than
// an omission. This package is imported by other programs, and one that read the
// environment on its own would let a variable exported for some unrelated reason
// redirect where its caller writes files. The bmd command does read one —
// BMD_CACHE_DIR, which its -cache-dir flag overrides — because a tool a person
// runs is the layer where that is the expected behaviour rather than a surprise.
func WithCacheDir(dir string) Option {
	return optionFunc(func(c *loaderConfig) error {
		if dir == "" {
			return fmt.Errorf("loader: cache dir is empty "+
				"(omit WithCacheDir entirely for the default): %w", ErrInvalidRequest)
		}

		c.cacheDir = dir

		return nil
	})
}

// WithConcurrency sets how many chunks are fetched at once. The default is 8.
//
// One number governs the whole loader, not one per call: [Loader.FetchAll]
// running twenty requests uses the same budget as a single [Loader.Fetch], so
// the setting means what it says regardless of how the work arrives.
//
// Turn it *down* for 1s data. Each worker holds one decoded archive, and a
// month of 1s candles is around 810 MB, so the default of 8 is several
// gigabytes at that interval. The default is chosen for 1m and coarser, where
// it costs at most about 110 MB.
func WithConcurrency(n int) Option {
	return optionFunc(func(c *loaderConfig) error {
		if n < 1 {
			return fmt.Errorf("loader: concurrency %d: must be at least 1: %w", n, ErrInvalidRequest)
		}

		c.concurrency = n

		return nil
	})
}

// WithRateLimit lowers the sustained rate this loader is allowed to spend
// against the REST endpoint's quota, in weight units per second. The default is
// [vision.DefaultWeightPerSecond].
//
// # What the unit is
//
// Binance meters the REST mirror as REQUEST_WEIGHT, not as requests: 6000 per
// minute per IP address, measured on 2026-08-20, and one klines call costs 2 of
// them. So the quota is 100 weight per second, the default takes 40, and the
// argument here is in the same unit Binance publishes rather than a translation
// of it that would need re-deriving every time the cost of a call changes.
//
// Only the REST tail is paced. data.binance.vision is a static file server with
// no quota, so listing and archive downloads are unaffected — [WithConcurrency]
// is the knob for those.
//
// # Why you would turn it down
//
// The quota is per IP, not per API key or per process, so everything on the
// machine draws from the same 6000: a live trading bot, a second backtest,
// another copy of this library. The default already leaves most of the budget
// unspent for exactly that reason, and it cannot know how much company it has.
// If a history download shares an address with something latency-sensitive,
// spending less than 40 is the way to say so.
//
// Exceeding the quota is worse than being slow. Binance escalates a 429 a
// client keeps ignoring into an HTTP 418, an IP ban running from two minutes to
// three days and lengthening with repeat offences — which punishes the address
// rather than the process, so the ban shows up in the trading bot's logs.
//
// # Why it will not let you go up
//
// Values above the quota's own 100 per second are rejected rather than clamped.
// There is no rate above it that Binance permits, so accepting one would be
// accepting a setting that cannot be honoured, and clamping silently would have
// this function report success for a policy it did not apply. The ceiling is
// the quota itself rather than the default, so raising the rate towards it
// stays possible for a caller who knows they have the address to themselves.
//
// # The one thing to know before using it
//
// A loader built without this option shares one process-wide limiter with every
// other such loader, because two buckets each honouring the documented rate
// permit twice it — correct alone and wrong in aggregate. This option opts out
// of that sharing: the loader gets a bucket of its own. With a single loader in
// the process, which is the normal case, that is exactly what it looks like.
// With several, set it on all of them, or the ones left on the default are
// spending from a second bucket that knows nothing about this one.
func WithRateLimit(weightPerSecond float64) Option {
	return optionFunc(func(c *loaderConfig) error {
		if weightPerSecond <= 0 {
			return fmt.Errorf("loader: rate limit %g: must be greater than zero: %w",
				weightPerSecond, ErrInvalidRequest)
		}

		// The quota expressed per second. Written as the division rather than
		// as 100 so that the published number stays the only place the figure
		// lives, and a correction to it reaches this check for free.
		const quotaPerSecond = float64(vision.WeightLimitPerMinute) / 60

		if weightPerSecond > quotaPerSecond {
			return fmt.Errorf("loader: rate limit %g: exceeds the published quota of %g weight per second: %w",
				weightPerSecond, quotaPerSecond, ErrInvalidRequest)
		}

		c.limiter = vision.NewLimiter(weightPerSecond, vision.BurstFor(weightPerSecond))

		return nil
	})
}

// WithHTTPClient supplies the [http.Client] used for every request.
//
// The default is a package-wide client built in internal/vision, and it is
// worth knowing what you are replacing before you replace it. It clones
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
	return optionFunc(func(c *loaderConfig) error {
		if client == nil {
			return fmt.Errorf("loader: http client is nil "+
				"(omit WithHTTPClient entirely for the default): %w", ErrInvalidRequest)
		}

		c.client = client

		return nil
	})
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
	return optionFunc(func(c *loaderConfig) error {
		if fn == nil {
			return fmt.Errorf("loader: progress function is nil: %w", ErrInvalidRequest)
		}

		c.progress = fn

		return nil
	})
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
	return optionFunc(func(c *loaderConfig) error {
		if l == nil {
			return fmt.Errorf("loader: logger is nil "+
				"(omit WithLogger entirely to discard log output): %w", ErrInvalidRequest)
		}

		c.logger = l

		return nil
	})
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
	return optionFunc(func(c *loaderConfig) error {
		c.listBaseURL, c.downloadBaseURL, c.apiBaseURL = listing, download, api

		return nil
	})
}

// withClock replaces the source of "now".
//
// It governs *calendar* decisions only — where a range ends, whether a candle
// has closed, which month a listing seeks to. Delays and pauses read the real
// clock and are tested inside a testing/synctest bubble instead; see the note
// on the loader's gate for why the two are not the same question.
func withClock(now func() time.Time) Option {
	return optionFunc(func(c *loaderConfig) error {
		c.now = now

		return nil
	})
}

// withPolicy replaces the retry policy, so that a test can keep production's
// four attempts without spending production's 3.5 seconds of backoff.
func withPolicy(p vision.Policy) Option {
	return optionFunc(func(c *loaderConfig) error {
		c.policy = p

		return nil
	})
}

// withPauseBounds replaces the clamp a Retry-After is squeezed into.
//
// The shipped floor is a second, so a test of the retry loop would otherwise
// sit out several of them per run. Production values are the constants in
// loader.go; this changes the bounds, never the rule that they are applied.
func withPauseBounds(minPause, maxPause time.Duration) Option {
	return optionFunc(func(c *loaderConfig) error {
		c.minPause, c.maxPause = minPause, maxPause

		return nil
	})
}

// withLimiter replaces the REST rate limiter. Tests that are not about pacing
// pass an enormous one, so that an assertion about pagination does not also
// depend on a token-bucket calculation.
func withLimiter(lim *rate.Limiter) Option {
	return optionFunc(func(c *loaderConfig) error {
		c.limiter = lim

		return nil
	})
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
