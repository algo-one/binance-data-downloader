package binancedata

// Loader is where the three phases of this library meet:
//
//	Request ──► PLAN ──────► []Chunk ──► EXECUTE ──────► []Kline ──► REDUCE ──► []Kline
//	            (pure)                   (bounded pool)              (merge,
//	            no I/O                   + singleflight               dedup, trim)
//
// Everything below this file already exists and is tested on its own: the
// planner turns a range into chunks without touching the network, the cache
// turns one archive into candles, the REST fetcher does the same for the tail,
// and internal/vision does every HTTP call. What is left — and what this file
// is — is the arrangement: probing what Binance actually published, deciding
// which chunk comes from where, running them without swamping anything, and
// joining the results back into one contiguous range.
//
// # Why the pool is flat
//
// One queue of uniform work units, one limit. The implementation this library
// replaces nested two semaphores — a month held a permit while merely *waiting*
// on the days it had been split into — which is a task occupying a slot while
// doing nothing, and deadlocks or starves as soon as the outer units outnumber
// the limit. Nothing here acquires a permit while holding one: a chunk that
// turns out to be missing is expanded and fetched inside the permit it already
// has, sequentially. That is slower in the rare case and incapable of the
// failure in every case.
//
// # Why substitution is rare enough for that to be fine
//
// The bucket listing is consulted before any chunk is fetched, so a month
// Binance never published is known to be missing *before* a request is spent
// discovering it — see [Loader.resolve]. What is left for the runtime ladder is
// an archive that vanished between the listing and the download, which is not a
// thing that happens.

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/algo-one/binance-data-downloader/internal/plan"
	"github.com/algo-one/binance-data-downloader/internal/vision"
)

const (
	// maxPipelinePauses is how many times one chunk may pause the whole
	// pipeline for a rate limit before its error is reported instead.
	//
	// The retry policy inside internal/vision has already made four attempts
	// with backoff by the time anything reaches here, so a 429 that survives
	// to this layer is not a hiccup — it says the pipeline as a whole is going
	// too fast. Pausing everybody is the right response to that, and doing it
	// without bound is a hang: a server that answers 429 forever would be
	// waited on forever, and the caller would have no error to look at.
	maxPipelinePauses = 2

	// minPipelinePause and maxPipelinePause bound what a Retry-After header
	// can talk this package into.
	//
	// The floor exists because the header is reported verbatim and can be
	// zero — Binance sending none, or an HTTP-date that a clock two seconds
	// fast reads as already elapsed. A zero pause is no pause, which would
	// have every worker re-fire immediately at a server that just said it was
	// overloaded. The ceiling exists because the value is somebody else's
	// number: a misconfigured proxy answering "retry after 24 hours" must not
	// be able to hang a backtest until tomorrow.
	minPipelinePause = 1 * time.Second
	maxPipelinePause = 60 * time.Second
)

// Loader fetches candles, caching everything it downloads.
//
// A Loader is safe for concurrent use and is meant to be long-lived: one per
// process, built once and shared. That is not merely convenient — the
// concurrency limit, the connection pool and the REST rate limiter are all
// per-Loader, and two Loaders each pacing themselves correctly still exceed
// Binance's per-IP quota together.
//
// The zero value is not usable; build one with [NewLoader].
type Loader struct {
	// cache is tier 1 + tier 2, and the only thing here that touches the disk.
	// Its singleflight is what stops two workers fetching the same archive.
	cache *cache

	// lister asks the S3 bucket what exists; rest fetches the tail that no
	// archive covers yet.
	lister *vision.Lister
	rest   restFetcher

	// sem is the concurrency limit, shared by every call on this Loader so
	// that FetchAll over twenty requests uses the same budget as one Fetch.
	//
	// A semaphore rather than errgroup.SetLimit, for one reason: SetLimit
	// bounds a single group, and the whole point is that the bound spans
	// calls. See [Loader.stream] for the ordering property that replaces the
	// one SetLimit would have given.
	sem semaphore

	// gate pauses every worker at once when Binance says to slow down.
	gate gate

	// minPause and maxPause bound what a server's Retry-After can talk this
	// pipeline into waiting. See [minPipelinePause].
	minPause, maxPause time.Duration

	now      func() time.Time
	logger   *slog.Logger
	progress func(Progress)

	// progressMu serialises calls into progress, so a caller's callback need
	// not be safe for concurrent use. See [WithProgress].
	progressMu sync.Mutex
}

// NewLoader builds a Loader from the options given. See [Option] and the With*
// functions for what can be configured; the defaults are usable for everything
// except 1s data, where [WithConcurrency] wants turning down.
//
// It performs no I/O and creates no directories. A constructor that reached for
// the network would make every program that builds a Loader at startup fail to
// start when Binance is down, and one that created the cache directory would
// leave a tree behind for a program that then failed validation and fetched
// nothing.
//
// It does return an error, and that is the project's rule rather than this
// function's preference: validation that lives anywhere else is validation a
// caller can forget to run.
func NewLoader(opts ...Option) (*Loader, error) {
	cfg := defaultLoaderConfig()

	for i, opt := range opts {
		if opt == nil {
			// A nil Option is almost always a conditional that returned
			// nothing on one branch. Saying so beats a nil dereference.
			return nil, fmt.Errorf("loader: option %d is nil: %w", i, ErrInvalidRequest)
		}

		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// Three transports over two hosts plus the S3 endpoint, all sharing one
	// http.Client — which is the point of correctness requirement 8. A
	// Transport pools per host, so one client keeps three separate pools
	// rather than mixing them, and a nil client here means the process-wide
	// default rather than http.DefaultClient. See internal/vision/client.go.
	dl := vision.NewDownloader(cfg.downloadBaseURL, cfg.client, cfg.policy)

	c, err := newCache(cfg.cacheDir, dl)
	if err != nil {
		return nil, err
	}

	return &Loader{
		cache:  c,
		lister: vision.NewLister(cfg.listBaseURL, cfg.client, cfg.policy),
		// includePartial is left at its zero value: the candle currently
		// forming is dropped. There is no option for it, and no configuration
		// field either — one that nothing can set is an accepted-and-ignored
		// setting by another name. restapi.go keeps the field because
		// near-live use is a legitimate thing to want; the day it is wanted, a
		// public option and a line here are the whole change.
		rest:     restFetcher{api: vision.NewAPI(cfg.apiBaseURL, cfg.client, cfg.policy, cfg.limiter)},
		sem:      newSemaphore(cfg.concurrency),
		minPause: cfg.minPause,
		maxPause: cfg.maxPause,
		now:      cfg.now,
		logger:   cfg.logger,
		progress: cfg.progress,
	}, nil
}

// Fetch returns every candle in the requested range, in ascending order of open
// time, with no duplicates.
//
// The range is closed — both Start and End are included, so a candle is
// returned when Start <= OpenTime <= End. A full year of 2024 is Start
// 2024-01-01 and End 2024-12-31T23:59:59.999999999Z; see [Request] for why the
// nines are there and what writing 2025-01-01 instead would get you. A zero End
// means "now, as of this call".
//
// # What an error means
//
// Nothing is returned alongside one. If any part of the range has no data in
// any of the three sources, the whole call fails with an error wrapping
// [ErrNotAvailable] naming the empty span, rather than returning a shorter
// range than was asked for. Silently returning less than requested is the
// failure this library is built to avoid: a backtest cannot tell the difference
// between "the market was quiet" and "two months are missing".
//
// The one span exempt from that is the one that has not happened yet. A request
// ending now normally ends part-way through a candle that has not closed, and
// an unclosed candle is deliberately not returned — see [restFetcher] — so a
// tail with nothing settled in it is expected rather than missing.
//
// # How far that guarantee reaches
//
// To the chunk, which is the granularity Binance publishes at. A month, day or
// REST range that produced nothing is an error; a chunk that produced *some* of
// what its span could hold is not examined further.
//
// That line is deliberate rather than convenient. Archives are legitimately
// partial — SHIBUSDT's 2021-05 archive holds 22 rows for a 31-day month because
// the pair was listed on the 10th — so a rule that demanded a full chunk would
// reject real data, which is the same reason codec.go checks that every candle
// is inside its period but never that the period is full.
//
// In practice the case that matters is caught anyway, because an absent period
// is an absent *archive*. Asking for BTCUSDT from 2015 makes every month before
// 2017-08 a chunk of its own with nothing in it, and that is an error. What is
// not caught is a pair that began trading part-way through a month whose archive
// does exist: the range then starts at the first real candle rather than at
// Start, and no error is returned.
//
// # What an error leaves behind
//
// Possibly a warmer cache. A failing chunk cancels its siblings, but a download
// the cache has already started is not stopped: it finishes and populates the
// cache for the next run, which is the right trade for a directory that
// outlives the process. So this call can return an error while bytes are still
// being written under the cache directory, and a program that deletes that
// directory immediately after a failure is racing work it cannot see.
// Retrying, which is the ordinary response, is the case it is optimised for.
//
// # Memory
//
// The whole range is held at once. A [Kline] is 312 bytes, so five years of 1m
// candles is roughly 820 MB. Use [Loader.Stream] to consume a large range
// without materialising it.
func (l *Loader) Fetch(ctx context.Context, req Request) ([]Kline, error) {
	// A capacity hint, not a promise. Ranges are routinely shorter than their
	// span — a pair listed mid-range, a market that halted — so this is the
	// same estimate the decoder uses for the same reason, capped well below
	// what a long range would need. Without it, appending five years of 1m
	// candles regrows and copies the slice about twenty times.
	out := make([]Kline, 0, l.sizeHint(req))

	for k, err := range l.Stream(ctx, req) {
		if err != nil {
			return nil, err
		}

		out = append(out, k)
	}

	return out, nil
}

// FetchAll runs several requests together and returns each one's candles.
//
// The map is keyed by the request *as given*, not as resolved, so a caller
// looks up exactly the value they passed in. That works because [Request] is
// comparable and because its Start and End are required to be UTC — see the
// note on that type for why time.Time is otherwise a trap as a map key.
//
// # One budget, not one per request
//
// Every request shares the Loader's concurrency limit, so twenty requests move
// as fast as the limit allows rather than twenty times faster than it. They
// also share the cache's deduplication: two overlapping ranges for one symbol
// download the months they have in common exactly once, which is what makes
// this a single call rather than the register-then-start API the Python
// implementation needed.
//
// # The first error stops everything
//
// A failure cancels the remaining requests and FetchAll returns a nil map with
// that error. Returning a half-filled map alongside an error would make every
// caller check two things, and the useful ones would check one.
func (l *Loader) FetchAll(ctx context.Context, reqs []Request) (map[Request][]Kline, error) {
	out := make(map[Request][]Kline, len(reqs))

	if len(reqs) == 0 {
		return out, nil
	}

	var mu sync.Mutex

	// No SetLimit on this group. The limit that matters is the Loader's
	// semaphore, which every request takes a permit from for its plan phase
	// and again for each chunk; limiting here as well would be the nested-limit
	// arrangement this package exists without. What this costs is one goroutine
	// per request, most of them blocked on that semaphore, at a few kilobytes
	// of stack each.
	//
	// Two requests for the same symbol and interval still list the bucket
	// twice: the index is built per call and not memoised on the Loader. That
	// is deliberate for now — an index is a snapshot of two different kinds of
	// fact, and a memoised "this month does not exist yet" is how a process
	// decides at 00:05 that today has no data and believes it until restarted.
	// Sharing it through singleflight would also inherit the foreign-
	// cancellation edge cache.klines has to retry around.
	g, gctx := errgroup.WithContext(ctx)

	for _, req := range reqs {
		g.Go(func() error {
			klines, err := l.Fetch(gctx, req)
			if err != nil {
				// The request is named because the error from below has no
				// idea which of twenty it belongs to.
				return fmt.Errorf("%s %s: %w", req.Symbol, req.Interval, err)
			}

			mu.Lock()
			defer mu.Unlock()

			out[req] = klines

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}

// Stream yields the requested candles one at a time, in ascending order of open
// time, without holding the whole range in memory.
//
// Ranging over it is the intended use, and `break` is honoured — the pipeline
// behind it is cancelled and every worker stops:
//
//	for k, err := range loader.Stream(ctx, req) {
//	    if err != nil {
//	        return err
//	    }
//	    ...
//	}
//
// # What it costs and what it saves
//
// Chunks are fetched concurrently and yielded in order, and a worker holds its
// permit until its candles have been consumed. So the memory in flight is
// bounded by the concurrency limit rather than by the length of the range: five
// years of 1m candles streams in about 110 MB instead of 820 MB.
//
// The floor is one chunk, and a chunk is a whole archive. A month of 1s candles
// decodes to roughly 810 MB whatever this function does, because the cache's
// unit is a file. Streaming bounds how many of those exist at once; it does not
// make one of them smaller.
//
// # Errors
//
// An error is yielded once, with the zero [Kline], and the iteration then ends.
// Unlike [Loader.Fetch] it may arrive *after* some candles have already been
// yielded — a stream cannot un-yield what the caller has already seen — so a
// consumer that needs all-or-nothing should use Fetch.
func (l *Loader) Stream(ctx context.Context, req Request) iter.Seq2[Kline, error] {
	return func(yield func(Kline, error) bool) {
		// One clock reading for the whole call. Every calendar decision below
		// — where the range ends, which candle is still forming, whether an
		// empty span should have had data in it — is made against this one
		// instant, so they cannot disagree with each other.
		now := l.now().UTC()

		resolved, chunks, err := l.resolveUnderLimit(ctx, req, now)
		if err != nil {
			yield(Kline{}, err)
			return
		}

		l.stream(ctx, resolved, chunks, now, yield)
	}
}

// resolveUnderLimit is [Loader.resolve] holding a pool permit.
//
// The plan phase does I/O — the bucket listing, two concurrent requests for
// most intervals — and it runs before any chunk is fetched, so without this it
// sits entirely outside the concurrency limit. FetchAll over N requests would
// open 2N simultaneous listings whatever WithConcurrency said, and it would do
// it at the one moment of the call when every request is at the same stage.
// That is the shape that earns a 429, and then the HTTP 418 that [Loader.pause]
// is deliberately written not to wait out.
//
// The permit is released before the chunks are fetched, so nothing here holds
// one while waiting for another — the property the flat pool exists to keep.
// One permit covers up to two listings rather than one, which is close enough:
// the bound that matters is that there is one.
func (l *Loader) resolveUnderLimit(ctx context.Context, req Request, now time.Time) (Request, []plan.Chunk, error) {
	if err := l.sem.acquire(ctx); err != nil {
		return Request{}, nil, err
	}
	defer l.sem.release()

	return l.resolve(ctx, req, now)
}

// resolve is the plan phase: validate the request, ask the bucket what exists,
// expand the range into chunks, and route each chunk to a source that is known
// to have it.
//
// It is the only phase that needs the network before any candle is fetched, and
// it is one or two requests: the S3 listing, seeking straight to the range with
// a marker so the cost is set by the range asked for rather than by how long
// the symbol has traded.
func (l *Loader) resolve(ctx context.Context, req Request, now time.Time) (Request, []plan.Chunk, error) {
	resolved, err := req.resolve(now)
	if err != nil {
		return Request{}, nil, err
	}

	// The listing is seeked from the 1st of the month the range starts in,
	// which is at or before every chunk [plan.Expand] can emit: monthly chunks
	// begin on the 1st, daily ones at a midnight inside the range. That is
	// what makes the index authoritative for every chunk below — a marker any
	// later would leave has() answering "not listed" for periods it was simply
	// never asked about, which is the failed-lookup-read-as-absent conflation
	// this package is arranged to prevent.
	since := monthStart(resolved.Start)

	index, err := fetchArchiveIndex(ctx, l.lister, resolved.Market, resolved.Symbol, resolved.Interval, since)
	if err != nil {
		return Request{}, nil, err
	}

	// endExclusive, not End: everything from here down is half-open. This is
	// the boundary between the two conventions, and it is the whole of it —
	// see [Request.endExclusive].
	chunks, err := plan.Expand(plan.Spec{
		Start:           resolved.Start,
		End:             resolved.endExclusive(),
		ArchivesThrough: index.through,
		HasDaily:        resolved.Interval.HasDailyArchives(),
		HasMonthly:      resolved.Interval.HasMonthlyArchives(),
	})
	if err != nil {
		return Request{}, nil, err
	}

	chunks = l.route(ctx, chunks, index, resolved)

	l.logger.DebugContext(ctx, "plan resolved",
		"symbol", resolved.Symbol, "interval", resolved.Interval.String(),
		"start", resolved.Start, "end", resolved.End,
		"archivesThrough", index.through, "chunks", len(chunks))

	return resolved, chunks, nil
}

// route settles every chunk against what the bucket listing actually holds.
//
// This is where [archiveIndex.has] earns its place, and it does two jobs that
// [plan.Expand] cannot, because Expand has no network to ask with.
//
// **Downgrade.** An archive the listing does not have is sent down the ladder
// before a request is spent discovering it. The real case is BTCUSDT's 1mo
// archive for March 2024, which does not exist while February and April both
// do: without this, that month costs a 404, a fan-out into thirty-one daily
// chunks that do not exist either, and sixty-two more 404s, before landing on
// the REST range it could have started at.
//
// **Upgrade.** A run of daily chunks is replaced by the month covering it when
// that month exists and enough of it is wanted — the threshold [plan.Chunk]
// documents. This runs *first*, and it has to: consolidating onto a month that
// turns out to be absent is strictly worse than never consolidating, because
// the downgrade then fans the whole month out rather than the days that were
// asked for. Doing both here, in that order, is what makes the trade-off sound;
// deciding it in the planner meant deciding it against Spec.ArchivesThrough,
// which answers a different question. See [plan.Consolidate].
//
// The recursion in [Loader.routeOne] is bounded by the ladder itself — monthly
// to daily to REST, and [plan.Substitute] refuses to go below REST — so it is
// at most two levels deep however carelessly it is called.
func (l *Loader) route(ctx context.Context, chunks []plan.Chunk, index archiveIndex, req Request) []plan.Chunk {
	// Upgrade before downgrading, so that nothing is consolidated onto a month
	// the next step would immediately have to take apart again.
	chunks = plan.Consolidate(chunks, func(t time.Time) bool { return index.has(aggMonthly, t) })

	out := make([]plan.Chunk, 0, len(chunks))

	for _, c := range chunks {
		out = l.routeOne(ctx, out, c, index, req)
	}

	return coalesceREST(out)
}

func (l *Loader) routeOne(
	ctx context.Context, dst []plan.Chunk, c plan.Chunk, index archiveIndex, req Request,
) []plan.Chunk {
	agg, isArchive := aggregationFor(c.Kind)
	if !isArchive || index.has(agg, c.Start) {
		return append(dst, c)
	}

	subs, err := plan.Substitute(c, req.Interval.HasDailyArchives())
	if err != nil {
		// Nothing left to try. Keep the chunk as planned rather than dropping
		// it: the execute phase reports an empty span as an error, and a chunk
		// silently removed here would become a range that came back short with
		// nothing to explain it.
		return append(dst, c)
	}

	l.logger.DebugContext(ctx, "archive not published, substituting",
		"symbol", req.Symbol, "interval", req.Interval.String(),
		"chunk", c.String(), "substitutes", len(subs))

	for _, s := range subs {
		dst = l.routeOne(ctx, dst, s, index, req)
	}

	return dst
}

// coalesceREST joins adjacent REST chunks into one.
//
// Substitution produces runs of them: a month that was never published expands
// into thirty-one days that were never published either, each of which falls
// through to its own REST range. Fetching those separately is thirty-one
// paginated calls against the one endpoint in this library with a quota, to
// discover thirty-one times over that Binance has nothing there. Joined, it is
// one call.
//
// Adjacent means End equals Start exactly, which is a property half-open ranges
// give for free — see [Request] on why the ranges are half-open in the first
// place. Nothing else about a REST chunk constrains its bounds, so joining two
// is exact rather than approximate.
func coalesceREST(chunks []plan.Chunk) []plan.Chunk {
	out := make([]plan.Chunk, 0, len(chunks))

	for _, c := range chunks {
		last := len(out) - 1

		if last >= 0 &&
			c.Kind == plan.KindRESTRange &&
			out[last].Kind == plan.KindRESTRange &&
			out[last].End.Equal(c.Start) {
			out[last].End = c.End

			continue
		}

		out = append(out, c)
	}

	return out
}

// aggregationFor maps a planner chunk kind onto the bucket granularity that
// serves it, and reports false for a kind that no archive covers.
func aggregationFor(k plan.Kind) (aggregation, bool) {
	switch k {
	case plan.KindMonthlyArchive:
		return aggMonthly, true
	case plan.KindDailyArchive:
		return aggDaily, true
	case plan.KindRESTRange:
		return 0, false
	default:
		return 0, false
	}
}

// sourceFor maps a planner chunk kind onto the public [Source] reported through
// [Progress]. The zero Source is returned for an unset kind, which no plan
// contains.
func sourceFor(k plan.Kind) Source {
	switch k {
	case plan.KindMonthlyArchive:
		return SourceMonthlyArchive
	case plan.KindDailyArchive:
		return SourceDailyArchive
	case plan.KindRESTRange:
		return SourceRESTAPI
	default:
		return 0
	}
}

// stream is the execute and reduce phases.
//
// # The shape, and the deadlock it avoids
//
// Each chunk gets an unbuffered channel of its own, and the consumer reads them
// in order. A worker therefore blocks in its send until its candles have been
// taken, holding its permit for that whole time — which is the backpressure
// that bounds memory, and is deliberate rather than accidental.
//
// That arrangement deadlocks under one condition: if a later chunk could take
// the last permit while an earlier one is still waiting for one, the consumer
// would wait forever for a chunk that can never start. Permits are therefore
// acquired *in chunk order*, by the producer goroutine below, before each
// worker is launched. Chunk 0's permit is taken before chunk 1's, so the chunk
// the consumer is waiting for is always already running.
//
// # Reduce
//
// Merging is not a separate pass. Chunks are already sorted and arrive in
// order, so trimming to the requested range and dropping anything that does not
// strictly follow what was yielded last is enough — and doing it here means a
// duplicate is dropped before it is ever yielded rather than after the whole
// range has been assembled. Duplicates are real: substitution can overlap a
// chunk boundary, and archives overshoot the range they were fetched for.
func (l *Loader) stream(
	ctx context.Context, req Request, chunks []plan.Chunk, now time.Time, yield func(Kline, error) bool,
) {
	// A cancel of our own, so that a consumer breaking out of the loop stops
	// the workers rather than leaving them to finish into a channel nobody
	// will read.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	out := make([]chan []Kline, len(chunks))
	for i := range out {
		out[i] = make(chan []Kline)
	}

	var done atomic.Int64

	producerDone := make(chan struct{})

	go func() {
		defer close(producerDone)

		for i, c := range chunks {
			// In order, and before the goroutine starts. See the note above.
			if err := l.sem.acquire(gctx); err != nil {
				return
			}

			g.Go(func() error {
				defer l.sem.release()

				klines, err := l.work(gctx, req, c, now)

				l.report(Progress{
					Request: req, Source: sourceFor(c.Kind),
					Start: c.Start, End: c.End,
					Klines: len(klines),
					Total:  len(chunks), Done: int(done.Add(1)),
					Err: err,
				})

				if err != nil {
					return err
				}

				select {
				case out[i] <- klines:
					return nil
				case <-gctx.Done():
					return gctx.Err()
				}
			})
		}
	}()

	// Everything started above is wound down here, on every path out of this
	// function including a consumer that stopped early. Wait is safe to call
	// after the deferred cancel has already ended the group.
	defer func() {
		cancel()
		<-producerDone
		_ = g.Wait()
	}()

	// last is the open time most recently yielded, and the whole of the merge
	// step. haveLast rather than last.IsZero(): the zero time is a legal
	// instant, and a rule that treats it as "nothing yet" is a rule with a
	// value it silently mishandles.
	var (
		last     time.Time
		haveLast bool
	)

consume:
	for i := range chunks {
		var klines []Kline

		select {
		case klines = <-out[i]:
		case <-gctx.Done():
			// A worker failed, or the caller's context ended. Either way the
			// error is on the group, and the wait below is where it is read —
			// so this stops consuming rather than reporting anything itself.
			break consume
		}

		for _, k := range klines {
			// The closed range spelt out directly: before Start or after End is
			// outside it. Written this way rather than against endExclusive
			// because this is the one place in the pipeline whose subject is the
			// caller's own range rather than a chunk of it, so the caller's own
			// convention is the one that belongs here.
			if k.OpenTime.Before(req.Start) || k.OpenTime.After(req.End) {
				continue // an archive overshooting the range it was fetched for
			}

			if haveLast && !k.OpenTime.After(last) {
				continue // an overlap between two chunks
			}

			last, haveLast = k.OpenTime, true

			if !yield(k, nil) {
				return
			}
		}
	}

	<-producerDone

	// g.Wait alone is not enough to decide whether this run succeeded.
	//
	// errgroup only ever records an error a function passed to g.Go returned,
	// and a cancellation of the *caller's* context sets none by itself. The
	// window where that matters is narrow and real: the producer blocked in
	// sem.acquire holding no workers, every worker launched so far already
	// finished and returned nil. Cancel then, and Wait reports nil while the
	// consumer has stopped part-way through the range — a short result with
	// nothing to explain it, which is the one failure this package's whole
	// error contract exists to prevent.
	//
	// The semaphore is shared across the Loader, so that window is not
	// theoretical: during a FetchAll one request's producer can sit in acquire
	// for as long as the other requests hold every permit.
	//
	// ctx here is this function's own derived context; the deferred cancel
	// above has not run yet, so a non-nil error on it means the caller went
	// away rather than that we are shutting down.
	err := g.Wait()
	if err == nil {
		err = ctx.Err()
	}

	if err != nil {
		yield(Kline{}, err)
	}
}

// work fetches one planned chunk, pausing the whole pipeline if Binance says
// everyone is going too fast.
//
// The pause is this layer's business and nowhere else's. internal/vision has
// already retried the request four times with backoff by the time an error
// reaches here, and a 429 that outlived that is not a statement about one
// request — it says the pool is too wide or the range too large, and only the
// layer holding the pool can act on it. A ban is the exception: HTTP 418 means
// the address is barred for anything from two minutes to three days, so waiting
// is not a strategy and retrying earns the next, longer one.
func (l *Loader) work(ctx context.Context, req Request, c plan.Chunk, now time.Time) ([]Kline, error) {
	for pauses := 0; ; pauses++ {
		if err := l.gate.wait(ctx); err != nil {
			return nil, err
		}

		klines, err := l.ladder(ctx, req, c, now)
		if err == nil {
			if err := checkGap(klines, req, c, now); err != nil {
				return nil, err
			}

			return klines, nil
		}

		if pauses >= maxPipelinePauses || !l.pause(ctx, err) {
			return nil, err
		}
	}
}

// ladder fetches a chunk, falling back down monthly → daily → REST if the
// source it names turns out not to exist.
//
// In practice this never runs past its first line. [Loader.route] has already
// asked the bucket listing what exists and rerouted anything it does not have,
// so reaching here with an [ErrNotAvailable] means an archive that was listed
// and then 404'd — a race with Binance publishing, or a mirror out of step.
// It exists because "never" is a claim about somebody else's server.
//
// The fallbacks run sequentially, inside the permit this chunk already holds.
// Acquiring more permits here is what the flat pool exists not to do: a worker
// waiting on workers is the nested-semaphore deadlock in a new costume.
func (l *Loader) ladder(ctx context.Context, req Request, c plan.Chunk, now time.Time) ([]Kline, error) {
	klines, err := l.source(ctx, req, c, now)
	if err == nil || !errors.Is(err, ErrNotAvailable) {
		return klines, err
	}

	subs, subErr := plan.Substitute(c, req.Interval.HasDailyArchives())
	if subErr != nil {
		return nil, err // bottom of the ladder: report what actually failed
	}

	l.logger.DebugContext(ctx, "listed archive was missing when fetched, falling back",
		"symbol", req.Symbol, "interval", req.Interval.String(),
		"chunk", c.String(), "substitutes", len(subs), "err", err)

	var out []Kline

	for _, s := range subs {
		part, err := l.ladder(ctx, req, s, now)
		if err != nil {
			return nil, err
		}

		out = append(out, part...)
	}

	return out, nil
}

// source fetches exactly what the chunk names, with no fallback.
func (l *Loader) source(ctx context.Context, req Request, c plan.Chunk, now time.Time) ([]Kline, error) {
	if agg, isArchive := aggregationFor(c.Kind); isArchive {
		return l.cache.klines(ctx,
			archiveRef{
				Market:   req.Market,
				Symbol:   req.Symbol,
				Interval: req.Interval,
				Agg:      agg,
				Period:   c.Start,
			},
			decodeSpec{Interval: req.Interval, Start: c.Start, End: c.End})
	}

	if c.Kind == plan.KindRESTRange {
		return l.rest.klines(ctx,
			restRef{
				Market:   req.Market,
				Symbol:   req.Symbol,
				Interval: req.Interval,
				Start:    c.Start,
				End:      c.End,
			}, now)
	}

	return nil, fmt.Errorf("chunk %s: unknown kind: %w", c, ErrInvalidRequest)
}

// pause decides whether err is worth slowing the whole pipeline for, and does
// it. It reports whether the chunk should be tried again.
func (l *Loader) pause(ctx context.Context, err error) bool {
	if !errors.Is(err, ErrRateLimited) || errors.Is(err, ErrIPBanned) {
		return false
	}

	// The server's own number, when it gave one. errors.As walks the chain, so
	// this reaches the internal type through the two %w verbs that translated
	// it — without the caller of this library ever needing to know the type
	// exists.
	var rl *vision.RateLimitError

	var after time.Duration
	if errors.As(err, &rl) {
		after = rl.RetryAfter
	}

	after = min(max(after, l.minPause), l.maxPause)

	l.gate.pause(after)
	l.logger.WarnContext(ctx, "rate limited, pausing the pipeline", "pause", after, "err", err)

	return true
}

// report delivers one progress event, one at a time.
func (l *Loader) report(p Progress) {
	if l.progress == nil {
		return
	}

	l.progressMu.Lock()
	defer l.progressMu.Unlock()

	l.progress(p)
}

// sizeHint estimates how many candles a request will produce, for use as a
// slice capacity and nothing else.
//
// It validates first so that a request which is about to be rejected does not
// buy a 20 MB slice on its way to an error — estimateRows caps its answer, but
// the cap is still 65,536 candles.
func (l *Loader) sizeHint(req Request) int {
	if req.Validate() != nil {
		return 0
	}

	// The request has not been resolved yet — this runs before Fetch commits to
	// any work — so the zero End is filled in here rather than by resolve.
	//
	// Then the *resolved copy* is what endExclusive is asked for, rather than
	// this function adding a nanosecond of its own. There is one place in this
	// library that knows how wide the step between the two range conventions is,
	// and a second one here would be a second thing to remember to change.
	// Value receivers make the copy free of consequence: assigning to req cannot
	// be seen by the caller.
	if req.End.IsZero() {
		req.End = l.now().UTC()
	}

	if req.End.Before(req.Start) {
		return 0
	}

	spec := decodeSpec{Interval: req.Interval, Start: req.Start, End: req.endExclusive()}

	return spec.estimateRows()
}

// minTime and maxTime are the time.Time spellings of the min and max builtins,
// which are constrained to ordered types and so do not accept a struct.
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}

	return b
}

// checkGap decides whether a chunk that came back short is a hole in Binance's
// data, and says so.
//
// # The question is asked about the intersection, not the chunk
//
// A chunk routinely covers more than the request: an archive is a whole month
// or a whole day, and [plan.Consolidate] deliberately widens a plan further.
// Judging emptiness on the chunk's own extent gets both directions wrong.
//
// Too lenient: a delisted pair whose final monthly archive holds the first ten
// days of the month is *not* empty, so a request for the second half of that
// month passes the check, has every candle trimmed away by the reduce step, and
// returns an empty slice with a nil error. That is exactly the silent short
// range the error contract exists to prevent, and widening plans made it the
// normal shape rather than an exotic one.
//
// Too strict: a monthly chunk substituted into days produces daily chunks that
// lie entirely *before* the request, which have no data because the pair had not
// listed yet — and failing the call for them fails a request whose own range is
// completely available. The same arithmetic also produced a message naming a
// span whose end preceded its start.
//
// So the intersection is computed first. An empty one means the chunk cannot
// contribute to this request at all, and nothing about it is worth reporting.
func checkGap(klines []Kline, req Request, c plan.Chunk, now time.Time) error {
	// Both operands of the min must be exclusive ends or the intersection is
	// one nanosecond short at the tail, which would let a missing final candle
	// pass as "outside the request".
	from, to := maxTime(c.Start, req.Start), minTime(c.End, req.endExclusive())
	if !from.Before(to) {
		return nil // the chunk lies outside the request entirely
	}

	for _, k := range klines {
		// Sorted ascending, so the first candle inside the intersection ends
		// the question. Consolidation can put a great many candles in front of
		// it, which is why this stops rather than counting.
		if !k.OpenTime.Before(from) && k.OpenTime.Before(to) {
			return nil
		}
	}

	if !expectsCandles(from, to, req.Interval, now) {
		return nil
	}

	// RFC3339Nano rather than RFC3339, and the reason is a message this once
	// got wrong. The intersection is half-open and its end can now sit one
	// nanosecond past a whole instant — a caller whose End is 2025-01-02T00:00
	// is asking for the single candle that opens there, so the span checked is
	// [2025-01-02T00:00:00Z, 2025-01-02T00:00:00.000000001Z). Formatted with
	// RFC3339 both bounds print identically and the message reads as an empty
	// range that somehow failed. RFC3339Nano omits trailing zeros, so every
	// span that lands on a whole second still prints exactly as before.
	return fmt.Errorf(
		"%s %s [%s,%s): no candles from any source: %w",
		req.Symbol, req.Interval,
		from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano),
		ErrNotAvailable)
}

// expectsCandles reports whether a span that produced nothing is a hole in
// Binance's data rather than one that simply has not finished happening.
//
// The distinction is the whole of [Loader.Fetch]'s error contract. An empty
// month in 2024 means data is missing and the caller must be told; an empty
// half-hour at the end of a range means the current candle has not closed yet,
// which is the ordinary state of every request that ends at "now" and must not
// be an error.
//
// So the question asked is precise: could a candle have both opened and closed
// inside [start, end) by now? [alignUp] finds the first open time on the grid at
// or after start, and it counts only if it falls inside the span and its
// interval has already elapsed. Anything else legitimately holds nothing.
//
// Note what is *not* checked: whether the span is full. Archives are routinely
// partial — SHIBUSDT's 2021-05 daily archive holds 22 rows for a 31-day month,
// because the pair was listed on the 10th — and a completeness test here would
// reject real data. The one-directional rule is the same one codec.go applies
// to rows.
func expectsCandles(start, end time.Time, iv Interval, now time.Time) bool {
	open, ok := alignUp(start, iv)
	if !ok {
		return false
	}

	if !open.Before(end) {
		return false // no candle opens inside this span at all
	}

	return !intervalEnd(open, iv).After(now)
}

// monthStart returns midnight UTC on the 1st of t's month.
//
// internal/plan has one of these too, and duplicating four lines is the price
// of that package importing nothing but errors, fmt and time. See its package
// comment for what that buys.
func monthStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// semaphore is a counting semaphore built from a buffered channel, which is the
// idiomatic Go spelling: a send takes a permit, a receive returns one, and the
// channel's capacity is the limit. It needs no mutex and no condition variable
// because a channel already is both.
//
// It is a named type over a channel rather than a struct so that the zero value
// is unusable — a nil channel blocks forever on send — which is what makes
// forgetting [newSemaphore] a hang at the first acquire rather than an
// unlimited pool that looks like it works.
type semaphore chan struct{}

func newSemaphore(n int) semaphore {
	return make(semaphore, n)
}

// acquire takes a permit, or returns the context's error if the wait is
// cancelled first.
func (s semaphore) acquire(ctx context.Context) error {
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release returns a permit. It must be called exactly once per successful
// acquire; a deferred call at the top of the worker is how that is kept true
// along every path out, including a panic.
func (s semaphore) release() {
	<-s
}

// gate holds every worker back until an instant, and is how one chunk's rate
// limit becomes the whole pipeline's problem.
//
// The alternative — each worker backing off on its own — is what earns an IP
// ban: forty workers that each ride out a 429 privately are still forty workers
// hitting a quota that is counted per address, and the endpoint escalates a
// client that keeps ignoring it into an HTTP 418. One shared deadline means one
// pause.
//
// The zero value is an open gate.
type gate struct {
	mu    sync.Mutex
	until time.Time
}

// pause closes the gate for d, or leaves it as it is if it is already closed
// for longer. Never shortening an existing pause is what stops two workers
// hitting the same 429 from talking each other down to the smaller of the two
// delays.
//
// # Which clock this uses, and why it is not the injected one
//
// time.Now, deliberately, where every calendar decision in this file goes
// through the Loader's injected clock. The project's rule draws the line there:
// time is injected into *logic* so that a rule about the 1st of the month can
// be tested without waiting for one, and *timing* — delays, backoff, pacing —
// is tested inside a testing/synctest bubble, which gives the whole time
// package a fake clock and costs no real time. A pause is timing. Reading the
// injected clock here would also be wrong rather than merely unconventional: a
// test that freezes it to a fixed instant would produce a deadline that never
// arrives.
func (g *gate) pause(d time.Duration) {
	until := time.Now().Add(d)

	g.mu.Lock()
	defer g.mu.Unlock()

	if until.After(g.until) {
		g.until = until
	}
}

// wait blocks until the gate is open, or the context ends.
//
// The loop re-reads the deadline after every sleep rather than sleeping once:
// another worker may have extended the pause while this one was waiting, and a
// single sleep would let it through early — precisely into the server that is
// already refusing everybody.
func (g *gate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		until := g.until
		g.mu.Unlock()

		d := time.Until(until)
		if d <= 0 {
			// Checked even when there is nothing to wait for, so that a
			// cancelled context cannot buy one more chunk's worth of work.
			return ctx.Err()
		}

		t := time.NewTimer(d)

		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

// VerifyCache re-hashes every archive in this Loader's cache against the
// .CHECKSUM sidecar Binance published with it, yielding one [CacheEntry] per
// archive:
//
//	for entry, err := range loader.VerifyCache(ctx) {
//	    if err != nil {
//	        return err // the cache directory could not be walked
//	    }
//	    if entry.Err != nil {
//	        fmt.Println(entry.Path, entry.Err)
//	    }
//	}
//
// It is the on-demand half of the library's integrity guarantee. Archives are
// verified once, when they are downloaded, and never again: re-hashing a 93 MB
// file on every read would cost more than the CSV parse the second cache tier
// exists to avoid. That leaves one gap — a file that was correct when written
// and was damaged afterwards — and this is how it is closed, whenever a caller
// decides to spend the I/O.
//
// # Two error channels, two meanings
//
// The yielded error ends the iteration: the cache directory could not be
// walked, or ctx was cancelled. A bad *archive* is not that and does not stop
// anything, because reporting every bad file is the whole job — it arrives in
// [CacheEntry.Err] with a nil error beside it. This is the same split
// [Loader.Stream] uses, and the loop above is the shape both want.
//
// Nothing is deleted, downloaded or repaired. What to do about a mismatch is
// the caller's decision, and the cache heals itself either way: an archive that
// is removed is downloaded again on the next request for it.
//
// A cache directory that does not exist yet yields nothing and no error, since
// that is indistinguishable from a cache with no files in it.
func (l *Loader) VerifyCache(ctx context.Context) iter.Seq2[CacheEntry, error] {
	return l.cache.verify(ctx)
}
