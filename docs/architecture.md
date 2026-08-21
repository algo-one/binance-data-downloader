# Architecture

> **Status:** all nine stages are complete — 0 (scaffolding), 1 (domain types),
> 2 (time and availability), 3 (parsing), 4 (downloader), 5 (cache), 6 (REST
> fetcher), 7 (loader orchestration), 8 (CLI) and 9 (documentation and release).
> Everything described below exists. The remaining step is the v0.1.0 tag
> itself.

## What the library does

Turn a request — a symbol, an interval, and a time range — into a contiguous,
verified slice of candles, as fast as possible on the second and subsequent
runs.

## Where the data comes from

Binance publishes historical klines in two places, and neither alone is enough.

**Bulk archives** at `https://data.binance.vision`, as monthly and daily ZIPs:

```
/data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip
```

Each has a `.CHECKSUM` sidecar holding a SHA-256 of the archive. Archives are
the bulk of any historical range and are immutable once published — which is
what makes aggressive caching safe.

**The REST mirror** at `https://data-api.binance.vision/api/v3/klines`, for the
tail. Archives lag real time by roughly a day (verified 2026-08-17: the
2026-08-16 daily archive existed, 2026-08-17 did not), so anything newer has to
be paginated out of the API.

**A third endpoint answers "what exists?"** Binance exposes the underlying S3
bucket listing publicly:

```
https://s3-ap-northeast-1.amazonaws.com/data.binance.vision
    ?delimiter=/&prefix=data/spot/monthly/klines/BTCUSDT/1h/
```

This matters more than it looks. Availability could be guessed at with a
calendar heuristic ("is it past the first Tuesday of the month?"), but Binance
is not contractually bound to any such schedule, and a heuristic that is wrong
for one month drops days without saying so. Asking the bucket what it actually
contains removes the guess entirely.

Findings from probing it directly on 2026-08-18, all of which shape the code:

- **The archives have holes.** `BTCUSDT-1mo-2024-03.zip` does not exist, while
  `2024-02` and `2024-04` both do. No date arithmetic predicts a missing month
  in the middle of a published range; only the listing reveals it. **A second
  hole turned up on 2026-08-21**, when `bmd list -symbol BTC/USDT -interval 1mo
  -archives` was run against the live bucket for the first time: `2026-03` is
  missing as well, out of 105 published months. Two holes rather than one is
  what turns this from an anecdote into a rate — roughly one month in fifty is
  absent, on the most heavily traded pair Binance has.
- **The interval availability tables in the Python source are wrong.** They
  declare `1s` daily-only. Binance publishes `1s` monthly archives too —
  `BTCUSDT-1s-2024-03.zip` is 93 MB of real data. See `interval.go`.
- **The S3 hostname is unavoidable.** Sending the listing query to
  `data.binance.vision` returns the HTML file-browser page — a static page that
  calls this same S3 API from JavaScript — with HTTP 200 and content-type
  `text/html`. Only the regional S3 endpoint answers with XML. Listing therefore
  depends on the provider, the region and the bucket name, none of which Binance
  promises to keep; see "Listing is an optimisation" below.
- **An unknown symbol is not a 404.** A prefix matching nothing answers HTTP 200
  with a well-formed, empty `ListBucketResult`. "This symbol never existed",
  "this interval has no archives yet" and "the prefix is misspelt" are one
  indistinguishable answer, and it looks exactly like success.
- **Listings paginate sooner than they look.** S3 caps a response at 1000 keys,
  and every `.zip` has a `.zip.CHECKSUM` beside it, so one page holds 500 days.
  BTCUSDT's daily history runs to seven pages. Passing a `marker` seeks straight
  into the range — keys sort lexicographically and the dates in them are ISO, so
  lexicographic order is chronological order — which makes the cost proportional
  to the range requested rather than to how long the symbol has traded.

### Listing is an optimisation, never the source of truth

Because the listing endpoint bakes in three facts about AWS that Binance could
change without notice, no correctness property may rest on it. The rule the code
enforces: a **failed** listing is never read as an **empty** one. `List` returns
`(objects, nil)` when the bucket answered — possibly with nothing — and
`(nil, err)` when it did not, and the planner refuses to emit a plan on error.
Conflating the two is precisely how the ported implementation returned ranges
with days missing and no error to show for it.

The rule needs one line of enforcement that is easy to miss. `encoding/xml`
matches child elements by name and ignores the document's root, so a struct
without an `XMLName` field accepts *any* well-formed XML and leaves every field
at its zero value — which is to say, a listing that is not truncated and holds
no keys. A proxy's login page or a CDN error document answered with HTTP 200
would decode into "Binance has published nothing here", with a nil error to
vouch for it. `listBucketResult` therefore pins its root element, and anything
else is a parse error: the honest answer is that we do not know what is there.

The same rule has a second edge, on the way *in* rather than out. Every value
interpolated into a bucket path formats into something that looks like a legal
path segment, so none of them fails loudly on its own: `fmt.Stringer`'s fallback
renders an unset interval as `Interval(0)`, `path.Join` drops an empty symbol so
the interval slides into its slot, and `path.Clean` resolves a `..` away. Each
produces a well-formed key naming a *different* object, which 404s, which the
root package then reports as `ErrNotAvailable` — a statement about Binance's
calendar standing in for a bug in the caller. `archivePrefix` therefore
validates all four of market, aggregation, interval and symbol, and asserts the
symbol is already normalised rather than normalising it, since a value
normalised in two places is one that gets normalised in only one of them.

`fetchArchiveIndex` closes the same gap one level up. `HasMonthlyArchives` and
`HasDailyArchives` both begin with `IsValid()`, so an interval that fails
validation reported false to both, launched zero goroutines, and let `g.Wait`
return nil over an empty `errgroup` — an empty index and a nil error, produced
without a request being made. It is guarded on the granularities rather than on
`IsValid` so that an interval Binance publishes at neither still says so.

## The pipeline

```
Request ──► PLAN ──────► []chunk ──► EXECUTE ──────► []Kline ──► REDUCE ──► []Kline
            (pure)                   (bounded pool)              (merge,
            no I/O                   + singleflight               filter)
```

**Plan** (`internal/plan`) — pure functions, no I/O. `Expand` turns a resolved
`Request` into chunks, each one of `KindMonthlyArchive`, `KindDailyArchive` or
`KindRESTRange`; `Substitute` is the pure rule for what to try when an archive
turns out to be missing (month → days → REST); `Consolidate` is the threshold
that decides whether a partly-wanted month is cheaper as one archive or as its
days, and takes availability as a predicate so that it can stay pure. All
calendar logic lives here, and the package imports only `errors`, `fmt` and
`time` — so it is *incapable* of I/O, not merely expected to avoid it.

The chunks are sorted, contiguous and cover the whole requested range, and
`Expand` verifies that itself on every call rather than trusting the arithmetic.
The check is one pass over a handful of chunks, and it converts the only failure
mode nobody would notice — a missing day in the middle of a range, returned with
no error — into a loud one.

**Execute** — one bounded worker pool over a flat list of chunks, limited by a
semaphore held on the `Loader` so the budget spans calls rather than one
`errgroup`. `singleflight` collapses duplicate chunks across overlapping
requests. Context cancellation reaches every goroutine.

The flatness is the point. Nested limits — one for months, another for the days
inside a month — deadlock or starve as soon as an outer unit holds a permit
while merely *waiting* on its constituent inner units: a task occupying a slot
while doing nothing. One queue of uniform work units cannot have that problem.

**Reduce** — chunks arrive already sorted, and in order, so there is no separate
merge pass: trimming to the requested range and dropping anything that does not
strictly follow the last candle yielded is the whole of it. Doing it inline means
a duplicate is dropped before it is yielded rather than after the range has been
assembled, which is what lets `Stream` bound its memory. See "Orchestration"
below.

## Downloading

`internal/vision` is the only package that speaks HTTP, and `download.go` in the
root is the seam where its answers become this package's vocabulary. Four things
about it were decided rather than defaulted.

**One `http.Client` for the process.** An `http.Client` is not a connection; it
is a handle onto a `Transport`, and the `Transport` holds the pool. A client per
request therefore builds a pool, uses one connection from it and throws it away
— 100–200 ms of TCP and TLS handshaking before a byte of payload, on every
archive. `NewHTTPClient` clones `http.DefaultTransport` (keeping proxy support
and HTTP/2 negotiation, which a hand-built `&http.Transport{}` silently drops)
and raises `MaxIdleConnsPerHost` from its default of **2** to 64, since every
request in this library goes to the same host.

It is also the *default*, not merely the recommendation. `NewLister` and
`NewDownloader` accept a nil client, and defaulting that to `http.DefaultClient`
would hand anyone who took the documented default a transport keeping two idle
connections per host — this requirement's own failure, from the constructor
advertising it. A package-level `sync.OnceValue(NewHTTPClient)` supplies one
shared client instead, built lazily so that merely importing the package does
not read the proxy environment and start a dialer. Listing and downloading share
it deliberately: a `Transport` pools per host, so one client keeps separate pools
for S3 and for `data.binance.vision` rather than mixing them.

`Client.Timeout` is deliberately left unset. It bounds the *entire* exchange
including the body read, so any value large enough to let a 93 MB `1s` archive
finish on a slow link is far too large to catch a hung connection, and any value
small enough to catch a hang kills a transfer that was streaming perfectly well.
The two jobs are split instead: `ResponseHeaderTimeout` catches a server that
accepts a connection and says nothing, and the per-call `context.Context` bounds
the operation — which is the better tool anyway, because the caller owns it.

**Every response body is drained, not merely closed.** `net/http` returns a
connection to the idle pool only once its body has been read to EOF, so
`Close()` on a partly-read body closes the TCP connection instead of pooling it.
That is invisible while everything succeeds and fires exactly when it hurts: a
burst of 503s, or a month of 404s for archives that do not exist, would
re-handshake for every one. `TestRetriesReuseOneConnection` counts connections
rather than requests, and measures 1 instead of 4.

**Retries are a function, not a `RoundTripper`.** Wrapping the transport would
give every request retries for free, but a transport sees bytes rather than
meaning — it cannot know that a 404 on an archive is routine while a 404 on a
listing is a misconfiguration — and retries hidden inside it cannot be pinned by
counting calls at the call site. `doWithRetry` is shared by the lister and the
downloader:

| | |
| --- | --- |
| Attempts | 4 — 500 ms, doubling, capped at 8 s (≈3.5 s of waiting in total) |
| Jitter | Full: a uniform draw from `[0, ceiling)`, so forty workers that all receive a 503 do not retry in lockstep |
| Retried | Transport errors, 408, 425, 429, 500, 502, 503, 504 |
| Never retried | **404** above all — the most common non-200 here, and a fact rather than a hiccup — plus every other 4xx |
| `Retry-After` | Honoured in both RFC 9110 forms (delta-seconds and HTTP-date), as a **floor** — clamped to `[BaseDelay, 30 s]`, then jittered on top |
| Cancellation | The backoff is a `select` on `ctx.Done()`, so an interrupted run stops mid-wait rather than sitting out the delay |
| Request bodies | Rewound via `GetBody` on every retry. `http.Request.Clone` copies the `Body` field rather than the reader, so a retried request would otherwise send nothing |

`Retry-After` is a floor rather than a replacement because taking it literally
undoes both of the properties above it in that table. A header is the one input
that makes every worker's delay *identical by construction* — forty workers each
told to wait 1 s re-fire on the same millisecond, which is the thundering herd
the jitter row exists to prevent, reassembled by the code deferring to the
server. And `retryAfter` reports zero both for a literal `Retry-After: 0` and
for any HTTP-date already elapsed, which a clock two seconds fast produces from
an ordinary "two seconds from now"; `Policy.wait` treats anything at or below
zero as no wait at all, so every remaining attempt would fire back-to-back at a
server that had just said it was overloaded. Clamping low and adding jitter on
top means nobody retries earlier than the server asked and nobody retries in
lockstep.

Exhausting the attempts returns the last *response*, not an error: the caller
decides what a status means, and synthesising an error would discard the body,
which is the only diagnostic there is.

A 429 that survives every attempt is the exception worth naming, because it is
not a fact about one request — it says the pipeline as a whole is going too
fast, and only the layer owning the worker pool can act on that. `statusError`
therefore returns a `*vision.RateLimitError` carrying the server's `Retry-After`
verbatim, unclamped: the 30 s cap bounds what this package will wait *inside*
one request, while how long a whole pipeline should pause is Stage 7's policy to
set. `errors.Is(err, ErrRateLimited)` still works through the type's `Unwrap`,
so a caller that only wants the condition never learns the struct exists.

**Archives are hashed as they stream.** `Download` writes through an
`io.MultiWriter` into both the destination and a `sha256.Hash`, so a 93 MB
archive is verified for the price of the copy that was happening anyway — no
buffering, no reading the file back, one 32 KiB copy buffer regardless of size.
That is what makes verifying *every* download affordable enough to be
non-optional, where the ported implementation stored checksums it never compared
against anything.

The sidecar is fetched first, before the archive. It is 91 bytes, so an
unpublished month costs one tiny request rather than a large transfer that ends
in a 404, and every archive that does arrive is compared against a hash already
in hand — there is no path where bytes are written, the sidecar then fails to
load, and the caller is left holding an unverified file that looks finished.

The parse of that sidecar is liberal about whitespace and strict about the hash:
64 characters, hexadecimal, or it is rejected. It also checks that the sidecar
names the archive it was fetched for, because a mirror serving the wrong
directory's sidecar otherwise fails later as a checksum mismatch, sending
whoever investigates hunting for a corruption that never happened.

**Errors cross the boundary by translation.** `internal/vision` cannot import
the root package — the dependency runs the other way — so it returns its own
`ErrNotFound` and `ErrRateLimited`, and `translateVisionError` re-labels them as
`ErrNotAvailable` and `ErrRateLimited`. It uses two `%w` verbs in one
`fmt.Errorf`, which `errors.Is` walks to either: the transport's message (naming
the key and the status) survives alongside the sentinel callers branch on.

## Parsing

`codec.go` is where the format Binance publishes meets the types this package
defines. An archive is a ZIP holding one CSV member with twelve columns and, for
spot, no header row. Reading that is easy; three things about it are not, and
each was a bug in the implementation this replaces.

**The header is sniffed, per file.** Spot archives carry none and futures
archives do. The test examines two fields rather than one: a first field that is
not an integer suggests a header, but it equally describes a data row with a
corrupt timestamp, and dropping that as a "header" would lose a candle in
silence. A genuine header has names in every column, so the price field settles
it. A first row where nothing parses at all is read as a header — a file of
column names is plausible, a row corrupt in twelve places is not.

**The timestamp unit is detected per row.** Binance switched these files from
milliseconds to microseconds at 2025-01-01T00:00Z, verified against the real
archives for 2024-12-31 and 2025-01-01, which are the last and first days of
each era and are committed as fixtures. The threshold is 1e14, and it cannot be
approached from either side by real data: read as milliseconds that value is the
year 5138, read as microseconds it is March 1973, and Binance began trading in
2017. The ported implementation read the unit from a file's final row and
applied it to every row.

**Every candle is checked against the file it came from.** The decoder is told
which interval and which period the archive claims to cover, and rejects a
candle that falls outside the period, sits off the interval's grid, closes
outside its own interval, or does not strictly follow the previous row. This is
what makes a unit misdetection loud: microseconds read as milliseconds land in
1970, which is not inside any archive period.

The eight decimal fields are checked against the relationships that hold between
them by definition — low is the lowest of the four prices, nothing is negative,
and a taker-buy volume cannot exceed the total it is part of. These exist to
catch the one corruption per-field parsing cannot: a column index off by one
produces values that parse perfectly and are simply wrong. Each rule was
verified against 132,506 real rows first, because a false positive here rejects
a whole archive.

The grid test is done in **microseconds**, not milliseconds. Since 2025 the
files are written in microseconds, so a millisecond-granularity comparison is
coarser than the data it validates and would wave through a timestamp off the
grid by less than 1 ms.

Decoding takes a `context.Context`, like everything else in this package that
touches a file. A consumer ranging over the iterator can stop it with `break`,
but the collecting form's loop belongs to the library, and a 1s month is several
seconds of work a cancelled backtest should not wait for. The check runs every
1024 rows, and a cancellation is reported as `context.Canceled` — never as
`ErrCorruptArchive`, since nothing is wrong with the bytes.

The check is deliberately one-directional. Every candle must be inside the
period; the period need not be full of candles. `SHIBUSDT-1d-2021-05` holds 22
rows for a 31-day month because the pair was listed on the 10th, and demanding
completeness here would reject real data. Whether a *range* is covered is the
planner's question, asked about chunks rather than rows.

### Alignment grids

Candle open times sit on a grid, and three intervals anchor theirs somewhere
other than the Unix epoch:

| Interval | Grid | How it is known |
| --- | --- | --- |
| `1s` … `1d` | Multiples of the interval from the epoch | They divide 24 h evenly and the epoch is midnight UTC |
| `3d` | Three-day multiples from **1970-01-02** | Measured against live archives for 2018-01, 2021-06, 2024-03/04/05 and 2025-02. The grid does not restart monthly: March 2024's last candle opens on the 31st, April's first on the 3rd |
| `1w` | Monday 00:00 UTC | The epoch was a Thursday, so "multiples of seven days" rejects every real weekly candle |
| `1mo` | The 1st, 00:00 UTC | Calendar months are 28–31 days and have no fixed duration |

The grid is read in two directions. `aligned` asks whether an instant sits on it,
which is what validates a decoded candle; `alignUp` computes the next instant
that does, which is what lets the loader ask whether a candle could have opened
and closed inside a span. They are kept together in `codec.go` and tested against
each other rather than against a list of answers, because two functions
disagreeing about where the grid lines fall is a bug with no symptom — a candle
quietly treated as missing, or a gap quietly excused.

### `CodecVersion`

A compile-time constant, bumped in the same commit as any change to what the
decoder produces from given bytes. The cache stamps it into every derived file;
see `docs/caching.md` for why a checksum alone cannot detect a parser fix.

### Cost

Measured at 1.39 µs per row (Apple M1 Pro, Go 1.26.5, one allocation per row —
the CSV record string), so a 44,640-row month of `1m` candles decodes in about
62 ms. That lands inside the 60–70 ms figure `docs/caching.md` predicted before
the code existed, and it is the number the two-tier cache exists to avoid
paying on every backtest run.

## Caching

`docs/caching.md` is the design and the measurements; this is what the shape of
it means for the rest of the pipeline.

`cache.klines` is the cache's whole surface within this package: one archive in,
its candles out. Stage 7 calls it once per chunk and never learns which tier
answered, whether anything was downloaded, or whether the derived file had to be
rebuilt. Everything below that line — the two tiers, the atomic writes, the
deduplication — is `cache.go`'s business.

Four things about it were decided rather than defaulted.

**A hit reads two small files and no archive.** The `.CHECKSUM` sidecar gives
the hash, the Parquet footer gives what it was built from, and the rows follow
if the two agree. Tier 1 is not opened and not re-hashed, which is what keeps a
hit ten times cheaper than parsing the archive would be — and which also means
a cache whose archives have been pruned still serves every read that does not
need a rebuild.

**Tier 2 is read column by column, not row by row.** The obvious implementation
— parquet-go's `GenericReader`, one Go struct per row through reflection — was
written first and measured at 26.5 ms per symbol-month against the 62 ms of CSV
parsing it replaces. That margin does not justify a second copy of the data on
disk. Reading each column as one contiguous run instead takes 6.1 ms, and reads
positionally, which is safe only because the file's schema is verified against
the expected column names and physical types first: a column order off by one
puts the high price in the low price's field and every value still parses, and a
column with the right name and the wrong storage would reach the generic page
reader and panic inside parquet-go rather than return an error.

**Deduplication registers before any I/O.** `singleflight` is keyed on the
Parquet path and entered on the way in, so a saturated Stage 7 pool cannot let
two workers past the check before either registers — correctness requirement 5,
and one of the ported implementation's real bugs. Waiters get a copy of the
candles, because Stage 7 trims ranges in place.

**Nothing is written to a final path.** Every file goes to a temporary file in
the destination directory and is renamed in, so an interrupted run leaves either
the old file or the new one. A Parquet file truncated by a crash is the failure
worth naming: its footer is written last, so a torn one is not readable at all —
but only because nothing wrote it in place.

## The REST tail

The archives lag real time by roughly a day — verified on 2026-08-17, when the
2026-08-16 daily archive existed and the 2026-08-17 one did not — so a request
ending today has a tail that has never been written to a file. `internal/plan`
already accounts for it: `Expand` emits a `KindRESTRange` chunk past the archive
frontier, and `Substitute` ends its ladder there, because `3d`, `1w` and `1mo`
have no daily archives and a hole in their monthly ones (the real, missing
`BTCUSDT-1mo-2024-03`) has nowhere else to go.

`data-api.binance.vision` is a **different service from the bucket** despite the
shared domain. It is the read-only half of the trading API: no key, market data
only — and, unlike a static file server, it has a quota.

**The rows are never decoded as numbers.** A kline arrives as a JSON array
mixing unquoted numbers with quoted decimals, and the one-line decode into
`[]any` turns every one of those numbers into a `float64`. That is precisely
what this library exists not to do. Each element is lifted out as
`json.RawMessage` and rendered back to the exact characters Binance sent, so the
digits reach `udecimal` unrounded.

**One decoder, two formats.** The response carries the same twelve columns in
the same order as the CSV inside an archive (verified against the live endpoint
on 2026-08-20), so `decodeRow`, `checkTimes` and `checkValues` police both. The
two column counts live in packages that cannot import each other, so their
equality is asserted at compile time rather than assumed. `restapi_test.go`
proves the two paths agree by decoding a real archive and a real REST capture of
the same day and comparing all 24 candles field by field.

**The candle in progress is dropped.** The endpoint always returns the interval
currently forming, whose volume and close price are still moving; archives never
contain one. Everything downstream assumes a candle is settled once seen — the
Parquet tier caches it, Stage 7 will merge on open time — so admitting one makes
two identical requests seconds apart disagree, with neither being wrong. A
candle is returned once `intervalEnd(OpenTime)` has passed, compared against an
injected clock rather than `time.Now`. It is a field on the fetcher, so Stage 7
can offer it as an option.

**Pagination terminates three ways**, because any one alone is a bug waiting for
the right response: an empty page, a page shorter than requested, or a cursor
that reaches the end of the range. The cursor is taken from the last candle of
each page and advanced to the instant its successor opens. Since every candle
opens at or after the cursor that requested it, the cursor strictly increases,
so the loop cannot stand still.

It advanced by one millisecond first, which is one millisecond *into* the candle
it means to move past. Binance documents `startTime` as inclusive without saying
inclusive of what, and that cursor is only safe under the reading where the
filter is on open time; under the other — the kline whose interval *contains*
the timestamp — page 2 repeats the candle page 1 ended with and the
strict-increase check fails the whole fetch. Landing on the next open is correct
under both, so the question never has to be answered. No test could have caught
it either, since a handler written in this repository encodes whichever reading
its author assumed; `restapi_test.go` now runs the same range against a handler
implementing each.

### Rate limiting, and why only here

The bucket has no quota; this endpoint does. Measured on 2026-08-20 via
`/api/v3/exchangeInfo`:

```
REQUEST_WEIGHT   6000 per minute, per IP address
```

and one klines call costs 2. Exceeding it is worse than being slow. Binance
escalates a 429 that a client keeps ignoring into an HTTP **418** — an IP ban
running from two minutes to three days, lengthening with repeat offences. That
punishes the address rather than the process, so a ban earned by a history
download also locks out a live trading bot on the same host, and no retry can
undo it.

So the limiter is preventative; the `Retry-After` handling in `retry.go` is the
reactive half, and by the time it runs some damage is already done. It is
`golang.org/x/time/rate`, configured to 40 weight per second against a ceiling
of 100 — deliberately leaving most of the budget unspent, because the quota is
per IP and anything else on the machine draws from the same 6000.

It is process-wide via `sync.OnceValue`, for the same reason the `http.Client`
is. Two limiters each honouring the documented rate permit twice it — each
correct alone and wrong together.

**This started as a hand-rolled bucket and was replaced within the stage**, and
the reason is worth recording because it generalises. The only argument for
owning fifty lines of token-bucket arithmetic was that everything else in
`internal/vision` injects its clock so tests can assert on delays instead of
spending them, and `rate.Limiter` reads `time.Now()` internally.

`testing/synctest` dissolves that argument. A test inside a bubble gets a
private fake clock for the *entire* `time` package, so the limiter's internal
`time.Now()` and its timers are virtual without it knowing, and the test asserts
on exact durations — 50 ms, not "between 40 and 70" — while running in zero real
time. What was left was arithmetic maintained against a canonical implementation
whose cancellation path is more careful than ours: it restores only the tokens
later reservations have not already claimed.

That is what the move to a Go 1.25 floor bought, and it paid for itself twice
over: `x/sync` came off the v0.18.0 pin it only had because v0.20.0 wants 1.25,
and `x/sys` moved to v0.47.0, closing GO-2026-5024 — an advisory left open in
Stage 5 for the same reason.

The limit of the technique is worth stating too, since it decides the shape of
several tests. Time in a bubble advances only when every goroutine in it is
*durably blocked* — blocked on something only another goroutine in the same
bubble can release. A goroutine waiting on a real socket never qualifies, so a
test driving an `httptest.Server` cannot use a bubble. Those tests assert on
request counts instead, and `retry.go` keeps `Policy.Now` and `Policy.After` for
exactly that reason.

The reservation is spent **per request, not per call**. `Policy.Reserve` is
consulted inside the retry loop, because that is the only layer that knows how
many requests one call becomes: a retryable status turns one call into as many
as `MaxAttempts`, and it does so exactly when the quota is under pressure, since
429 and every retryable 5xx are the statuses that cause the extra attempts. Spent
per call, the limiter that exists to pre-empt an IP ban is what permits the burst
that earns one.

A 418 is reported as `*vision.RateLimitError` with `Banned` set, and its
`Unwrap` returns **both** `ErrRateLimited` and `ErrIPBanned`. Those are
internal; the root package re-attaches the same pair from `errors.go`, so a
library consumer asking only "should I slow down?" gets a yes and one that can
tell a ban from a throttle can. `X-MBX-USED-WEIGHT-1M` is decoded onto
`vision.KlinesPage` and available to the layer above, which does not read it
yet — Stage 7 owns progress and diagnostics and is where it becomes visible.

**A status decides whose fault a failure is; the body only decides the detail.**
Binance answers a 5xx with the same `{"code","msg"}` document it uses for a 400,
so a body-first reading reports an outage as the caller's own bug — and
`ErrInvalidRequest` is documented as always the caller's to fix, which would have
Stage 7 refuse to retry the one failure retrying exists for. A 4xx therefore
carries `vision.ErrBadRequest` and a 5xx `vision.ErrServerError`, whether or not
either explained itself. The 5xx has no public sentinel and reaches the caller
unrecognised: an error that arrives unlabelled is a smaller lie than one that
arrives mislabelled.

**A 404 from this endpoint is not a calendar fact.** The bucket is a static file
server, where a missing object genuinely means the month was never published;
the REST endpoint answers a range it has nothing in with 200 and an empty array,
so a 404 from it means the base URL or the path is wrong. `translateRESTError` is
that one difference — it leaves the 404 untyped, where the shared translation
would make it `ErrNotAvailable` and Stage 7's requirement-4 policy would degrade
the whole REST tail to nothing and report success.

## Orchestration

`loader.go` is the arrangement everything below it was built for: probe what
Binance published, decide where each chunk comes from, run them without swamping
anything, and join the results back into one contiguous range. Six things about
it were decided rather than defaulted.

**The listing is consulted before anything is fetched.** `Expand` assumes every
archive it names exists, because it has no network to ask with; the bucket
listing already knows better, and `archiveIndex.has` was sitting unused since
Stage 2 for exactly this. `Loader.route` walks the plan and settles it against
what the bucket actually holds, in both directions.

*Downgrade*: an archive the listing does not have goes down the ladder before a
request is spent discovering it, which turns the real `BTCUSDT-1mo-2024-03` hole
from a 404, a fan-out into thirty-one daily chunks that do not exist either, and
sixty-two more 404s, into a REST range chosen before a request is spent.

*Upgrade*: a run of daily chunks becomes the month covering it when that month
exists and enough of it is wanted — the threshold below. This runs first, and the
order is the whole point: consolidating onto a month that turns out to be absent
is strictly worse than not consolidating, because the downgrade then fans out the
entire month instead of the days that were asked for.

The index is authoritative for this because of where the listing seeks from. It
is marked at the 1st of the month the range starts in, which is at or before
every chunk `Expand` can emit — monthly chunks begin on the 1st, daily ones at a
midnight inside the range. A marker any later would have `has` answer "not
listed" for periods it was never asked about, which is the failed-lookup-read-as
-absent conflation the whole availability design exists to prevent.

**Adjacent REST ranges are joined.** Substitution produces runs of them: a month
that was never published becomes thirty-one days that were never published
either, each falling through to its own REST range. Fetching those separately is
thirty-one paginated calls against the one endpoint in this library with a quota,
to learn thirty-one times over that Binance has nothing there. Joining is exact
rather than approximate because the ranges are half-open, so "adjacent" is
`End.Equal(Start)` and nothing has to be added or subtracted.

**The plan phase runs under a permit too.** The bucket listing is I/O — two
concurrent requests for most intervals — and it happens before any chunk is
fetched, so a limit taken only per chunk does not bound it at all. `FetchAll`
over *N* requests opened 2*N* simultaneous listings whatever `WithConcurrency`
said, and did it at the one moment of the call when every request is at the same
stage. That is the shape that earns a 429 and then the 418 the pause is written
not to wait out. The permit is released before the chunks are fetched, so nothing
holds one while waiting for another.

What is *not* deduplicated is the listing itself: two requests for the same
symbol and interval list the bucket twice, because the index is built per call.
Memoising it would mean caching "this month does not exist yet", which is how a
process decides at 00:05 that today has no data and believes it until restarted.

**The pool is flat, and permits are taken in chunk order.** One queue of uniform
work units, one limit, and nothing acquires a permit while holding one — a chunk
that turns out to be missing is expanded and fetched *inside* the permit it
already has, sequentially. That is slower in the rare case and incapable of the
nested-semaphore deadlock in every case.

The ordering is the part that is easy to miss. `Stream` gives each chunk an
unbuffered channel and consumes them in order, so a worker holds its permit until
its candles have been taken — which is the backpressure that bounds memory to the
concurrency limit rather than to the length of the range. That arrangement
deadlocks if a later chunk can take the last permit while an earlier one is still
waiting for one, so permits are acquired by a producer goroutine in chunk order,
before each worker is launched. The chunk the consumer is waiting for is
therefore always already running.

It is a semaphore on the `Loader` rather than `errgroup.SetLimit` for one reason:
`SetLimit` bounds a single group, and the limit has to span calls. `FetchAll`
over twenty requests uses the same budget as one `Fetch`, or the setting would
mean twenty times what it says.

**Requirement 5 holds by construction, not by ordering.** The requirement is
worded as "registration happens before the limit is acquired", which describes
the shape of the bug it came from: the ported implementation checked, then took a
permit, then registered, so a saturated pool let several tasks through the gap.
Here the check, the registration and the work are all inside `singleflight`,
which holds its own lock across them — so nothing can come between them and the
permit's position does not matter. `TestConcurrentRequestsFetchAnArchiveOnce`
saturates the pool deliberately, holding the first archive request open in the
server until all eight callers have queued behind it, because a test that merely
starts eight goroutines passes just as happily when the deduplication is missing.

**A rate limit pauses the pipeline; a ban stops it.** By the time an error
reaches this layer, `internal/vision` has already retried four times with
backoff, so a 429 that survives is not a statement about one request — it says
the pool is too wide or the range too large, and only the layer holding the pool
can act on that. A shared gate closes for the server's own `Retry-After`, clamped
to `[1 s, 60 s]` and never shortened by a second worker hitting the same 429, and
the chunk is retried at most twice. HTTP 418 is the exception: the address is
barred for two minutes to three days, so waiting is not a strategy and retrying
earns the next, longer one. It is reported immediately.

The clamp exists on both sides. `RateLimitError.RetryAfter` is reported verbatim
and can be zero — Binance sending none, or an HTTP-date a fast clock reads as
already elapsed — and a zero pause is no pause, which has every worker re-fire at
a server that just said it was overloaded. The ceiling is there because the value
is somebody else's number, and a misconfigured proxy answering "retry after 24
hours" must not hang a backtest until tomorrow.

**An empty span is an error, and "empty" is defined precisely.** Requirement 4
says a 404 degrades that chunk only, and the fallback ladder is what delivers
that — the day is recovered from REST and the month still returns. What is left
is the bottom of the ladder: a chunk that produced no candles from any source.
That fails the whole call, with an error wrapping `ErrNotAvailable` naming the
span, and no partial result alongside it.

The question is asked about the **intersection of the chunk with the request**,
never about the chunk alone, and both directions of that matter. A chunk covers
more than the request routinely — archives are indivisible and consolidation
widens plans further — so a delisted pair whose final monthly archive holds the
first ten days of a month is not *empty*, and a check on the chunk's own extent
would pass it, trim every candle away and return success with nothing in it.
Pointing the other way, a monthly chunk substituted into days yields daily chunks
lying entirely before the request, which have no data because the pair had not
listed yet; failing the call for those fails a request whose own range is
completely available. An empty intersection is therefore skipped outright. A backtest handed 22 candles for a
31-day month cannot tell "the market was quiet" from "nine days are missing", and
every number computed from the second is wrong with no sign that it is.

The definition has to exclude the present or it would fire on every ordinary
request. A range ending at "now" ends part-way through a candle that has not
closed, and unclosed candles are deliberately dropped, so the final chunk
routinely produces nothing. `expectsCandles` therefore asks a precise question:
could a candle have both *opened and closed* inside this chunk by now? `alignUp`
finds the first open time on the grid at or after the chunk's start, and it
counts only if it falls inside the chunk and its interval has elapsed.

Note what is not checked. Whether the chunk is *full* is never asked — archives
are legitimately partial, `SHIBUSDT-1d-2021-05` holding 22 rows for a 31-day
month because the pair listed on the 10th — so a completeness test here would
reject real data. It is the same one-directional rule `codec.go` applies to rows,
one level up.

So the guarantee reaches as far as the chunk, which is the granularity Binance
publishes at, and that is worth stating rather than implying. In practice the
case that matters is caught anyway, because an absent period is an absent
*archive*: asking for BTCUSDT from 2015 makes every month before 2017-08 a chunk
of its own with nothing in it. What is not caught is a pair that began trading
part-way through a month whose archive does exist — the range then starts at the
first real candle rather than at `Start`, with no error.

**One consequence worth stating: an error can leave the cache warmer.** A failing
chunk cancels its siblings, but a download the cache has already started is not
stopped — it finishes and populates the cache for the next run, which is the
right trade for a directory that outlives the process. So `Fetch` can return an
error while bytes are still being written, and a program that deletes its cache
directory immediately after a failure is racing work it cannot see. Retrying,
which is the ordinary response, is the case this is optimised for.

### Partial months, and the comment that contradicted the code

`Expand` used to fan a partly-wanted month out into up to 31 daily chunks, while
`Chunk`'s own doc comment argued 130 lines above that over-fetching a whole month
"is not waste in a cache-backed library". Both cannot be right, and Stage 7 owned
the decision.

Neither end of it is right for both shapes of request. Twenty-five days of
January as daily downloads is fifty requests to avoid fetching six days nobody
asked for, and it leaves the cache holding twenty-five files that the next
request for January cannot use as a month. One day of January as a monthly
download is 93 MB of 1s candles to serve 86,400 of them.

So the rule is a threshold — take the month once at least half of it is wanted,
days otherwise — and the comment now states it. Over-fetching genuinely is cheap
here, but "cheap" is a claim about the ratio between what was fetched and what
was wanted, and that ratio is what the threshold bounds: the worst monthly
over-fetch is just under 2× and the worst daily request count is 62.

**Where the rule is applied matters as much as the rule.** It went into `Expand`
first, tested against `Spec.ArchivesThrough` — and that cannot answer the
question it was asked. `ArchivesThrough` is the later of the monthly and daily
frontiers, so it says whether a period is before the frontier, not whether a
monthly archive for it exists. Dailies lag real time by about a day and monthlies
by up to a month plus that day, so the two disagree for most of every month, and
in exactly that window `Expand` would pick a whole-month chunk for a month
Binance has not written yet. The chunk 404s, `Substitute` fans out the entire
month, and a request for twenty days of February costs twenty-nine daily archives
— worse than the plan before the threshold existed.

The trade is only sound with the listing in hand, so it lives in
`plan.Consolidate`, which takes availability as a predicate and stays pure, and
is called from `Loader.route`, which has the index. `Expand` is availability-blind
again, which is what it was always documented to be.

## Package layout

```
.
├── doc.go            package documentation (the pkg.go.dev landing page)
├── version.go        module version, read from the embedded build info
├── interval.go       Interval + which intervals exist at which aggregation
├── market.go         Market, DataType
├── symbol.go         symbol normalisation across three formats
├── kline.go          the Kline struct and column helpers
├── errors.go         sentinel errors
├── request.go        Request, validation, per-call resolution of End
├── availability.go   bucket paths, archive names, the archive index
├── codec.go          zip → csv → []Kline; CodecVersion lives here
├── download.go       archive key → verified bytes; vision errors → sentinels
├── cache.go          the two tiers, atomic writes, singleflight
├── parquet.go        tier 2: the schema, the footer stamp, the column reader
├── restapi.go        the REST tail: pagination, partial-candle policy
├── options.go        functional options, Progress, Source
├── loader.go         plan/execute/reduce; Fetch / FetchAll / Stream
│
├── internal/
│   ├── plan/         range → []Chunk (pure; imports only errors, fmt, time)
│   └── vision/       every HTTP call: one shared client, retries, both hosts
│       ├── client.go    the process-wide http.Client and its Transport
│       ├── retry.go     Policy, doWithRetry, Retry-After
│       ├── body.go      draining and quoting response bodies
│       ├── listing.go   S3 ListObjects with marker pagination
│       ├── download.go  one object, streamed and hashed in a single pass
│       ├── klines.go    data-api.binance.vision/api/v3/klines
│       └── limiter.go   x/time/rate, sized to that endpoint's quota
│
├── cmd/bmd/          the CLI binary
│   ├── main.go       command dispatch, exit statuses, signal handling
│   ├── flags.go      dates → time.Time, shared flags → loader options
│   ├── download.go   flags → Request, Stream → an encoder
│   ├── list.go       Loader.Available, rendered
│   ├── verify.go     Loader.VerifyCache, rendered
│   ├── output.go     csv/json/parquet encoders; destinations; atomic writes
│   └── progress.go   the one-line terminal display
├── testdata/         real archives, byte-untouched (see its README)
└── docs/
```

The public API sits in the **root package**, so consumers write
`binancedata.NewLoader(...)` with no sub-package imports.

### Why `codec`, `cache` and `restapi` are not under `internal/`

The layout originally drawn for this project put all five of those directories
under `internal/`. It cannot compile. Go forbids import cycles at package
granularity, and the split as drawn requires one:

```
root ──────► internal/plan        loader.go must call the planner
  ▲                │
  └────────────────┘              the planner needs binancedata.Interval
```

Three ways out were weighed, and the cost of each was measured rather than
argued:

1. **Move the domain types to `internal/core` and re-export them from the root
   as type aliases.** The textbook fix, and the one this project rejected. A
   `type Kline = core.Kline` renders on pkg.go.dev as exactly that one line:
   no fields, no methods, no `Equal`, because the documentation tool will not
   describe an internal package. Confirmed with `go doc` on a scratch module.
   Documentation is a deliverable here, so losing every public type's body is
   not a trade worth making.
2. **Have internal packages exchange their own row types.** `internal/codec`
   returning `[]codec.Row` means copying every candle at the package boundary —
   millions of struct copies per request, to buy nothing.
3. **Split on whether a package needs domain types at all.** Adopted.

The rule is one question asked per package: *can this package do its job using
only standard-library types?*

| Package | Needs `Kline` / `Interval`? | Where it lives |
| --- | --- | --- |
| `plan` | No — [`plan.Spec`](../internal/plan/plan.go) carries two booleans instead | `internal/plan` |
| `vision` | No — URL strings in, keys and bytes out | `internal/vision` |
| `codec` | Yes; its entire output is `[]Kline` | root, unexported |
| `cache` | Yes; it returns `[]Kline` and writes them column by column | root, unexported |
| `restapi` | Yes; it returns `[]Kline` | root, unexported |

Unexported identifiers in the root package hide just as thoroughly from
consumers as `internal/` does, so the public API is identical either way. What
the split buys is narrower and worth naming precisely: **`internal/plan` imports
only `errors`, `fmt` and `time`, so it cannot perform I/O.** The claim "all
calendar logic is pure and testable with no network" stops being a convention
maintained by code review and becomes something the compiler refuses to let
anyone break.

What it costs is that the rule is invisible from a directory listing — hence
this section — and that `plan.Spec` duplicates two facts that `Interval` already
knows. The decision is also cheap to reverse: collapsing `internal/plan` into
the root is a mechanical rename, not a redesign.

## Public API

The domain types exist as of Stage 1, `Request` as of Stage 2, and `Loader`
with its options as of Stage 7. This is the whole surface.

```go
type Request struct {
    Symbol   string    // "BTC/USDT", "BTC-USDT" or "BTCUSDT" — all normalised
    Interval Interval
    Market   Market    // required; MarketSpot is the only implemented value
    Start    time.Time // inclusive; must be UTC
    End      time.Time // inclusive; must be UTC; zero means "now, at call time"
}

func (r Request) Validate() error

func NewLoader(opts ...Option) (*Loader, error)

func WithCacheDir(dir string) Option
func WithConcurrency(n int) Option
func WithHTTPClient(c *http.Client) Option
func WithProgress(fn func(Progress)) Option
func WithLogger(l *slog.Logger) Option

func (l *Loader) Fetch(ctx context.Context, req Request) ([]Kline, error)
func (l *Loader) FetchAll(ctx context.Context, reqs []Request) (map[Request][]Kline, error)
func (l *Loader) Stream(ctx context.Context, req Request) iter.Seq2[Kline, error]

// Added in Stage 8, because the CLI could not otherwise be a thin shell.
func (l *Loader) Available(ctx context.Context, q AvailabilityQuery) (Availability, error)
func (l *Loader) VerifyCache(ctx context.Context) iter.Seq2[CacheEntry, error]
func WriteParquet(ctx context.Context, w io.Writer, seq iter.Seq2[Kline, error]) (int, error)

type AvailabilityQuery struct {
    Symbol   string
    Interval Interval
    Market   Market
    Since    time.Time // optional; bounds the answer and its cost
}

type Availability struct {
    Symbol          string
    Interval        Interval
    Market          Market
    Monthly, Daily  []time.Time // start instants, ascending
    ArchivesThrough time.Time   // first instant no archive covers
}

func (a Availability) MonthlyGaps() []time.Time
func (a Availability) DailyGaps() []time.Time

type CacheEntry struct {
    Path    string // the archive on disk, absolute
    Sidecar string // its .CHECKSUM file, absolute; always set
    Size    int64
    Err     error  // nil if it hashes to what its sidecar says
}

type Progress struct {
    Request     Request   // the request this work belongs to, resolved
    Source      Source    // monthly archive, daily archive or REST
    Start, End  time.Time // the chunk's own range
    Klines      int       // candles it produced, before merging and trimming
    Total, Done int       // chunks planned, chunks finished
    Err         error
}
```

### `WithoutChecksumVerify` was dropped rather than built

Earlier drafts of this API listed it. It cannot be honoured, and the reason is
structural rather than a matter of taste. The `.CHECKSUM` sidecar's hash is not
only a safety check — it is half of the Parquet stamp, so it is what tells a
cached derived file whether it still matches the archive it was built from. An
option that skipped it could only be a no-op, or could only work by disabling
tier 2 and paying the 62 ms CSV parse on every read instead of 6 ms.

It also saves nothing. The sidecar is 91 bytes and is fetched first regardless,
and the SHA-256 is computed by an `io.MultiWriter` on a copy that has to happen
anyway. `ensureArchive` never re-hashes an archive already on disk, so there is
no expensive re-check to turn off either. Verification stays non-optional, which
is what requirement 9 and `docs/caching.md` already assumed.

### The range is closed, and the seams are not

**A `Request` is closed**: both `Start` and `End` are included, so a candle is
returned when `Start <= OpenTime <= End`. `End` reads most naturally as *the
open time of the last candle you want*. A full year of 2024 is `Start`
2024-01-01 and `End` 2024-12-31T23:59:59.999999999Z.

This was not the original design, and the reason it changed is worth recording
because the original reasoning was not wrong. Half-open ranges are what let the
pieces a range is cut into rejoin without arithmetic — `[Jan, Feb) + [Feb, Mar)`
is `[Jan, Mar)`, with each boundary written exactly once. Inclusive ends need a
"+1 of something" at every seam, and the something changes at the 2025
millisecond→microsecond switch; every one of those is a chance to drop or
duplicate a candle, silently.

All of that is still true, and **all of it still applies below the public API**.
Every internal boundary — `plan.Chunk`, `decodeSpec`, `vision.KlineQuery`,
`Progress.Start`/`End` — is half-open and unchanged. What moved is only where
the conversion happens: once, in `Request.endExclusive`, and the something it
adds is **one nanosecond**.

A nanosecond is the right step for the same reason the "+1 of something"
objection was right about milliseconds. It is a unit Binance has never published
in — archives carry milliseconds, and microseconds since 2025 — so nothing can
fall strictly between `End` and `End+1ns`. The conversion is exact rather than
approximately right, and it is the same single line on both sides of the 2025
switch.

**What it costs.** Writing `End` 2025-01-01 for "all of 2024" is no longer an
error and is not reported as one: it asks for the candle opening at midnight on
New Year's Day, so January's archive is fetched to get it and one extra candle
arrives at the end of the slice. That tax is charged to whoever writes the
boundary. It is accepted because the alternative was a CLI whose `--end` meant
something different from the library's `End`, which is the kind of difference
nobody notices until a backtest is a day short. `bmd` expands a bare `--end`
date to that day's last instant, which lands the exclusive bound exactly on the
seam.

`Start == End` is legal and asks for a single candle. Under the half-open rule
that spelling was empty by definition and `Validate` rejected it.

A zero `End` means "now, resolved when the request is executed". Prefer it to
writing `time.Now()`: a stored end date is a snapshot that ages, and the whole
point of the zero value is that there is nothing stored to go stale.

`FetchAll` is one call rather than a two-phase register-then-start API. The
only reason to split those phases is to deduplicate work across requests, and
`singleflight` provides that for free — so the API keeps the capability without
the stateful step.

`Stream` exists because a `Kline` measures 312 bytes (measured in Stage 1: two
24-byte `time.Time`, eight 32-byte `udecimal.Decimal`, one `int64`, no padding),
so five years of one-minute candles is roughly 820 MB held at once. A backtest
can consume candles without materialising the range.

## The command line

`bmd` is three commands over the library, and the interesting part of building
it was discovering that two of them had nothing to call.

### The CLI needed public API before it could be a thin shell

`docs/cli.md` has always promised that the tool holds no logic of its own and
that anything it can do can be done from Go code. That was not achievable as the
API stood. `cmd/bmd` is a separate package, so it sees only exported
identifiers, and:

- `bmd verify` needs the sidecar parser and the cache tree layout — `readSidecar`
  and `cachePaths`, both unexported.
- `bmd list` needs the bucket index — `fetchArchiveIndex` and `archiveIndex`,
  both unexported.
- `--format parquet` needs the schema — `writeKlines`, `parquetRow`,
  `encodeDecimal`, all unexported, and `writeKlines` refuses to write without a
  `parquetStamp` naming a source archive that an export does not have.

The alternative was to reimplement each of those inside the CLI, which would
have put the parquet schema in the repository twice and given the tool three
capabilities its own library could not offer. So Stage 8 added three small
public APIs instead, listed above. One thing worth noting for future work: the
Stage 2 rule that a package stays under `internal/` only if it needs no domain
types governs what the **root** imports. A package imported only by `cmd/` may
import the root package freely — there is no cycle — so an `internal/output`
speaking `[]Kline` would have been legal. It was not needed once the three
functions existed.

### The CLI cannot test the pipeline, and should not

The options that aim a `Loader` at an `httptest.Server` — `withTestHosts`,
`withClock`, `withPolicy`, `withLimiter` — are unexported, deliberately: they
are a test seam, not API. A test in `package main` therefore cannot build a
Loader that talks to a fake Binance.

That is the right constraint rather than a problem to route around. The pipeline
is already covered end to end in `loader_test.go`, against three fake hosts and
real archives. What is left for the CLI is everything between a command line and
that pipeline, and that is what its tests cover: `newLoader` is a package
variable holding a factory, the commands talk to a three-method `loader`
interface declared beside them, and the tests supply their own implementation.
The encoders are pinned with golden files, because the thing under test is an
exact document — a test asserting "the output contains open_time" would pass on
output with the columns in the wrong order or a price rounded through a float64.

### Decisions worth recording

**`-end` is inclusive, and a bare date covers the whole day.** This is the
reason `Request.End` became inclusive; the reasoning is in the closed-range
section above. The expansion is what makes the two agree at every interval, and
it also puts the library's exclusive bound exactly on a chunk seam, so the plan
is identical to the one the old half-open spelling produced.

**Output is written through a temporary file and renamed.** A download
interrupted halfway otherwise leaves a CSV that is silently short. The stakes
differ by format and neither is acceptable: a truncated CSV looks complete, and
a parquet file whose footer was never written is not a parquet file. A second
run replaces the file rather than refusing, because running a download twice is
the documented way to check the cache and must produce byte-identical output.

**The clock is read in `buildRequest`.** It is the only place in the project
that reads one inside logic, and the layer where that is correct: everything
below takes its time as a parameter precisely so the reading happens once, at
the edge. `Request` documents a preference for leaving `End` zero, which is
right for a long-running process that stores a request and wrong for a CLI —
resolving it here is what makes the generated file name, the summary line and
the candles describe the same instant.

**`bmd verify` walks tier 1 only.** Tier 2 is checked on every read against the
archive's hash and the codec version, and rebuilt when either fails, so it is
verified continuously by the code that uses it. Tier 1 is the only tier nothing
re-reads, which is what makes it the one worth a command.

## Documentation and release

Stage 9 found most of its own deliverables already built — `doc.go`, the README
and the five documents in `docs/` were written in Stage 0 and kept current by
every stage since, and `revive`'s `exported` rule has meant since Stage 0 that
no exported identifier can ship without a doc comment. What was left was the
examples, one deferred API change, and the release machinery.

### Examples are compiled; only some of them run

`example_test.go` holds sixteen, in `package binancedata_test` so that they can
reach only exported identifiers — worth having the compiler check in a
repository where half the code is under `internal/`.

They split in two, and the split is forced rather than chosen. `go test` runs an
example only when it ends in an `// Output:` comment, and an example that calls
`Fetch` could only produce output by fetching from Binance, which no test here
does. So the seven examples over pure logic — `ParseInterval`,
`NormalizeSymbol`, `Interval.HasDailyArchives`, `Interval.Duration`,
`Request.Validate`, `Availability.MonthlyGaps`, `Closes` — carry an `// Output:`
block and are assertions. The nine that hold a `Loader` do not.

Pointing the second group at an `httptest.Server` would make them executable —
`WithHTTPClient` is public, so a fake transport is reachable from the external
test package — and it is deliberately not done. Example source is what a reader
sees on the documentation page, and a screenful of test-server plumbing teaches
nothing about calling `Fetch`. The coverage is already bought by
`loader_test.go`, which stands up three fake hosts and counts what each is
asked for.

Compile-checking is stronger than it sounds: `go vet` binds every example name
to a real identifier. Renaming `ExampleLoader_Stream` to `ExampleLoader_Streem`
fails with *"refers to unknown field or method: Loader.Streem"*, so a renamed
method breaks the build rather than the documentation.

### `Option` became an interface

Deferred from Stage 7 on the grounds that documentation polish was Stage 9's
remit, and done here because a tag makes the representation permanent.

`type Option func(*loaderConfig) error` published a signature naming a type no
other package can see, so the generated documentation rendered the declaration
as an instruction the reader cannot follow — the private config leaking out
through the one place it was meant to stay behind. It is now an interface with a
single unexported `apply` method and an unexported `optionFunc` adapter, which
renders as a named type with its contents filtered out. Callers are unaffected:
every `With*` signature is unchanged, and there was never a way to build an
`Option` by hand.

This is the same question Stage 2 answered when it rejected the `internal/core`
alias: an identifier the documentation names but the reader cannot reach is
worse than one it never mentions.

### The version comes from the tag, and nothing else

`version.go` has always read `debug.ReadBuildInfo` rather than a constant, and
Stage 9 measured what that actually yields, because it decides whether the
release pipeline has to inject anything:

| Build | `Version()` |
| --- | --- |
| `go run ./cmd/bmd` | `(devel)` |
| `go build -buildvcs=false` | `(devel)` |
| `go build`, untagged commit | `v0.0.0-20260821114356-2970247f722f` |
| `go build`, clean tree at a tag | the tag |
| `go build`, dirty tree at a tag | the tag, plus `+dirty` |

Since Go 1.24 the toolchain reads version control and stamps what it finds, so
a checkout at `v0.1.0` produces a binary reporting `v0.1.0` with no `-ldflags`
involved. `.goreleaser.yaml` therefore overrides goreleaser's default ldflags,
which would otherwise inject `-X main.version` and create a second source of
truth for the same number. The `+dirty` suffix is a bonus: a binary built from
uncommitted changes says so instead of impersonating the release.

### goreleaser is configured but not wired to CI

`.goreleaser.yaml` cross-compiles `bmd` for darwin and linux on amd64 and arm64
— matching the CI matrix rather than reaching wider, since an artefact for a
platform nothing tests is a promise made without evidence — archives each with
the licence and `docs/cli.md`, and writes one `checksums.txt`.

There is no release workflow. The repository is private and its audience
installs with `go install`, so prebuilt binaries are not needed yet. What is
needed is a release config that has been *run* before the day it matters, which
is what `mise run release:snapshot` is for: it builds all four platforms and
publishes nothing. A release config first executed on release day is the one
piece of a project guaranteed to be untested when it counts.

## Scope

**Spot klines only**, with three deliberate extension points so futures is an
increment rather than a rewrite:

1. **The `Market` type.** URL building is a `switch` over `Market`, so USD-M
   (`/data/futures/um/`) and COIN-M (`/data/futures/cm/`) become new cases.
2. **Header sniffing.** Futures CSVs carry a header row; spot's do not. The
   parser sniffs instead of hardcoding — worth doing for spot alone, since a
   hardcoded answer silently eats or emits a row the day the format shifts.
3. **Per-row timestamp-unit detection.** Spot moved to microseconds on
   2025-01-01; futures stayed in milliseconds. Detecting per row covers both
   and fixes a real spot bug.

Other data types — aggTrades, trades, bookTicker, fundingRate — are out of
scope. Each has a distinct CSV schema and belongs in its own later stage.

## Error model

Live as of Stage 1, in `errors.go`.

```go
var (
    ErrNotAvailable   = errors.New("data not available")   // a 404: a fact, not a failure
    ErrChecksum       = errors.New("checksum mismatch")
    ErrCorruptArchive = errors.New("corrupt archive")
    ErrInvalidRequest = errors.New("invalid request")
    ErrRateLimited    = errors.New("rate limited")
    ErrIPBanned       = errors.New("ip banned")            // added in Stage 6
)
```

Callers test with `errors.Is` / `errors.As`. `ErrNotAvailable` is the one worth
highlighting: a missing archive is a fact about the calendar, not a failure, and
making it a typed error means the compiler forces every caller to acknowledge
it — where an empty-result-and-no-error convention relies on them remembering.

`ErrIPBanned` is the sixth, and the bar it had to clear is the one
`ErrRateLimited` already states: a sentinel earns its place when the right
response is different *in kind*. A throttle means wait; a ban means stop, since
no backoff rides out two minutes to three days and retrying earns the next,
longer one. Every error carrying it also carries `ErrRateLimited`, so nothing a
caller already wrote has to change. The distinction existed inside
`internal/vision` before Stage 6's review and could not be reached from outside
the module at all — a comment promising `errors.Is(err, vision.ErrIPBanned)` to
consumers who cannot import `internal/`.

Two conditions deliberately have **no** sentinel. A 5xx from the REST endpoint
reaches the caller unrecognised, because "Binance's side failed" is a state this
vocabulary does not describe and mislabelling it as either the caller's bug or a
gap in the calendar is worse than leaving it plain. Exceeding the REST page cap
is likewise untyped: it is a resource bound rather than a diagnosis, and the
caller's move is to split the range, not to give up on it.

## Correctness requirements

Each of these is a rule the implementation must satisfy, and each becomes a
regression test in the stage that satisfies it. They are listed together
because they are easy to get subtly wrong and silent when they are.

| # | Requirement | Stage |
| --- | --- | --- |
| 1 | The end of a request range is resolved **per call**, never captured once at construction — a long-running process must not drift onto a stale end date | 2 ✅ |
| 2 | Month-boundary comparisons anchor on the 1st of the month. A range such as `2024-12-20` → `2025-01-03` must not be misclassified as "current month", which **silently drops the last days** | 2 ✅ |
| 3 | Every day in the requested range is accounted for by exactly one chunk. Whatever the monthly/daily/REST split, no path may leave a **silent gap** at the tail | 2 ✅ |
| 4 | A 404 on one daily archive degrades that day only; the rest of the month still returns. A 404 from the *REST* endpoint is not this: it answers an empty range with 200, so a 404 there is a misconfiguration and stays untyped | 4 (typed error) → 6 (the REST distinction) → 7 ✅ (policy) |
| 5 | Deduplication registers the in-flight key **before** any concurrency limit is acquired, so saturation cannot let two workers fetch the same chunk | 5 ✅ (the cache owns the key) → 7 ✅ (the pool sits outside it) |
| 6 | Header presence is **sniffed per file**, never hardcoded per market | 3 ✅ |
| 7 | The timestamp unit is detected **per row**, so a file spanning the 2025 ms→µs switch parses correctly | 3 ✅ |
| 8 | One `http.Client` is shared for the process, so connections are reused rather than reopened per request | 4 ✅ |
| 9 | Checksums are **verified**, not merely stored — at download time, and on demand via `bmd verify` | 4 ✅ (download) → 8 ✅ (`bmd verify`) |
| 10 | Cache writes are atomic: temp file in the destination directory, then `rename`. A crash never leaves a torn file | 5 ✅ |

Two rules that apply everywhere rather than to one stage: validation lives in a
constructor that returns an `error`, so it cannot silently fail to run; and
every option a constructor accepts must be honoured — an accepted-and-ignored
setting is a defect, not a stub.

## Stages

| # | Stage | Status |
| --- | --- | --- |
| 0 | Scaffolding and tooling — mise, go.mod, linting, CI, docs skeleton | **done** |
| 1 | Domain types — `Interval`, `Market`, `DataType`, symbol normalisation, `Kline`, `errors.go` | **done** |
| 2 | Time and availability — month/day expansion, availability probing, UTC validation, S3 listing client | **done** |
| 3 | Parsing — zip → csv → `[]Kline`; header sniffing, per-row ms/µs detection, `CodecVersion` | **done** |
| 4 | Downloader — shared `http.Client`, retry with backoff, SHA-256 verification, typed 404 | **done** |
| 5 | Two-tier cache — ZIP + `.CHECKSUM` with atomic writes; footer-stamped Parquet; `singleflight` | **done** |
| 6 | REST fetcher — pagination for the recent tail, rate limiting | **done** |
| 7 | Loader orchestration — plan/execute/reduce, bounded pool, progress, `Fetch`/`FetchAll`/`Stream` | **done** |
| 8 | CLI — `cmd/bmd`, csv/json/parquet output, progress, `bmd verify`, `bmd list` | **done** |
| 9 | Docs and release — runnable examples, `Option` as an interface, goreleaser | **done** |

## Dependencies

| Dependency | Purpose | Stage |
| --- | --- | --- |
| `github.com/quagmt/udecimal` | Exact prices and volumes | 1 |
| `golang.org/x/sync` | `errgroup` (the two availability listings in parallel; in Stage 7 the loader's error collection and cancellation, with the concurrency bound itself a semaphore so it can span calls), `singleflight` (dedup) | 4 |
| `github.com/parquet-go/parquet-go` | Tier-2 cache and CLI export; pure Go. Raised the module's floor from Go 1.24.0 to 1.24.9, which it declares from v0.26.0 onward — accepted deliberately as a patch-level bump; see `go.mod` | 5 |
| `golang.org/x/time` | `rate.Limiter`, the token bucket pacing the REST endpoint | 6 |

The module's floor is **Go 1.25.0**, and it has moved twice. parquet-go pushed
it from 1.24.0 to 1.24.9 in Stage 5, because a module's floor cannot sit below
any of its dependencies'. Stage 6 moved it to 1.25.0 deliberately, for
`testing/synctest` — see the rate-limiting section above for what that bought.
CI has a job pinned to the floor so that a further move has to be a decision
rather than an accident.

Everything else is standard library: `net/http`, `archive/zip`, `encoding/csv`,
`crypto/sha256`, `log/slog`, `flag`, `iter`, `testing`.

The CLI uses stdlib `flag` rather than cobra — the tool is essentially one
command, and a small module graph matters for a published library. DuckDB was
ruled out because its Go driver requires cgo, which would forfeit static linking
and cross-compilation.

## Testing

- **Table-driven tests** throughout.
- **`httptest.Server`** for every network path, so the suite runs offline and
  deterministically. No test touches Binance.
- **Committed fixtures** in `testdata/`: five real archives, stored byte for byte
  as Binance served them, with the `.CHECKSUM` sidecars they were published
  with. They are deliberately *not* trimmed — re-zipping would invalidate those
  checksums, and Stage 4 onward verifies against the genuine values. Rows no
  real archive contains, such as a file that changes timestamp unit halfway
  through, are synthesised in the test itself.
- **`t.TempDir()`** for all cache tests.
- **Golden files** for CLI output and for reproducible Parquet.
- **Runnable examples** double as tests. Seven of the sixteen in
  `example_test.go` execute with their output checked; all sixteen are compiled,
  and `go vet` binds each example's name to a real identifier, so a rename
  breaks the build. See "Documentation and release" above for why the other
  nine cannot run.
- **`-race`** on every run.
- **A frozen clock** — time is injected, never read from `time.Now()` inside
  logic, so calendar rules are testable. The retry policy carries its timer and
  its clock as fields for the same reason: the suite asserts on the delays that
  were *requested* rather than sleeping through 3.5 s of backoff per test.
- **Connections are counted, not just requests.** `httptest.Server`'s
  `ConnState` hook is what makes "the body was drained so the connection was
  reused" a testable claim rather than a hopeful comment.
- **The cache is tested by breaking it.** Each claim it makes is checked by
  making the claim's opposite impossible to satisfy: the archive is replaced
  with garbage to prove a hit never reads it, the Parquet is overwritten with a
  stale stamp, a newer codec version and then rubbish to prove each rebuild
  trigger fires, and the archive is deleted to prove a pruned cache still
  serves. Every one of them also asserts the request count did not move.
- **Concurrency is arranged, not hoped for.** The deduplication test blocks the
  first request inside the server until the other seven callers have queued
  behind it, because a test that merely starts goroutines and counts requests
  passes just as happily when the deduplication is missing.
- **The loader is tested against three fake hosts at once.** `loader_test.go`
  stands up an `httptest.Server` for each of the listing, archive and REST
  endpoints, and counts what each was asked for. That is what makes "the plan
  avoided a request" a testable claim: a test that only checks the candles came
  back passes just as happily when the pipeline made sixty-two pointless round
  trips getting them.
- **Grid functions are checked against each other, not against a list.**
  `aligned` tests whether an instant sits on a candle grid and `alignUp` computes
  the next one that does, and two functions disagreeing about where the grid
  lines fall would be silent. The test states the relationship — the answer is on
  the grid, is not before the input, and skips no grid point in between — and
  sweeps it across all sixteen intervals.
