# Architecture

> **Status:** design document. Stages 0 (scaffolding), 1 (domain types),
> 2 (time and availability), 3 (parsing), 4 (downloader) and 5 (cache) are
> complete; the packages described below arrive stage by stage. Sections marked
> *planned* describe code that does not exist yet.

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
  in the middle of a published range; only the listing reveals it.
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
turns out to be missing (month → days → REST). All calendar logic lives here,
and the package imports only `errors`, `fmt` and `time` — so it is *incapable*
of I/O, not merely expected to avoid it.

The chunks are sorted, contiguous and cover the whole requested range, and
`Expand` verifies that itself on every call rather than trusting the arithmetic.
The check is one pass over a handful of chunks, and it converts the only failure
mode nobody would notice — a missing day in the middle of a range, returned with
no error — into a loud one.

**Execute** — one bounded worker pool (`errgroup.SetLimit`) over a flat list of
chunks. `singleflight` collapses duplicate chunks across overlapping requests.
Context cancellation reaches every goroutine.

The flatness is the point. Nested limits — one for months, another for the days
inside a month — deadlock or starve as soon as an outer unit holds a permit
while merely *waiting* on its constituent inner units: a task occupying a slot
while doing nothing. One queue of uniform work units cannot have that problem.

**Reduce** — chunks arrive already sorted; merge, deduplicate on `open_time`,
and trim to the requested range.

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
each page and advanced one millisecond — the endpoint's own resolution, and the
smallest step that cannot return the same candle twice. Since every candle opens
at or after the cursor that requested it, the cursor strictly increases, so the
loop cannot stand still.

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

A 418 is reported as `*vision.RateLimitError` with `Banned` set, and its
`Unwrap` returns **both** `ErrRateLimited` and `ErrIPBanned`. A caller asking
only "should I slow down?" gets a yes; one that can tell a ban from a throttle
can. `X-MBX-USED-WEIGHT-1M` is parsed and reported rather than acted on — when
it climbs while the local accounting says otherwise, something else on the
address is spending the quota, which is a diagnosis no local bookkeeping reaches.

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
├── options.go        functional options for NewLoader                        (planned)
├── loader.go         Fetch / FetchAll / Stream                               (planned)
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

The domain types exist as of Stage 1 and `Request` as of Stage 2. `Loader` and
the options are still *planned*.

```go
type Request struct {
    Symbol   string    // "BTC/USDT", "BTC-USDT" or "BTCUSDT" — all normalised
    Interval Interval
    Market   Market    // required; MarketSpot is the only implemented value
    Start    time.Time // inclusive; must be UTC
    End      time.Time // exclusive; must be UTC; zero means "now, at call time"
}

func (r Request) Validate() error

func NewLoader(opts ...Option) (*Loader, error)

func (l *Loader) Fetch(ctx context.Context, req Request) ([]Kline, error)
func (l *Loader) FetchAll(ctx context.Context, reqs []Request) (map[Request][]Kline, error)
func (l *Loader) Stream(ctx context.Context, req Request) iter.Seq2[Kline, error]
```

**Ranges are half-open**: `Start` is included, `End` is excluded, so a full year
of 2024 is `Start` 2024-01-01 and `End` 2025-01-01. This is what lets the pieces
a range is cut into rejoin without arithmetic — `[Jan, Feb) + [Feb, Mar)` is
`[Jan, Mar)`, with each boundary written exactly once. Inclusive ends need a
"+1 of something" at every seam, and the something changes at the 2025
millisecond→microsecond switch; every one of those is a chance to drop or
duplicate a candle, silently.

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
)
```

Callers test with `errors.Is` / `errors.As`. `ErrNotAvailable` is the one worth
highlighting: a missing archive is a fact about the calendar, not a failure, and
making it a typed error means the compiler forces every caller to acknowledge
it — where an empty-result-and-no-error convention relies on them remembering.

## Correctness requirements

Each of these is a rule the implementation must satisfy, and each becomes a
regression test in the stage that satisfies it. They are listed together
because they are easy to get subtly wrong and silent when they are.

| # | Requirement | Stage |
| --- | --- | --- |
| 1 | The end of a request range is resolved **per call**, never captured once at construction — a long-running process must not drift onto a stale end date | 2 ✅ |
| 2 | Month-boundary comparisons anchor on the 1st of the month. A range such as `2024-12-20` → `2025-01-03` must not be misclassified as "current month", which **silently drops the last days** | 2 ✅ |
| 3 | Every day in the requested range is accounted for by exactly one chunk. Whatever the monthly/daily/REST split, no path may leave a **silent gap** at the tail | 2 ✅ |
| 4 | A 404 on one daily archive degrades that day only; the rest of the month still returns | 4 (typed error) → 7 (policy) |
| 5 | Deduplication registers the in-flight key **before** any concurrency limit is acquired, so saturation cannot let two workers fetch the same chunk | 5 ✅ (the cache owns the key) → 7 (the pool sits outside it) |
| 6 | Header presence is **sniffed per file**, never hardcoded per market | 3 ✅ |
| 7 | The timestamp unit is detected **per row**, so a file spanning the 2025 ms→µs switch parses correctly | 3 ✅ |
| 8 | One `http.Client` is shared for the process, so connections are reused rather than reopened per request | 4 ✅ |
| 9 | Checksums are **verified**, not merely stored — at download time, and on demand via `bmd verify` | 4 ✅ (download) → 8 (`bmd verify`) |
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
| 7 | Loader orchestration — plan/execute/reduce, bounded pool, progress, `Fetch`/`FetchAll`/`Stream` | |
| 8 | CLI — `cmd/bmd`, csv/json/parquet output, progress | |
| 9 | Docs and release — runnable examples, pkg.go.dev polish, v0.1.0 | |

## Dependencies

| Dependency | Purpose | Stage |
| --- | --- | --- |
| `github.com/quagmt/udecimal` | Exact prices and volumes | 1 |
| `golang.org/x/sync` | `errgroup` (the two availability listings in parallel, then the Stage 7 bounded pool), `singleflight` (dedup) | 4 |
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
