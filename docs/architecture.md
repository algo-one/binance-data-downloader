# Architecture

> **Status:** design document. Stages 0 (scaffolding) and 1 (domain types) are
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

Two findings from probing it directly on 2026-08-18 show why no heuristic would
have worked:

- **The archives have holes.** `BTCUSDT-1mo-2024-03.zip` does not exist, while
  `2024-02` and `2024-04` both do. No date arithmetic predicts a missing month
  in the middle of a published range; only the listing reveals it.
- **The interval availability tables in the Python source are wrong.** They
  declare `1s` daily-only. Binance publishes `1s` monthly archives too —
  `BTCUSDT-1s-2024-03.zip` is 93 MB of real data. See `interval.go`.

## The pipeline

```
Request ──► PLAN ──────► []chunk ──► EXECUTE ──────► []Kline ──► REDUCE ──► []Kline
            (pure)                   (bounded pool)              (merge,
            no I/O                   + singleflight               filter)
```

**Plan** *(planned: `internal/plan`)* — pure functions, no I/O. Expands a
`Request` into chunks, each one of `monthlyArchive`, `dailyArchive` or
`restRange`. All calendar logic lives here, so it is unit-testable against a
frozen clock with no network at all.

**Execute** — one bounded worker pool (`errgroup.SetLimit`) over a flat list of
chunks. `singleflight` collapses duplicate chunks across overlapping requests.
Context cancellation reaches every goroutine.

The flatness is the point. Nested limits — one for months, another for the days
inside a month — deadlock or starve as soon as an outer unit holds a permit
while merely *waiting* on its constituent inner units: a task occupying a slot
while doing nothing. One queue of uniform work units cannot have that problem.

**Reduce** — chunks arrive already sorted; merge, deduplicate on `open_time`,
and trim to the requested range.

## Package layout

```
.
├── doc.go            package documentation (the pkg.go.dev landing page)
├── version.go        module version, read from the embedded build info
├── interval.go       Interval + which intervals exist at which aggregation   (planned)
├── market.go         Market, DataType                                        (planned)
├── symbol.go         symbol normalisation across three formats               (planned)
├── kline.go          the Kline struct and column helpers                     (planned)
├── errors.go         sentinel errors                                         (planned)
├── options.go        functional options for NewLoader                        (planned)
├── loader.go         Fetch / FetchAll / Stream                               (planned)
│
├── internal/
│   ├── plan/         request → []chunk (pure, no I/O)                        (planned)
│   ├── vision/       data.binance.vision downloader + S3 listing             (planned)
│   ├── restapi/      data-api.binance.vision klines                          (planned)
│   ├── codec/        zip → csv → []Kline; CodecVersion lives here            (planned)
│   └── cache/        tier 1 (zip) + tier 2 (parquet) + memory                (planned)
│
├── cmd/bmd/          the CLI binary
├── testdata/         committed fixture archives                              (planned)
└── docs/
```

The public API sits in the **root package**, so consumers write
`binancedata.NewLoader(...)` with no sub-package imports. Everything else lives
under `internal/`, which the Go compiler forbids any other module from
importing. That is a real, enforced API boundary — the internals can be
restructured freely between stages without breaking a single consumer.

## Public API

The domain types below exist as of Stage 1. `Request`, `Loader` and the options
are still *planned*.

```go
type Request struct {
    Symbol   string    // "BTC/USDT", "BTC-USDT" or "BTCUSDT" — all normalised
    Interval Interval
    Market   Market    // required; MarketSpot is the only implemented value
    Start    time.Time // must be UTC
    End      time.Time // must be UTC; defaults to now at call time
}

func NewLoader(opts ...Option) (*Loader, error)

func (l *Loader) Fetch(ctx context.Context, req Request) ([]Kline, error)
func (l *Loader) FetchAll(ctx context.Context, reqs []Request) (map[Request][]Kline, error)
func (l *Loader) Stream(ctx context.Context, req Request) iter.Seq2[Kline, error]
```

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
| 1 | The end of a request range is resolved **per call**, never captured once at construction — a long-running process must not drift onto a stale end date | 2 |
| 2 | Month-boundary comparisons anchor on the 1st of the month. A range such as `2024-12-20` → `2025-01-03` must not be misclassified as "current month", which **silently drops the last days** | 2 |
| 3 | Every day in the requested range is accounted for by exactly one chunk. Whatever the monthly/daily/REST split, no path may leave a **silent gap** at the tail | 2 |
| 4 | A 404 on one daily archive degrades that day only; the rest of the month still returns | 4 |
| 5 | Deduplication registers the in-flight key **before** any concurrency limit is acquired, so saturation cannot let two workers fetch the same chunk | 7 |
| 6 | Header presence is **sniffed per file**, never hardcoded per market | 3 |
| 7 | The timestamp unit is detected **per row**, so a file spanning the 2025 ms→µs switch parses correctly | 3 |
| 8 | One `http.Client` is shared for the process, so connections are reused rather than reopened per request | 4 |
| 9 | Checksums are **verified**, not merely stored — at download time, and on demand via `bmd verify` | 5 |
| 10 | Cache writes are atomic: temp file in the destination directory, then `rename`. A crash never leaves a torn file | 5 |

Two rules that apply everywhere rather than to one stage: validation lives in a
constructor that returns an `error`, so it cannot silently fail to run; and
every option a constructor accepts must be honoured — an accepted-and-ignored
setting is a defect, not a stub.

## Stages

| # | Stage | Status |
| --- | --- | --- |
| 0 | Scaffolding and tooling — mise, go.mod, linting, CI, docs skeleton | **done** |
| 1 | Domain types — `Interval`, `Market`, `DataType`, symbol normalisation, `Kline`, `errors.go` | **done** |
| 2 | Time and availability — month/day expansion, availability probing, UTC validation, S3 listing client | |
| 3 | Parsing — zip → csv → `[]Kline`; header sniffing, per-row ms/µs detection, `CodecVersion` | |
| 4 | Downloader — shared `http.Client`, retry with backoff, SHA-256 verification, typed 404 | |
| 5 | Two-tier cache — ZIP + `.CHECKSUM` with atomic writes; footer-stamped Parquet; `singleflight` | |
| 6 | REST fetcher — pagination for the recent tail, rate limiting | |
| 7 | Loader orchestration — plan/execute/reduce, bounded pool, progress, `Fetch`/`FetchAll`/`Stream` | |
| 8 | CLI — `cmd/bmd`, csv/json/parquet output, progress | |
| 9 | Docs and release — runnable examples, pkg.go.dev polish, v0.1.0 | |

## Dependencies

| Dependency | Purpose | Stage |
| --- | --- | --- |
| `github.com/quagmt/udecimal` | Exact prices and volumes | 1 |
| `golang.org/x/sync` | `errgroup` (bounded pool), `singleflight` (dedup) | 5 |
| `github.com/parquet-go/parquet-go` | Tier-2 cache and CLI export; pure Go | 5 |

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
- **Committed fixtures** in `testdata/`: one pre-2025 millisecond archive and one
  post-2025 microsecond archive, so both parser paths are covered by spot alone.
  Trimmed to a few hundred rows.
- **`t.TempDir()`** for all cache tests.
- **Golden files** for CLI output and for reproducible Parquet.
- **`-race`** on every run.
- **A frozen clock** — time is injected, never read from `time.Now()` inside
  logic, so calendar rules are testable.
