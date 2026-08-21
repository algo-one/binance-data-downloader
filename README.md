# binance-data-downloader

Download and cache historical Binance market data — as a Go library and as a
command-line tool.

```go
// No options needed: the cache lands in the OS cache directory.
loader, err := binancedata.NewLoader()

klines, err := loader.Fetch(ctx, binancedata.Request{
    Symbol:   "BTC/USDT",
    Interval: binancedata.Interval1h,
    Market:   binancedata.MarketSpot,
    Start:    start,
    End:      end, // leave zero for "now, at call time"
})
```

Ranges are closed — a candle is returned when `Start <= OpenTime <= End` — so a
full year of 2024 is `Start` 2024-01-01 and `End` 2024-12-31T23:59:59.999999999Z.
`End` reads most naturally as *the open time of the last candle you want*.
Writing `End` 2025-01-01 is legal and asks for the candle that opens at midnight
on New Year's Day, which costs January's archive to fetch.

For a range too large to hold in memory, `loader.Stream(ctx, req)` yields the
same candles one at a time; `loader.FetchAll(ctx, reqs)` runs several requests
under one concurrency budget and deduplicates the archives they share.

## Command line

```bash
mise run build

bmd download -symbol BTC/USDT -interval 1h -start 2024-01-01 -end 2024-03-31
bmd list     -symbol BTC/USDT -interval 1mo      # what Binance actually publishes
bmd verify                                        # re-hash the cache
```

`-start` and `-end` are both inclusive, and a bare `-end` date covers that whole
day. Output is csv, json or parquet. See [docs/cli.md](docs/cli.md).

> **Status: under construction.** Stages 0–8 are complete: the library API above
> and the `bmd` tool both work today. Stage 9 is documentation, runnable
> examples and a v0.1.0 release — see
> [docs/architecture.md](docs/architecture.md) for the staged plan.

## Why

Binance publishes bulk historical klines as ZIP archives at
[data.binance.vision](https://data.binance.vision), each with a SHA-256
`.CHECKSUM` sidecar. The archives lag real time by about a day, so the newest
candles have to come from the REST mirror instead. Getting a clean, contiguous,
verified range of candles means stitching those two sources together, handling
the calendar rules for which archives exist, and caching aggressively enough
that a backtest can re-read five years of one-minute data on every run without
paying for it twice.

That is what this does.

## Design notes

**Exact numbers.** Prices and volumes are
[`udecimal.Decimal`](https://github.com/quagmt/udecimal), not `float64`.
Binance quote volumes reach 20 significant digits; `float64` carries 15.95, and
no `int64` fixed-point scale spans the range either — real PEPE daily volume
overflows `int64` at 1e8 scaling, silently and negatively. The archives are
text, the numbers in them are exact, and this library keeps them that way.

**Two-tier cache.** Tier 1 is the raw ZIP exactly as Binance served it, kept
alongside its `.CHECKSUM`. Tier 2 is a Parquet file derived from it, which is
what reads actually hit. Each Parquet records the SHA-256 of the archive it was
built from in its own footer, so a cached file can be trusted without re-hashing
anything. In the steady state nothing is ever rebuilt. See
[docs/caching.md](docs/caching.md).

**Embeddable first.** The library is designed to sit inside a backtesting
framework, so the CLI is a thin shell over it rather than the other way round.

## Install

As a library:

```bash
go get github.com/algo-one/binance-data-downloader
```

As a CLI:

```bash
go install github.com/algo-one/binance-data-downloader/cmd/bmd@latest
```

Requires Go 1.25.0 or newer.

## Development

Tooling is managed with [mise](https://mise.jdx.dev/), which pins the Go
version, the linter and the test runner so every machine and CI runner use
identical bits.

```bash
mise trust      # once, to allow this repo's mise.toml
mise install    # fetch the pinned toolchain
mise tasks      # list everything below
```

| Task | What it does |
| --- | --- |
| `mise run build` | Compile the `bmd` CLI into `./bin` |
| `mise run test` | Run all tests with the race detector |
| `mise run lint` | Run golangci-lint |
| `mise run fmt` | Format all Go code in place |
| `mise run fmt:check` | Fail if anything is unformatted (CI uses this) |
| `mise run cover` | Test with coverage and open the HTML report |
| `mise run tidy` | Sync `go.mod` with the imports in the code |
| `mise run ci` | Everything CI runs, in order |

## Documentation

| Document | Contents |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | How the pieces fit together, and the staged build plan |
| [docs/caching.md](docs/caching.md) | The two-tier cache, its invariants, and why it exists |
| [docs/cli.md](docs/cli.md) | The `bmd` command-line tool |
| [docs/go-notes.md](docs/go-notes.md) | The Go idioms this codebase leans on, in one place |

The code itself is commented far more heavily than typical Go. That is
deliberate: this repository doubles as a way to learn the language, so the
comments explain *why* a construct is used, not just what it does.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
