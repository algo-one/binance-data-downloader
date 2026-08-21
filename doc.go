// Package binancedata downloads and caches historical Binance market data.
//
// It is both an embeddable Go library and the engine behind the bmd
// command-line tool. The design goal is that a backtest can ask for five years
// of candles and get them back quickly on every run, without re-downloading or
// re-parsing anything it has already seen.
//
// # Getting started
//
// Build a [Loader] once and share it — one per process. It is safe for
// concurrent use, and the concurrency limit, the connection pool and the REST
// rate limiter all live on it, so two Loaders each pacing themselves correctly
// would still exceed Binance's per-IP quota together.
//
//	loader, err := binancedata.NewLoader()
//	if err != nil {
//	    return err
//	}
//
//	klines, err := loader.Fetch(ctx, binancedata.Request{
//	    Symbol:   "BTC/USDT",
//	    Interval: binancedata.Interval1h,
//	    Market:   binancedata.MarketSpot,
//	    Start:    start,
//	    End:      end, // leave zero for "now, at call time"
//	})
//
// Ranges are closed: a candle is returned when Start <= OpenTime <= End, so a
// full year of 2024 is Start 2024-01-01 and End 2024-12-31T23:59:59.999999999Z.
// See [Request] for why the last instant is spelt out that way, and for what
// End 2025-01-01 would fetch instead.
//
// [Loader.Stream] yields the same candles one at a time for a range too large
// to hold at once, and [Loader.FetchAll] runs several requests under one
// concurrency budget, downloading whatever they have in common exactly once.
//
// Three more calls answer questions that are not "give me candles".
// [Loader.Available] reports what Binance actually publishes for a symbol and
// interval, holes included — the archives have them, and no calendar predicts
// which. [Loader.VerifyCache] re-hashes cached archives against the checksums
// they were published with. [WriteParquet] writes candles in the same format
// the cache stores its second tier in, for a query engine to read.
//
// # Examples
//
// example_test.go carries a worked example for most of the surface above, and
// they are the fastest way in. Seven of them execute on every test run with
// their printed output checked — the pure ones, which need no network — and the
// rest are compiled but not run, because running them would mean fetching from
// Binance and no test in this repository does that. Both kinds are checked by
// the compiler, so an example naming a field that no longer exists fails the
// build rather than misleading somebody.
//
// # Stability
//
// The version is v0.x. Everything documented here works and is tested, but the
// API carries no compatibility promise yet: a v0.2.0 may rename something a
// v0.1.0 caller used. Pin an exact version if that matters.
//
// # Where the data comes from
//
// Binance publishes bulk archives at https://data.binance.vision as monthly
// and daily ZIP files, each accompanied by a .CHECKSUM sidecar holding a
// SHA-256 of the archive. Archives lag real time by roughly a day, so the most
// recent candles are fetched instead from the REST mirror at
// https://data-api.binance.vision. This package hides that split: you ask for
// a time range, and it works out which sources cover it.
//
// # Caching
//
// Two tiers live under the cache directory:
//
//   - Tier 1 is the raw ZIP exactly as Binance served it, plus its .CHECKSUM.
//     It is the source of truth and is never re-downloaded once verified.
//   - Tier 2 is a Parquet file derived from tier 1, which is what reads
//     actually hit. Parsing a month of one-minute candles out of CSV costs
//     tens of milliseconds; reading the Parquet costs a fraction of that.
//
// Tier 2 records the SHA-256 of the tier-1 archive it was built from in its
// Parquet footer, so a cached Parquet can be trusted without re-hashing the
// ZIP. In the steady state nothing is ever rebuilt. See docs/caching.md.
//
// # A note on numbers
//
// Prices and volumes are [github.com/quagmt/udecimal.Decimal], not float64.
// Binance quote volumes reach 20 significant digits, which float64 (15.95
// digits) cannot represent exactly, and no int64 fixed-point scale covers the
// range either — real meme-coin volumes overflow int64 at 1e8 scaling. The
// archives are text, the values in them are exact, and this package preserves
// them digit for digit.
//
// Convert to float64 explicitly at the point where you need it, which for most
// callers is when feeding an indicator library.
//
// # Documentation
//
// The docs directory carries the longer-form material: architecture.md for how
// the pieces fit together, caching.md for the cache design and its invariants,
// numbers.md for why prices are decimals and what the alternatives measured,
// cli.md for the bmd tool, and go-notes.md, which explains the Go idioms this
// codebase leans on.
package binancedata
