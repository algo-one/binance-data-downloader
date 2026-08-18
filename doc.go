// Package binancedata downloads and caches historical Binance market data.
//
// It is both an embeddable Go library and the engine behind the bmd
// command-line tool. The design goal is that a backtest can ask for five years
// of candles and get them back quickly on every run, without re-downloading or
// re-parsing anything it has already seen.
//
// # Status
//
// Under construction. This file currently describes the intended API; the
// types below arrive stage by stage. See docs/architecture.md for the plan.
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
