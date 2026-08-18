# Numbers

Why prices and volumes are `udecimal.Decimal` and not something cheaper.

> **Status:** re-verified 2026-08-18 against live archives, after Stage 1
> landed. The original decision was made from a smaller sample; the conclusion
> did not change, but two of the supporting numbers did. Measured on an Apple
> M1 Pro, Go 1.26.5, `darwin/arm64`.

## What the data actually requires

Measured over **1,751,352 real values** — every numeric field of every row in 44
archives from `data.binance.vision`, covering BTCUSDT, ETHUSDT, SOLUSDT,
DOGEUSDT, PEPEUSDT and SHIBUSDT at the `1m` and `1mo` intervals.

| Property | Measured |
| --- | --- |
| Max decimal places | **8** |
| Max coefficient digits, prices | 14 |
| Max coefficient digits, volumes | **20** |
| Worst single value | `118661604939.99255335` — BTCUSDT `1mo` quote volume |

Twenty digits is the number that decides everything below. It comes from long
intervals, not short ones: a `1m` candle never exceeded 17 digits, but a `1mo`
candle aggregates a whole month of a high-volume pair into one row.

## Exactness, on real values

Percentage of real archive values that a representation cannot hold exactly.

| Representation | `1m` candles | `1mo` candles | Combined |
| --- | --- | --- | --- |
| `float64` | 0.0001 % (2) | 19.2 % (60) | 0.0035 % (62) |
| `int64` @ 1e8 | 0.24 % (4,226) | 9.6 % (30) | 0.24 % (4,256) |
| `govalues/decimal` | 0 | 0.96 % (3) | 0.0002 % (3) |
| **`udecimal`** | **0** | **0** | **0** |

Three things worth reading off that table.

**`float64` looks almost fine and is not.** Two failures in 1.75 million `1m`
values is the kind of rate that survives casual testing and then quietly
corrupts a backtest that sums a year of quote volume. On monthly candles it
fails one value in five.

**`int64` at 1e8 fails on the common case, not the exotic one.** All 4,226
failures are overflow, and they are meme-coin volumes — SHIB and PEPE trade in
quantities that need more than 18.96 digits once scaled. No other scale rescues
it: lowering the scale loses the eight decimal places the data actually has.

**`govalues/decimal` fails silently, which is worse than failing loudly.** Its
coefficient is a `uint64`, so it holds 19 digits; `Parse` documents itself as
returning a *"possibly rounded"* decimal and returns no error when it rounds.
It is right up to 19 digits and wrong at 20, with nothing to signal the
difference. That is disqualifying here regardless of its other merits — and it
has real merits, see the memory table.

## Speed

Parsing is the hot path: Stage 3 converts eight decimal fields per CSV row, and
a month of `1m` candles is ~44,000 rows.

| | Parse 1 row (8 fields) | allocs | `Add` | `Cmp` | `String` | → `decimal128(38,8)` |
| --- | --- | --- | --- | --- | --- | --- |
| `float64` | 272 ns | 0 | — | — | — | — |
| **`udecimal`** | **140 ns** | **0** | **7.5 ns** | **7.8 ns** | 39 ns (1 alloc) | **2.0 ns (0 allocs)** |
| `govalues` | 287 ns | 0 | 7.9 ns | 7.9 ns | **21 ns (0 allocs)** | — |
| `shopspring` | 789 ns | 24 | 169 ns (8) | 130 ns (6) | 153 ns (4) | 143 ns (8 allocs) |
| `apd/v3` | 899 ns | 16 | — | — | — | — |

`udecimal` parses a row in **half the time `strconv.ParseFloat` takes** for the
same eight fields, which is the opposite of what you would guess. Its worst
measured real value, the 20-digit one above, parses in 25 ns and still allocates
nothing — the 128-bit coefficient covers it, so the `big.Int` fallback never
fires on Binance data.

The last column is the tier-2 cache write path. Parquet `decimal128(38,8)` wants
an unscaled 128-bit integer, and `udecimal.ToHiLo` hands one over for free;
`shopspring` has to rescale a `big.Int` and allocate eight times to do it.

## Memory

One symbol-year of `1m` candles — 525,600 `Kline` values — measured as live heap
after a forced GC.

| | Bytes/`Kline` | Heap | 5 years | Build | GC cycle | Heap objects |
| --- | --- | --- | --- | --- | --- | --- |
| `float64` | 120 | 60 MB | 315 MB | 145 ms | 762 µs | 15 |
| `govalues` | 184 | 92 MB | 484 MB | 174 ms | 1.1 ms | 15 |
| **`udecimal`** | **312** | **156 MB** | **820 MB** | **88 ms** | **1.7 ms** | **15** |
| `shopspring` | 536 | 269 MB | 1.41 GB | 414 ms | 19.8 ms | 7,358,416 |

Struct sizes, for reference: `float64` 8, `govalues` 16, `udecimal` 32,
`shopspring` 16 **plus a heap `big.Int`**, `apd` 32 plus heap,
`ericlagergren/decimal` 104 plus heap.

The `shopspring` row is the one to dwell on. Seven million heap objects for a
single symbol-year means the garbage collector must trace every one of them on
every cycle, and a GC cycle costs **19.8 ms against `udecimal`'s 1.7 ms** — a
12× penalty paid repeatedly for as long as the data is live. `udecimal` stores
its coefficient inline, so a `[]Kline` is exactly one heap object no matter how
many candles it holds.

`udecimal` costs 2.6× the memory of `float64` and 1.7× that of `govalues`. That
is the price of being correct, and it is why `Stream` exists as an alternative
to holding a whole range at once.

## Verdict

`udecimal` is the right choice, and it is not close. Among representations that
are **exact on real Binance data**, it is also the fastest to parse, the
smallest in memory, the only one that allocates nothing, and the only one that
converts to the Parquet storage format for free.

The one honest argument against it is maturity: `shopspring/decimal` is the
de-facto standard Go decimal library with vastly more production exposure, while
`udecimal` is younger and less widely deployed. That risk is mitigated here by
the narrowness of what this library asks of it — parse text, store, compare,
convert — all of which are covered by the 1.75-million-value round-trip above.
`shopspring` remains the fallback if `udecimal` were ever abandoned; the
`Kline` field type is the only thing that would change.

## Reproducing

The harness is not committed — it needs five decimal libraries this module does
not depend on, and it downloads real archives, which no test in this repository
is allowed to do. To rebuild it: create a scratch module, `go get` the
candidates, fetch a few `1m` and `1mo` archives from
`data.binance.vision/data/spot/monthly/klines/<SYMBOL>/<INTERVAL>/`, and
round-trip every field of every row back to its original string.

The `1mo` archives matter more than their row count suggests. Sampling only
short intervals is exactly how a 19-digit ceiling looks safe.
