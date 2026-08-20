# Test fixtures

Real archives from `data.binance.vision`, stored **exactly as Binance served
them**. Fetched 2026-08-18.

## Do not modify these files

Each `.zip` is accompanied by the `.CHECKSUM` sidecar Binance publishes beside
it, and every one of them currently verifies. That is the whole point: from
Stage 4 onward the download and cache paths are tested against genuine
SHA-256 values rather than ones this repository made up for itself.

Re-zipping a fixture — to trim rows, to fix a typo, for any reason at all —
changes its bytes and silently invalidates its checksum. If a new case is
needed, fetch another real archive or build a synthetic one in the test itself,
as `codec_test.go` does for the rows no real archive contains.

## What each one is for

| File | Rows | Timestamps | Why it is here |
| --- | --- | --- | --- |
| `BTCUSDT-1h-2024-01-15.zip` | 24 | milliseconds | An ordinary day in the millisecond era |
| `BTCUSDT-1h-2024-12-31.zip` | 24 | milliseconds | The **last** day Binance wrote milliseconds |
| `BTCUSDT-1h-2025-01-01.zip` | 24 | microseconds | The **first** day Binance wrote microseconds |
| `BTCUSDT-1mo-2024-01.zip` | 1 | milliseconds | One monthly candle; its quote volume is 19 significant digits, which is what rules out `float64` |
| `BTCUSDT-1m-2025-01-15.zip` | 1440 | microseconds | Bulk data, and the input to `BenchmarkDecodeArchiveAll` |

The two December/January files bracket the unit switch at 2025-01-01T00:00Z:
the last candle of one closes at `1735689599999` (milliseconds) and the first
candle of the next opens at `1735689600000000` (microseconds). They are
adjacent instants written in different units, which is exactly the seam the
decoder has to get right.

No archive spans the switch — it fell on a day *and* month boundary — so the
per-row detection rule is exercised by a synthetic mixed file in `codec_test.go`
and these fixtures prove each side of it.

## The REST capture

| File | Rows | Why it is here |
| --- | --- | --- |
| `BTCUSDT-1h-2024-01-15.klines.json` | 24 | The **same day** as `BTCUSDT-1h-2024-01-15.zip`, fetched from `data-api.binance.vision/api/v3/klines` on 2026-08-20 |

This one is not an archive and has no `.CHECKSUM`, because Binance publishes
none for an API response. It is here to make one claim testable that nothing
else could check: this library treats a zipped CSV from the bucket and a JSON
array from the API as interchangeable sources of the same candles, and that is
only true if they decode identically.

`TestRESTAgreesWithTheArchive` in `restapi_test.go` decodes both fixtures and
compares all 24 candles field by field. Both sides are real data, so the test
fails if the REST decoder, the CSV decoder, or Binance's own two representations
ever drift apart. It was confirmed to fail when a single digit in the last
decimal place of one price was altered.

The same "do not modify" rule applies. Editing it would turn a comparison of two
real sources into a comparison of one real source against something this
repository made up.
