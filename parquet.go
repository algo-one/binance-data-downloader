package binancedata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/bits"
	"slices"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/quagmt/udecimal"
)

// This file is tier 2 of the cache: the columnar form of an archive, and the
// file that reads actually hit. cache.go owns when it is built and whether it
// may be trusted; this file owns what is in it.
//
// # Why a second copy of data we already have
//
// Decoding a ZIP costs about 62 ms per month of one-minute candles — measured,
// not estimated, in Stage 3. A backtest re-reads the same five years on every
// run, so ten symbols is roughly 40 seconds of CSV parsing before the first
// trade is simulated, every time. Nothing about that work changes between runs,
// which is the definition of something worth caching.
//
// Parquet is the form that makes the re-read cheap. The values are already
// binary, so nothing is parsed from text; the columns are stored separately, so
// a caller that only wants OHLCV never touches the taker-volume columns; and
// snappy decompresses several times faster than deflate.
//
// # The three properties this file is built around
//
//  1. One fixed schema. Every decimal column is DECIMAL(38,8), for every symbol
//     and every market, rather than a precision inferred per file. Inference is
//     what lets two cached files disagree about the same column and pushes the
//     reconciliation onto every reader.
//  2. Byte-identical output. The same archive decoded by the same [CodecVersion]
//     produces the same file, so golden tests are trivial and two machines'
//     caches can be diffed. Nothing here writes a timestamp, and every knob the
//     writer has that could drift is pinned below.
//  3. A footer that says where the file came from. See [parquetStamp].

const (
	// parquetPrecision and parquetScale fix the decimal schema at
	// DECIMAL(38,8) — 38 significant digits, 8 of them after the point.
	//
	// Both numbers come from measurement rather than taste. Across 1,751,352
	// real values from 44 archives (docs/numbers.md) the worst case is 20
	// significant digits and exactly 8 decimal places, so the scale is what
	// Binance publishes and the precision leaves nearly double the headroom
	// observed. 38 is also the largest precision that fits in 16 bytes, which
	// is what makes this decimal128 — the width every other tool in this space
	// means by "decimal".
	parquetPrecision = 38
	parquetScale     = 8

	// parquetDecimalBytes is the width of one decimal column value.
	//
	// parquet-go derives it from the precision — ceil((log10(2)+38)/log10(256))
	// — and the answer for 38 is 16. It is spelled out here because the array
	// type in [parquetRow] has to be written as a literal size, and a size that
	// disagreed with the precision would be caught by parquet-go at schema
	// construction, which is to say at runtime rather than here.
	parquetDecimalBytes = 16

	// parquetRowGroupRows caps a row group.
	//
	// A row group is buffered in memory before it is flushed, so this bounds
	// the writer's footprint: 65,536 rows across ten columns is a few megabytes
	// regardless of how long the archive is. It also lands where it does for a
	// reason — a month of one-minute candles is 44,640 rows and therefore one
	// row group, while a month of one-second candles is 2.6 million rows and
	// splits into forty. Left unset, parquet-go writes a single row group of
	// any size, which for the 1s case means buffering the whole month.
	//
	// It is pinned rather than defaulted because reproducibility depends on it:
	// a library upgrade that changed the default would change every byte of
	// every file built afterwards.
	parquetRowGroupRows = 1 << 16

	// parquetBatchRows is how many rows cross the parquet-go boundary per call.
	// Both directions work in batches because a call per row would pay the
	// per-call cost 2.6 million times for a 1s month; 1024 is large enough that
	// the overhead disappears and small enough that the buffer stays in cache.
	parquetBatchRows = 1024
)

// The footer keys binding a tier-2 file to the tier-1 archive it came from.
//
// They are namespaced with a "bmd." prefix because the footer is a shared
// namespace: parquet writers, readers and query engines all put keys in it, and
// an unprefixed "rows" would be a collision waiting to happen.
const (
	// metaSourceSHA256 is the SHA-256 of the archive this file was built from,
	// which is Binance's own published checksum. It is the first half of the
	// validity rule in docs/caching.md.
	metaSourceSHA256 = "bmd.source.sha256"

	// metaSourceFile names that archive. It is not part of the validity rule —
	// the hash is — but it turns a mismatch into a legible error, and it makes
	// a stray parquet file identifiable with a tool that knows nothing about
	// this library.
	metaSourceFile = "bmd.source.file"

	// metaCodecVersion is the [CodecVersion] that produced the rows. It is the
	// second half of the validity rule, and the only half that can fire while
	// the source archive is byte-for-byte unchanged: fix a decoding bug and
	// every file built by the old rules is wrong with nothing about its source
	// to show for it.
	metaCodecVersion = "bmd.codec.version"

	// metaRows is the number of candles written. Parquet records its own row
	// count in the footer, so this is redundant by design — it is a check on
	// the file agreeing with itself, which is exactly the sort of thing a
	// truncated or half-rewritten file fails.
	metaRows = "bmd.rows"
)

// errCacheStale means a tier-2 file exists but may not be used: it was built
// from a different archive, or by a different [CodecVersion].
//
// It is unexported and never reaches a caller. The cache treats it, and every
// other failure to read a parquet file, as "rebuild from tier 1" — the
// distinction it draws is only for the error message in the rare case where the
// rebuild fails too, and the reader deserves to know whether the file that was
// discarded was stale or corrupt.
var errCacheStale = errors.New("cached parquet is stale")

// parquetRow is one candle as it is stored on disk.
//
// It is a separate type from [Kline] rather than tags on Kline itself, and that
// is deliberate: the storage format and the domain type should be free to move
// independently. Kline carries [time.Time] and [udecimal.Decimal] because that
// is what callers want to compute with; this carries int64 microseconds and
// raw 16-byte decimals because that is what the format stores. Putting parquet
// tags on Kline would make every future change to the on-disk layout a change
// to the public API.
//
// # The struct tags
//
// The text after the field name in each tag is parquet-go's schema language:
//
//	timestamp(microsecond)  INT64 with a TIMESTAMP(isAdjustedToUTC, MICROS)
//	                        logical type, so other tools read these as instants
//	                        rather than as opaque integers
//	decimal(8:38)           scale 8, precision 38 — DECIMAL(38,8) stored in a
//	                        FIXED_LEN_BYTE_ARRAY(16)
//
// Microseconds rather than milliseconds is a correctness requirement, not a
// margin: since 2025-01-01 Binance writes these files in microseconds, and a
// close time such as 23:59:59.999999 loses its last three digits if stored as
// milliseconds. The field order below matches [Kline] so that the two can be
// read side by side.
type parquetRow struct {
	OpenTime  int64 `parquet:"open_time,timestamp(microsecond)"`
	CloseTime int64 `parquet:"close_time,timestamp(microsecond)"`

	Open  [parquetDecimalBytes]byte `parquet:"open,decimal(8:38)"`
	High  [parquetDecimalBytes]byte `parquet:"high,decimal(8:38)"`
	Low   [parquetDecimalBytes]byte `parquet:"low,decimal(8:38)"`
	Close [parquetDecimalBytes]byte `parquet:"close,decimal(8:38)"`

	Volume      [parquetDecimalBytes]byte `parquet:"volume,decimal(8:38)"`
	QuoteVolume [parquetDecimalBytes]byte `parquet:"quote_volume,decimal(8:38)"`

	TakerBuyBaseVolume  [parquetDecimalBytes]byte `parquet:"taker_buy_base_volume,decimal(8:38)"`
	TakerBuyQuoteVolume [parquetDecimalBytes]byte `parquet:"taker_buy_quote_volume,decimal(8:38)"`

	Trades int64 `parquet:"trades"`
}

// parquetStamp is what a tier-2 file records about its own provenance.
//
// Rows is absent because it is not an input: it is counted while writing and
// checked while reading.
type parquetStamp struct {
	// SourceFile is the archive's file name, e.g. BTCUSDT-1h-2024-01.zip.
	SourceFile string

	// SourceSHA256 is that archive's hash in lowercase hex — the value from
	// Binance's .CHECKSUM sidecar, which the download path verified the bytes
	// against rather than copying on faith.
	SourceSHA256 string
}

// writeKlines writes every candle from seq into dst as a parquet file, and
// returns the candles it wrote.
//
// # Why it both writes and returns
//
// The caller building a cache entry is, by definition, a caller that wanted the
// candles: it is servicing a request and filling the cache on the way past.
// Returning them here means the archive is decoded once rather than decoded,
// written, and read back. The rows still stream — parquet-go receives them in
// batches of [parquetBatchRows] and never sees the whole slice — so the only
// copy held whole is the one being returned.
//
// sizeHint preallocates that slice; it is a hint and may be wrong in either
// direction, since archives are routinely shorter than their period.
//
// # What dst holds afterwards
//
// On success, a complete parquet file. On error, an unusable prefix of one:
// a parquet file's footer is written last, so an interrupted write produces
// bytes that no reader will accept. The cache never lets those bytes reach a
// final path — see writeAtomic in cache.go — which is what makes a torn file
// impossible rather than merely unlikely.
func writeKlines(
	ctx context.Context,
	dst io.Writer,
	seq iter.Seq2[Kline, error],
	stamp parquetStamp,
	sizeHint int,
) ([]Kline, error) {
	if stamp.SourceFile == "" || stamp.SourceSHA256 == "" {
		// A file whose footer cannot say where it came from is one the read
		// path would have to accept on trust. Refusing to write it is cheaper
		// than deciding later what to do with it.
		return nil, fmt.Errorf("parquet: writing %q: incomplete stamp: %w", stamp.SourceFile, ErrInvalidRequest)
	}

	w := parquet.NewGenericWriter[parquetRow](dst, parquetWriterOptions()...)

	out := make([]Kline, 0, sizeHint)

	if _, err := writeRows(ctx, w, seq, &out); err != nil {
		// writeRows returns its errors unwrapped so that each caller can name
		// its own destination. A decoder error arrives here already wrapping
		// ErrCorruptArchive and naming the row, and %w keeps that intact.
		// Note that nothing is flushed or renamed on this path, so a corrupt
		// archive leaves no cache entry behind.
		return nil, fmt.Errorf("parquet: writing %q: %w", stamp.SourceFile, err)
	}

	// The stamp is written after the rows because one of its four values is not
	// knowable before then. Setting all four here rather than passing three as
	// constructor options keeps them in one place and in a fixed order, which
	// is one fewer thing to check when asking why two files differ.
	w.SetKeyValueMetadata(metaSourceFile, stamp.SourceFile)
	w.SetKeyValueMetadata(metaSourceSHA256, stamp.SourceSHA256)
	w.SetKeyValueMetadata(metaCodecVersion, strconv.Itoa(CodecVersion))
	w.SetKeyValueMetadata(metaRows, strconv.Itoa(len(out)))

	// Close writes the footer. An unclosed writer produces a file that is
	// entirely data and no index, which is to say not a parquet file at all —
	// so this error is never one to log and continue past.
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("parquet: writing %q: %w", stamp.SourceFile, err)
	}

	return out, nil
}

// writeRows streams seq into w in batches and returns how many rows it wrote.
//
// If collect is non-nil, every candle is appended to it as well. That is what
// the cache wants — it is filling a cache entry for a caller who asked for
// these candles, so decoding the archive once and keeping the result costs
// nothing — and it is exactly what an export does *not* want, since holding
// five years of candles to write them to a file the caller is not going to read
// back is 820 MB spent for no reason. One parameter, two behaviours, one loop.
//
// Errors come back unwrapped. The two callers write to different kinds of
// destination — a cache entry named after an archive, and whatever io.Writer a
// user handed [WriteParquet] — so each wraps with the name it can supply, and a
// message never says "writing" twice.
func writeRows(
	ctx context.Context,
	w *parquet.GenericWriter[parquetRow],
	seq iter.Seq2[Kline, error],
	collect *[]Kline,
) (int, error) {
	batch := make([]parquetRow, 0, parquetBatchRows)
	rows := 0

	for k, err := range seq {
		if err != nil {
			return rows, err
		}

		row, err := k.toParquetRow()
		if err != nil {
			return rows, err
		}

		if collect != nil {
			*collect = append(*collect, k)
		}

		batch = append(batch, row)
		rows++

		if len(batch) == cap(batch) {
			if _, err := w.Write(batch); err != nil {
				return rows, err
			}

			// Reslicing to zero length keeps the backing array, so the loop
			// allocates exactly one batch for the whole file.
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if _, err := w.Write(batch); err != nil {
			return rows, err
		}
	}

	// Cancellation is checked once, after the rows and before the footer.
	// Checking it inside the loop would be pointless: the loop's own source is
	// [decodeArchive], which already stops on a cancelled context, so by the
	// time control reaches here a cancelled caller has already stopped
	// decoding. What this catches is the case where the last row arrived just
	// as the caller gave up, and it costs one atomic load per file.
	if err := ctx.Err(); err != nil {
		return rows, err
	}

	return rows, nil
}

// WriteParquet writes candles to w as a parquet file, and returns how many it
// wrote.
//
// It is the export half of the format the cache stores its second tier in: same
// schema, same column types, same writer settings, so a file written here and a
// file written by the cache differ only in their footer metadata. Reach for it
// when you want candles on disk in a form a query engine can read — DuckDB,
// Polars, pandas, Spark — without going through CSV and its float64s.
//
//	f, err := os.Create("btcusdt-1h-2024.parquet")
//	// ...
//	n, err := binancedata.WriteParquet(ctx, f, loader.Stream(ctx, req))
//
// # Why it takes an iterator
//
// Because [Loader.Stream] produces one, and the pair of them is what lets a
// range larger than memory reach a file. A []Kline parameter would have forced
// the whole range to exist at once, which is the thing Stream is for avoiding —
// five years of one-minute candles is about 820 MB of Kline. A caller who
// *does* have a slice can pass slices.All(klines) with an error-free adapter,
// which is a cheap wrapper; the reverse is not cheap at all.
//
// # What it does not write
//
// No source stamp. The cache binds each tier-2 file to the archive it was built
// from with a SHA-256 in the footer, because a derived file that cannot prove
// its provenance is one the read path would have to accept on trust. An export
// has no single archive behind it — a year's worth of candles comes from twelve
// archives and possibly the REST API — so there is nothing honest to put there,
// and inventing a value would make a file that reads as a cache entry without
// being one. The codec version and the row count are written, because both are
// true of any file this function produces.
//
// # Errors
//
// Nothing is written to w after an error, but what was already written stays
// written: parquet puts its footer last, so an interrupted export leaves bytes
// no reader will accept rather than a short file that looks complete. If that
// matters, write to a temporary file and rename it once this returns nil —
// which is what the cache does, and for exactly this reason.
func WriteParquet(ctx context.Context, w io.Writer, seq iter.Seq2[Kline, error]) (int, error) {
	pw := parquet.NewGenericWriter[parquetRow](w, parquetWriterOptions()...)

	rows, err := writeRows(ctx, pw, seq, nil)
	if err != nil {
		return rows, fmt.Errorf("parquet: exporting: %w", err)
	}

	pw.SetKeyValueMetadata(metaCodecVersion, strconv.Itoa(CodecVersion))
	pw.SetKeyValueMetadata(metaRows, strconv.Itoa(rows))

	if err := pw.Close(); err != nil {
		return rows, fmt.Errorf("parquet: exporting: %w", err)
	}

	return rows, nil
}

// parquetWriterOptions is every writer setting this package pins.
//
// It exists as a function rather than a slice variable so that no caller can
// append to it and silently change the format for everyone else.
//
// Each option is here to keep the output reproducible, and the last one is the
// interesting case: parquet-go's default "created by" string is built from the
// module's build information, so it changes whenever the library is rebuilt
// from a different commit. That would make two byte-identical caches differ in
// their footers for a reason having nothing to do with their contents. Pinning
// it to [CodecVersion] means the string changes exactly when the meaning of the
// rows changes, which is the property docs/caching.md asks for.
func parquetWriterOptions() []parquet.WriterOption {
	return []parquet.WriterOption{
		// Snappy over the alternatives on purpose: it decompresses several
		// times faster than zstd or gzip at a modest cost in size, and this is
		// a read cache whose whole reason for existing is read speed.
		parquet.Compression(&parquet.Snappy),
		parquet.MaxRowsPerRowGroup(parquetRowGroupRows),
		parquet.CreatedBy("binancedata", strconv.Itoa(CodecVersion), ""),
	}
}

// checkParquet reports whether a tier-2 file would be accepted by [readKlines],
// without reading a single candle out of it.
//
// It is every gate that function opens with and deliberately nothing after
// them: [checkStamp] against the footer, [checkSchema] against the column
// layout, and the stamped row count [readKlines] bounds its allocations by.
// All three read only parquet's footer, which sits at the end of the file, so
// the whole question costs one open and one seek however large the file is.
//
// The row count is the easiest of the three to leave out, and leaving it out
// would be a real hole rather than a tidiness one. readKlines refuses a footer
// whose bmd.rows key is missing or unparseable, and refuses one whose count
// disagrees with what the row groups add up to — so a file failing only those
// is one this would call readable and the reader would reject. The archive
// beside it would be pruned on that verdict and the next read would go to the
// network, which is the one outcome pruning promises not to cause.
//
// Both of those are decidable from the footer, which is why they belong here.
// The row-group totals are footer metadata: parquet records each group's row
// count in the same structure as the key/value pairs, so comparing them against
// the stamp costs arithmetic and no further reading.
//
// # What it is for, and the gap it leaves
//
// Pruning tier 1. An archive may only be deleted when the parquet beside it can
// serve reads on its own, and "can serve" has to be decided by the same rule the
// read path uses or the two will drift apart — a prune that is more permissive
// than the reader deletes archives that are about to be needed again.
//
// It is deliberately *not* a full read, and the difference is worth naming
// because it is the one way a prune can cost something. [readKlines] goes on to
// decode every page, and parquet checksums each one, so bit rot inside a data
// page surfaces there and not here. An archive pruned on this evidence whose
// parquet later fails a page checksum has to be downloaded again.
//
// That residual is accepted rather than closed, and the arithmetic is why:
// closing it means decoding every cached file — 6.1 ms and up to 14 MB resident
// per symbol-month — to answer a question about disk space. It is also the same
// cost class as the [CodecVersion] bump that docs/caching.md already documents
// as pruning's price. What the two gates below do rule out is every *silent*
// version of the problem: a file from another tool, from another archive, from
// an older codec, or with its columns in a different order.
func checkParquet(r io.ReaderAt, size int64, want parquetStamp) error {
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return fmt.Errorf("parquet: reading %q: %w", want.SourceFile, err)
	}

	if err := checkStamp(f, want); err != nil {
		return err
	}

	if err := checkSchema(f, want); err != nil {
		return err
	}

	stamped, err := lookupInt(f, metaRows)
	if err != nil {
		return err
	}

	// The same arithmetic readKlines does, without the reading in between: it
	// bounds each row group against the stamp on the way through and compares
	// the total once at the end. A file whose footer disagrees with itself is a
	// truncated or half-rewritten one, and its archive is the only place the
	// missing rows still exist.
	var rows int64

	for _, rowGroup := range f.RowGroups() {
		rows += rowGroup.NumRows()
	}

	if rows != int64(stamped) {
		return fmt.Errorf("parquet: %q: row groups hold %d rows, footer says %d: %w",
			want.SourceFile, rows, stamped, ErrCorruptArchive)
	}

	return nil
}

// readKlines reads a tier-2 file, after checking that it may be used.
//
// The check is the validity rule from docs/caching.md, and it is two
// comparisons against the footer: the source hash must be the one in the
// archive's .CHECKSUM sidecar, and the codec version must be the one this
// binary was compiled with. Both are read from the footer, which parquet stores
// at the end of the file — so a rejected file costs one seek and no decoding.
//
// # The rows are read a column at a time
//
// Not a row at a time, which is what parquet-go's [parquet.GenericReader] does
// and what the obvious implementation would use. The obvious one was written
// first and measured: reconstructing 44,640 structs through reflection took
// 26.5 ms and allocated 99,573 times, against 62 ms to decode the same month
// from its CSV — a cache barely twice as fast as the work it replaces is not
// worth a second copy of the data on disk.
//
// Reading each column as one contiguous run of values instead takes 6.1 ms and
// allocates 479 times: about ten times faster than parsing the archive, which
// is the margin this whole design was predicated on. The values land straight
// in the [Kline] fields they belong to, so nothing intermediate is built.
//
// It is worth seeing why the column layout makes that possible. On disk, all
// 44,640 open times sit together, then all 44,640 opens, and so on; a run of
// one type can be copied in bulk, where a row-shaped read has to visit eleven
// different columns to assemble each candle. It is also why a caller that only
// wanted closing prices could skip nine columns entirely — an optimisation this
// code does not take, but the format leaves available.
//
// # What is deliberately not checked
//
// The archive is neither opened nor re-hashed. Re-hashing tier 1 on every read
// would cost more than the parse this cache exists to avoid, which is why
// integrity is verified where it is cheap — once, at download — and on demand
// through `bmd verify`.
//
// Every failure here means "rebuild from tier 1", including a corrupt file:
// parquet checksums each data page, so bit rot surfaces as a read error rather
// than as wrong numbers. The caller decides what to do about it.
func readKlines(ctx context.Context, r io.ReaderAt, size int64, want parquetStamp) ([]Kline, error) {
	f, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("parquet: reading %q: %w", want.SourceFile, err)
	}

	if err := checkStamp(f, want); err != nil {
		return nil, err
	}

	// Reading positionally — column 0 is the open time, column 1 the close time
	// — is only safe once the file's schema has been confirmed to be the one
	// this package writes. Skipping this would produce the quietest bug in the
	// catalogue: a column order that shifted by one puts the high price in the
	// low price's field, and every value still parses.
	if err := checkSchema(f, want); err != nil {
		return nil, err
	}

	// NumRows comes from a file this process may not have written, so it sizes
	// the slice only up to the same cap the decoder uses. A corrupt footer
	// claiming a billion rows would otherwise reserve gigabytes before the
	// first row is read.
	out := make([]Kline, 0, min(max(f.NumRows(), 0), maxRowsHint))

	// The stamped count is read before the rows rather than after, because the
	// loop below allocates from counts the file supplies and every one of those
	// has to be bounded by something. See the two checks inside it.
	stamped, err := lookupInt(f, metaRows)
	if err != nil {
		return nil, err
	}

	for _, rowGroup := range f.RowGroups() {
		// One context check per row group, for the same reason the decoder
		// checks every 1024 rows: a cancelled backtest should not wait out the
		// rest of a 2.6-million-row file. A row group is at most
		// parquetRowGroupRows candles, so the granularity is comparable.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rows := int(rowGroup.NumRows())

		// A row group larger than the writer's own cap was not written by this
		// package, and a count that overruns the footer's total means the file
		// disagrees with itself. Both are checked *before* the allocation below
		// rather than after the read, because a corrupt count is a number this
		// code would otherwise reserve memory for on the file's say-so.
		if rows < 0 || rows > parquetRowGroupRows {
			return nil, fmt.Errorf("parquet: reading %q: row group claims %d rows, cap is %d: %w",
				want.SourceFile, rows, parquetRowGroupRows, errCacheStale)
		}

		if len(out)+rows > stamped {
			return nil, fmt.Errorf("parquet: reading %q: row groups total more than the footer's %d rows: %w",
				want.SourceFile, stamped, ErrCorruptArchive)
		}

		// The candles for this row group are appended as zero values and then
		// filled in place, one column at a time. slices.Grow does the single
		// allocation; the reslice exposes the space it reserved.
		base := len(out)
		out = slices.Grow(out, rows)[:base+rows]
		dst := out[base : base+rows]

		chunks := rowGroup.ColumnChunks()
		if len(chunks) != len(klineColumns) {
			return nil, fmt.Errorf("parquet: reading %q: row group has %d columns, want %d: %w",
				want.SourceFile, len(chunks), len(klineColumns), errCacheStale)
		}

		for i, chunk := range chunks {
			if err := readColumn(dst, klineColumns[i], chunk); err != nil {
				return nil, fmt.Errorf("parquet: reading %q: column %s: %w",
					want.SourceFile, klineColumns[i].name, err)
			}
		}
	}

	// The other direction: too few rows rather than too many. This is what
	// catches a file that was truncated after its footer was written — rare,
	// but the failure it prevents is a short range returned as if it were
	// complete, which is the class of bug this library exists to eliminate.
	if stamped != len(out) {
		return nil, fmt.Errorf("parquet: reading %q: read %d rows, footer says %d: %w",
			want.SourceFile, len(out), stamped, ErrCorruptArchive)
	}

	return out, nil
}

// checkStamp applies the two-part validity rule to an open file.
func checkStamp(f *parquet.File, want parquetStamp) error {
	sum, ok := f.Lookup(metaSourceSHA256)
	if !ok {
		// A parquet file without our stamp is not ours: another tool's output
		// that happens to sit at this path, or a file from before the stamp
		// existed. Either way it cannot be validated, so it is not used.
		return fmt.Errorf("parquet: %q has no %s: %w", want.SourceFile, metaSourceSHA256, errCacheStale)
	}

	if sum != want.SourceSHA256 {
		return fmt.Errorf("parquet: %q was built from an archive hashing to %s, want %s: %w",
			want.SourceFile, sum, want.SourceSHA256, errCacheStale)
	}

	version, err := lookupInt(f, metaCodecVersion)
	if err != nil {
		return err
	}

	if version != CodecVersion {
		// The one trigger that fires while the cache is byte-identical: the
		// parser changed, so rows built by the old one have to go, however
		// intact the archive beside them still is.
		return fmt.Errorf("parquet: %q was built by codec version %d, this binary is version %d: %w",
			want.SourceFile, version, CodecVersion, errCacheStale)
	}

	return nil
}

// lookupInt reads one footer key as an integer.
//
// A missing or unparsable key is reported as staleness rather than corruption:
// the file is not one this version of the library wrote, which is a reason to
// rebuild and not a reason to fail.
func lookupInt(f *parquet.File, key string) (int, error) {
	s, ok := f.Lookup(key)
	if !ok {
		return 0, fmt.Errorf("parquet: footer has no %s: %w", key, errCacheStale)
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("parquet: footer %s is %q: %w", key, s, errCacheStale)
	}

	return n, nil
}

// toParquetRow converts a candle into its stored form.
func (k Kline) toParquetRow() (parquetRow, error) {
	var row parquetRow

	// A pair of a source value and where it is stored, so the eight
	// near-identical conversions below can be written as a loop rather than as
	// eight blocks that differ only in which error message they carry. The
	// pointer is what lets the loop write into the struct.
	//
	// The column *names* are deliberately not repeated here. They live in
	// [klineColumns], which the reader already uses, and taking them from there
	// means the two tables cannot drift into disagreeing about what this file
	// calls its columns. The order below therefore has to match that table's
	// decimal run, which is what [firstDecimalColumn] names and a test pins.
	fields := [...]struct {
		src udecimal.Decimal
		dst *[parquetDecimalBytes]byte
	}{
		{k.Open, &row.Open},
		{k.High, &row.High},
		{k.Low, &row.Low},
		{k.Close, &row.Close},
		{k.Volume, &row.Volume},
		{k.QuoteVolume, &row.QuoteVolume},
		{k.TakerBuyBaseVolume, &row.TakerBuyBaseVolume},
		{k.TakerBuyQuoteVolume, &row.TakerBuyQuoteVolume},
	}

	for i, f := range fields {
		b, err := encodeDecimal(f.src)
		if err != nil {
			return parquetRow{}, fmt.Errorf("%s: %w", klineColumns[firstDecimalColumn+i].name, err)
		}

		*f.dst = b
	}

	// UnixMicro is exact for every timestamp this library produces: the decoder
	// builds them with time.UnixMilli or time.UnixMicro, so neither carries a
	// sub-microsecond component to lose.
	row.OpenTime = k.OpenTime.UnixMicro()
	row.CloseTime = k.CloseTime.UnixMicro()
	row.Trades = k.Trades

	return row, nil
}

// klineColumn is one stored column and where its values belong in a [Kline].
//
// Exactly one of the two setters is non-nil, which is how the reader knows
// whether it is looking at an integer column or a decimal one. Go has no sum
// type to say "one of these two", so the invariant is stated here and checked
// by a test rather than by the compiler.
type klineColumn struct {
	name string

	setInt func(*Kline, int64)
	setDec func(*Kline, [parquetDecimalBytes]byte) error
}

// physical is the parquet type this column has to be stored as.
//
// The width is returned alongside the kind because FIXED_LEN_BYTE_ARRAY is the
// one physical type whose size is part of its identity: a 16-byte and a 12-byte
// fixed array are the same Kind and not the same column. A zero width means
// "this kind has no width of its own", which is the INT64 case.
func (c klineColumn) physical() (kind parquet.Kind, width int) {
	if c.setInt != nil {
		return parquet.Int64, 0
	}

	return parquet.FixedLenByteArray, parquetDecimalBytes
}

// firstDecimalColumn is where the eight decimal columns start in
// [klineColumns]. The two timestamps precede them and the trade count follows,
// so the decimals are one contiguous run — which is what lets [toParquetRow]
// index into this table instead of keeping a second copy of the names.
const firstDecimalColumn = 2

// klineColumns is the schema, in the order the columns appear in the file.
//
// This table is the counterpart to the col* constants in codec.go, and exists
// for the same reason: reading column 3 as the low price is a claim, and a
// claim written as a bare index is one nobody can check while reading the code.
// The order must match [parquetRow]'s field order, which is what
// [checkSchema] confirms against the file before a single value is read.
var klineColumns = [...]klineColumn{
	// .UTC() is not cosmetic. time.UnixMicro returns a time in the local zone,
	// and every time in this package is documented to be UTC — a candle whose
	// location depended on the machine that read it would compare unequal to
	// itself across two processes.
	{name: "open_time", setInt: func(k *Kline, v int64) { k.OpenTime = time.UnixMicro(v).UTC() }},
	{name: "close_time", setInt: func(k *Kline, v int64) { k.CloseTime = time.UnixMicro(v).UTC() }},

	{name: "open", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.Open, err = decodeDecimal(b)

		return err
	}},
	{name: "high", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.High, err = decodeDecimal(b)

		return err
	}},
	{name: "low", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.Low, err = decodeDecimal(b)

		return err
	}},
	{name: "close", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.Close, err = decodeDecimal(b)

		return err
	}},
	{name: "volume", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.Volume, err = decodeDecimal(b)

		return err
	}},
	{name: "quote_volume", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.QuoteVolume, err = decodeDecimal(b)

		return err
	}},
	{name: "taker_buy_base_volume", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.TakerBuyBaseVolume, err = decodeDecimal(b)

		return err
	}},
	{name: "taker_buy_quote_volume", setDec: func(k *Kline, b [parquetDecimalBytes]byte) (err error) {
		k.TakerBuyQuoteVolume, err = decodeDecimal(b)

		return err
	}},

	{name: "trades", setInt: func(k *Kline, v int64) { k.Trades = v }},
}

// checkSchema confirms that the file's columns are the ones this package
// writes, in the order it writes them, stored as the types it wrote them as.
//
// [parquet.Schema.Columns] returns one path per leaf column, which for a flat
// schema is a one-element path per column. Comparing those names is what makes
// the positional reads in [readKlines] safe: a file with the same columns in a
// different order — a plausible result of a future schema change, or of another
// tool having written it — is rejected rather than read into the wrong fields.
//
// # Why the physical type is checked as well as the name
//
// Because the same reasoning applies to a column with the right name and the
// wrong storage, and the consequence there is worse than a wrong number.
// [readPage] chooses its fast path by asserting on the decoded page's type and
// falls back to [readGenericPage] when the assertion fails, so a column named
// "high" that is physically an INT64 lands in the fallback's decimal branch and
// asks a [parquet.Value] holding an integer for its bytes. parquet-go
// implements that as an unsafe.Slice over a pointer only the byte-array kinds
// ever set, so it panics the calling goroutine rather than returning anything.
// A library that panics is a caller that crashes; checking the type here turns
// the whole class into an ordinary errCacheStale and a rebuild from tier 1.
func checkSchema(f *parquet.File, want parquetStamp) error {
	schema := f.Schema()

	columns := schema.Columns()
	if len(columns) != len(klineColumns) {
		return fmt.Errorf("parquet: %q has %d columns, want %d: %w",
			want.SourceFile, len(columns), len(klineColumns), errCacheStale)
	}

	for i, path := range columns {
		col := klineColumns[i]

		if len(path) != 1 || path[0] != col.name {
			return fmt.Errorf("parquet: %q column %d is %v, want %s: %w",
				want.SourceFile, i, path, col.name, errCacheStale)
		}

		// Lookup cannot fail for a path Columns has just handed over, but the
		// comma-ok form costs nothing and the alternative to checking it is a
		// nil dereference on the line below.
		leaf, ok := schema.Lookup(path...)
		if !ok {
			return fmt.Errorf("parquet: %q has no column %s: %w", want.SourceFile, col.name, errCacheStale)
		}

		kind, width := col.physical()

		if t := leaf.Node.Type(); t.Kind() != kind || (width > 0 && t.Length() != width) {
			return fmt.Errorf("parquet: %q column %s is stored as %s(%d), want %s(%d): %w",
				want.SourceFile, col.name, t.Kind(), t.Length(), kind, width, errCacheStale)
		}
	}

	return nil
}

// readColumn fills one field of every candle in dst from one column chunk.
//
// A column chunk is stored as a sequence of pages, each holding a run of
// values; the loop below walks them and hands each to [readPage], which appends
// into the candles that have not been filled yet.
//
// The batch buffer is allocated here, once for the whole chunk, rather than
// inside the page readers. parquet-go's pages hold 256 KiB by default, so a
// column at the writer's row-group cap spans several of them and a buffer per
// page would make allocations scale with page count on the one path this design
// exists to keep cheap.
func readColumn(dst []Kline, col klineColumn, chunk parquet.ColumnChunk) error {
	pages := chunk.Pages()

	// Closed on every path. A Pages holds a reader over the underlying file
	// and a decompression buffer; leaking one per column would leak eleven per
	// row group.
	defer func() { _ = pages.Close() }()

	buf := newPageBuffer(col)
	filled := 0

	for {
		page, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		n, err := readPage(dst[filled:], col, page, &buf)

		// Release returns the page's buffers to parquet-go's pool. It is not
		// required for correctness — Go collects them either way — but the
		// pool is why reading a month allocates a few hundred times instead of
		// a few hundred thousand.
		parquet.Release(page)

		if err != nil {
			return err
		}

		filled += n
	}

	// Every column must supply a value for every row. A short column would
	// leave candles half-filled, with zero prices that look like data; a long
	// one means the file disagrees with its own row count.
	if filled != len(dst) {
		return fmt.Errorf("%d values for %d rows: %w", filled, len(dst), ErrCorruptArchive)
	}

	return nil
}

// readPage copies one page's values into the candles at the front of dst.
//
// # Two paths
//
// parquet-go decodes a page into a typed buffer and exposes it through
// interfaces a caller can assert on: [parquet.Int64Reader] for the timestamp
// and trade-count columns, [parquet.BE128Reader] for the 16-byte decimals.
// Both hand over a whole run of values in one call, which is where the speed
// comes from.
//
// The fallback below them exists because those interfaces are an optimisation
// the library offers rather than a guarantee it makes: a page encoded some
// other way — a dictionary, or an encoding a future version prefers — exposes
// only the generic [parquet.ValueReader]. Reading it through that is several
// times slower and perfectly correct, which is the right trade for a path that
// exists so an upgrade cannot turn into a cache that refuses to read itself.
func readPage(dst []Kline, col klineColumn, page parquet.Page, buf *pageBuffer) (int, error) {
	values := page.Values()

	if col.setInt != nil {
		if reader, ok := values.(parquet.Int64Reader); ok {
			return readInt64Page(dst, col, reader, buf.ints)
		}
	}

	if col.setDec != nil {
		if reader, ok := values.(parquet.BE128Reader); ok {
			return readDecimalPage(dst, col, reader, buf.decs)
		}
	}

	return readGenericPage(dst, col, values, buf)
}

// pageBuffer is the scratch space one column's pages are decoded through.
//
// The three slices are the three shapes [readPage] can need, and only one of
// them is ever used for a given column. They share a struct so that
// [readColumn] can allocate once and hand the same buffer to every page in the
// chunk.
type pageBuffer struct {
	ints   []int64
	decs   [][parquetDecimalBytes]byte
	values []parquet.Value
}

// newPageBuffer allocates the batch buffer col's fast path will use.
//
// values is left nil on purpose and allocated on demand by [readGenericPage].
// That path is not taken for any file this package writes, so reserving a batch
// of [parquet.Value] up front would allocate eleven unused buffers per row
// group to serve a case that almost never arrives.
func newPageBuffer(col klineColumn) pageBuffer {
	if col.setInt != nil {
		return pageBuffer{ints: make([]int64, parquetBatchRows)}
	}

	return pageBuffer{decs: make([][parquetDecimalBytes]byte, parquetBatchRows)}
}

// readInt64Page fills dst from one page of an INT64 column — the two timestamps
// and the trade count.
//
// The shape of the loop is parquet-go's contract rather than a choice: ReadInt64s
// returns a count *and* io.EOF together on the call that drains the page, so the
// values have to be consumed before the error is examined. Checking err first,
// as the usual Go reading loop does, would silently drop the last batch.
//
// The bounds check is the one thing standing between a file that lies about its
// row count and a write past the end of the caller's slice.
func readInt64Page(dst []Kline, col klineColumn, reader parquet.Int64Reader, buf []int64) (int, error) {
	filled := 0

	for {
		n, err := reader.ReadInt64s(buf)
		if n > len(dst)-filled {
			return 0, errTooManyValues(len(dst))
		}

		for i, v := range buf[:n] {
			col.setInt(&dst[filled+i], v)
		}

		filled += n

		if errors.Is(err, io.EOF) {
			return filled, nil
		}

		if err != nil {
			return 0, err
		}
	}
}

// readDecimalPage fills dst from one page of a FIXED_LEN_BYTE_ARRAY(16) column
// — the eight prices and volumes. It is [readInt64Page] with a different
// element type and a setter that can fail, and the loop's shape is the same
// count-then-error contract described there.
//
// "BE128" is parquet-go's name for a big-endian 128-bit value, which is exactly
// how the format stores a DECIMAL(38,8). The bytes arrive in the layout
// [decodeDecimal] expects, so nothing reorders them on the way through.
func readDecimalPage(dst []Kline, col klineColumn, reader parquet.BE128Reader, buf [][parquetDecimalBytes]byte) (int, error) {
	filled := 0

	for {
		n, err := reader.ReadBE128s(buf)
		if n > len(dst)-filled {
			return 0, errTooManyValues(len(dst))
		}

		for i, v := range buf[:n] {
			if err := col.setDec(&dst[filled+i], v); err != nil {
				return 0, err
			}
		}

		filled += n

		if errors.Is(err, io.EOF) {
			return filled, nil
		}

		if err != nil {
			return 0, err
		}
	}
}

// readGenericPage is the encoding-independent path. See [readPage] for when it
// is taken and why it exists.
func readGenericPage(dst []Kline, col klineColumn, values parquet.ValueReader, buf *pageBuffer) (int, error) {
	if buf.values == nil {
		buf.values = make([]parquet.Value, parquetBatchRows)
	}

	filled := 0

	for {
		n, err := values.ReadValues(buf.values)
		if n > len(dst)-filled {
			return 0, errTooManyValues(len(dst))
		}

		for i, v := range buf.values[:n] {
			if col.setInt != nil {
				col.setInt(&dst[filled+i], v.Int64())

				continue
			}

			// [checkSchema] has already established that this column is stored
			// as a FIXED_LEN_BYTE_ARRAY(16), so this should be unreachable. It
			// is checked anyway because the cost of being wrong is not an error:
			// ByteArray is an unsafe.Slice over a pointer only the byte-array
			// kinds populate, so asking an integer Value for its bytes panics
			// the goroutine instead of returning something this code could
			// reject. A guard here keeps the failure inside the error type even
			// if a future encoding reaches this loop by a route nobody planned.
			if kind := v.Kind(); kind != parquet.FixedLenByteArray && kind != parquet.ByteArray {
				return 0, fmt.Errorf("value is stored as %s, want %s: %w",
					kind, parquet.FixedLenByteArray, ErrCorruptArchive)
			}

			raw := v.ByteArray()
			if len(raw) != parquetDecimalBytes {
				return 0, fmt.Errorf("value is %d bytes, want %d: %w", len(raw), parquetDecimalBytes, ErrCorruptArchive)
			}

			// A parquet.Value's bytes point into the page's own buffer, which
			// Release hands back to a pool. Copying into an array rather than
			// keeping the slice is what stops a later page from rewriting a
			// decimal that has already been read.
			var b [parquetDecimalBytes]byte
			copy(b[:], raw)

			if err := col.setDec(&dst[filled+i], b); err != nil {
				return 0, err
			}
		}

		filled += n

		if errors.Is(err, io.EOF) {
			return filled, nil
		}

		if err != nil {
			return 0, err
		}
	}
}

func errTooManyValues(rows int) error {
	return fmt.Errorf("column holds more values than the row group's %d rows: %w", rows, ErrCorruptArchive)
}

// pow10 is 10^n for the scales this file can be asked to shift by.
//
// A table rather than a loop because the values are known at compile time and
// there are nine of them; the index is always parquetScale minus a decimal's
// own precision, so it can never exceed 8.
var pow10 = [parquetScale + 1]uint64{
	1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000,
}

// maxUnscaledHi and maxUnscaledLo are 10^38, the exclusive upper bound on the
// magnitude of a DECIMAL(38,8) value's unscaled integer, as a 128-bit number
// split into two 64-bit halves.
//
// The bound is not 2^128. A 16-byte two's-complement integer holds up to about
// 3.4e38, but the declared precision of 38 digits says the value is at most
// 1e38 - 1, and a reader is entitled to believe the schema. Writing a larger
// value would produce a file that is valid parquet and lies about itself.
//
// The constants are spelled in hex rather than computed at init so that this
// stays a compile-time fact with no package-level initialisation; the test
// checks them against math/big rather than against a second copy of the same
// literal.
const (
	maxUnscaledHi = 0x4b3b4ca85a86c47a
	maxUnscaledLo = 0x098a224000000000
)

// encodeDecimal renders d as the 16 bytes a DECIMAL(38,8) column stores: the
// value multiplied by 10^8, as a big-endian two's-complement integer.
//
// # Why this is not a formatting problem
//
// The obvious implementation converts to a string and parses it back into a
// big.Int, and it would allocate twice per value — 20 million allocations for a
// five-year backtest of one-minute candles. udecimal already holds exactly what
// is needed: a 128-bit magnitude plus a sign plus a decimal precision, which
// [udecimal.Decimal.ToHiLo] hands over in a couple of nanoseconds. All that is
// left is a shift to the fixed scale and a byte order.
func encodeDecimal(d udecimal.Decimal) ([parquetDecimalBytes]byte, error) {
	var out [parquetDecimalBytes]byte

	neg, hi, lo, prec, ok := d.ToHiLo()
	if !ok {
		// udecimal falls back to a big.Int beyond 2^128. No Binance value comes
		// within twenty orders of magnitude of that, and such a value could not
		// be stored in this schema anyway.
		return out, fmt.Errorf("%s does not fit in 128 bits: %w", d, ErrCorruptArchive)
	}

	if int(prec) > parquetScale {
		// Rescaling downward would round, and a cache that rounds is a cache
		// that returns different numbers than the archive it claims to mirror.
		return out, fmt.Errorf("%s has %d decimal places, the schema stores %d: %w",
			d, prec, parquetScale, ErrCorruptArchive)
	}

	// Shift the coefficient up to the fixed scale: a value parsed as "0.5" has
	// precision 1 and coefficient 5, and must be stored as 50000000.
	hi, lo, ok = mul128(hi, lo, pow10[parquetScale-int(prec)])
	if !ok || !less128(hi, lo, maxUnscaledHi, maxUnscaledLo) {
		return out, fmt.Errorf("%s exceeds decimal(%d,%d): %w", d, parquetPrecision, parquetScale, ErrCorruptArchive)
	}

	// Big-endian, because that is what the parquet specification requires of a
	// decimal stored in a fixed-length byte array. It is also why the high half
	// goes first.
	binary.BigEndian.PutUint64(out[:8], hi)
	binary.BigEndian.PutUint64(out[8:], lo)

	if neg {
		negate(&out)
	}

	return out, nil
}

// decodeDecimal is the inverse of [encodeDecimal].
func decodeDecimal(b [parquetDecimalBytes]byte) (udecimal.Decimal, error) {
	// The sign of a two's-complement integer is its top bit.
	neg := b[0]&0x80 != 0
	if neg {
		negate(&b)
	}

	hi := binary.BigEndian.Uint64(b[:8])
	lo := binary.BigEndian.Uint64(b[8:])

	// Negating the most negative 128-bit value yields itself, so a magnitude
	// whose top bit is still set after the negation is that value. It is
	// unreachable from any file this package writes — the range check in
	// encodeDecimal stops eleven orders of magnitude short — but a corrupt or
	// foreign file can contain anything, and the alternative is returning a
	// number with the wrong sign.
	if neg && hi&(1<<63) != 0 {
		return udecimal.Decimal{}, fmt.Errorf("decimal is out of range: %w", ErrCorruptArchive)
	}

	d, err := udecimal.NewFromHiLo(neg, hi, lo, parquetScale)
	if err != nil {
		return udecimal.Decimal{}, fmt.Errorf("decimal: %w: %w", err, ErrCorruptArchive)
	}

	return d, nil
}

// mul128 multiplies the 128-bit value hi:lo by m, reporting whether the result
// still fits in 128 bits.
//
// [math/bits] is the standard library's access to the instructions a CPU
// already has for this: bits.Mul64 returns the full 128-bit product of two
// 64-bit words as a pair, and bits.Add64 threads a carry through an addition.
// Neither allocates, and both compile to a handful of instructions.
func mul128(hi, lo, m uint64) (uint64, uint64, bool) {
	// The low half's product contributes its own high word as a carry into the
	// high half.
	carry, low := bits.Mul64(lo, m)

	// The high half's product must not itself overflow into a third word.
	over, high := bits.Mul64(hi, m)
	if over != 0 {
		return 0, 0, false
	}

	high, c := bits.Add64(high, carry, 0)
	if c != 0 {
		return 0, 0, false
	}

	return high, low, true
}

// less128 reports whether the 128-bit value aHi:aLo is less than bHi:bLo.
func less128(aHi, aLo, bHi, bLo uint64) bool {
	if aHi != bHi {
		return aHi < bHi
	}

	return aLo < bLo
}

// negate replaces b with its two's complement, in place: invert every bit and
// add one.
//
// It is done in two 64-bit halves rather than byte by byte because the carry
// out of the low half is the only thing crossing between them, and bits.Add64
// reports exactly that. ^x is Go's bitwise complement — the operator most
// languages spell ~x, which in Go means nothing at all.
func negate(b *[parquetDecimalBytes]byte) {
	hi := binary.BigEndian.Uint64(b[:8])
	lo := binary.BigEndian.Uint64(b[8:])

	lo, carry := bits.Add64(^lo, 1, 0)
	hi, _ = bits.Add64(^hi, 0, carry)

	binary.BigEndian.PutUint64(b[:8], hi)
	binary.BigEndian.PutUint64(b[8:], lo)
}
