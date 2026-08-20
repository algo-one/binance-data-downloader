package binancedata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/quagmt/udecimal"
)

// fixtureKlines decodes one committed archive into candles, which is where
// every round-trip test below starts. Using real archives rather than
// hand-written candles matters here: the values that stress a decimal128 column
// are the ones Binance actually publishes, and nobody invents a
// 118661604939.99255335 by hand.
func fixtureKlines(t *testing.T, file string, spec decodeSpec) []Kline {
	t.Helper()

	r, size := readFixture(t, file)

	klines, err := decodeArchiveAll(t.Context(), r, size, spec)
	if err != nil {
		t.Fatalf("decoding %s: %v", file, err)
	}

	if len(klines) == 0 {
		t.Fatalf("decoding %s: no rows", file)
	}

	return klines
}

// hourlyFixture is the day-long 1h archive most tests here use: 24 candles,
// millisecond era.
func hourlyFixture(t *testing.T) []Kline {
	t.Helper()

	start, end := dayRange(2024, 1, 15)

	return fixtureKlines(t, "BTCUSDT-1h-2024-01-15.zip", decodeSpec{Interval: Interval1h, Start: start, End: end})
}

func testStamp() parquetStamp {
	return parquetStamp{
		SourceFile:   "BTCUSDT-1h-2024-01-15.zip",
		SourceSHA256: "ea666c96c3441451554ba118c469805b34455156c8f1da0234bd7f2546874d99",
	}
}

// collectSeq turns a slice back into the iterator writeKlines consumes, so a
// test can supply candles without going through an archive.
func collectSeq(klines []Kline) iter.Seq2[Kline, error] {
	return func(yield func(Kline, error) bool) {
		for _, k := range klines {
			if !yield(k, nil) {
				return
			}
		}
	}
}

func TestEncodeDecimalRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string

		// wantHex is the stored form, checked for the cases where the exact
		// bytes are the point. Empty means "only the round trip matters".
		wantHex string
	}{
		{
			name:    "zero",
			value:   "0",
			wantHex: "00000000000000000000000000000000",
		},
		{
			// The scale is fixed, so a value written with one decimal place is
			// stored with eight: 0.5 becomes an unscaled 50,000,000.
			name:    "rescaled up to the fixed scale",
			value:   "0.5",
			wantHex: "00000000000000000000000002faf080",
		},
		{
			name:    "one satoshi",
			value:   "0.00000001",
			wantHex: "00000000000000000000000000000001",
		},
		{
			// A real price from the fixture archives.
			name:  "price",
			value: "41732.35000000",
		},
		{
			// The worst real value measured across 1,751,352 archive values:
			// a BTCUSDT monthly quote volume, 20 significant digits. This is
			// the number that rules out float64 for the whole library.
			name:    "worst real value",
			value:   "118661604939.99255335",
			wantHex: "0000000000000000a4ad121984996f27",
		},
		{
			// Exactly 38 digits: 30 before the point and 8 after. The largest
			// value the schema can hold.
			name:  "largest representable",
			value: "999999999999999999999999999999.99999999",
		},
		{
			// Binance never publishes a negative price or volume, but the
			// storage format is signed and a decoder that got the two's
			// complement wrong would be wrong silently. Round-tripping one
			// costs nothing and pins the sign handling.
			name:    "negative",
			value:   "-0.00000001",
			wantHex: "ffffffffffffffffffffffffffffffff",
		},
		{
			name:  "large negative",
			value: "-118661604939.99255335",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := udecimal.MustParse(tt.value)

			b, err := encodeDecimal(in)
			if err != nil {
				t.Fatalf("encodeDecimal(%s): %v", tt.value, err)
			}

			if tt.wantHex != "" {
				if got := fmt.Sprintf("%x", b); got != tt.wantHex {
					t.Errorf("encodeDecimal(%s) = %s, want %s", tt.value, got, tt.wantHex)
				}
			}

			out, err := decodeDecimal(b)
			if err != nil {
				t.Fatalf("decodeDecimal: %v", err)
			}

			// Equal compares numerically, so 0.5 and 0.50000000 match. That is
			// the right comparison: the scale is a storage detail and the
			// number is what the library promises to preserve.
			if !out.Equal(in) {
				t.Errorf("round trip: got %s, want %s", out, in)
			}
		})
	}
}

func TestEncodeDecimalRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
	}{
		{
			// Nine decimal places cannot be stored at scale 8, and storing it
			// anyway would mean rounding — a cache that returns numbers its
			// source archive does not contain.
			name:  "more decimal places than the scale",
			value: "0.000000001",
		},
		{
			// 10^30 has an unscaled value of 10^38, one past the largest the
			// declared precision allows. It fits in 128 bits, which is exactly
			// why the bound is checked separately from the width.
			name:  "one past the declared precision",
			value: "1000000000000000000000000000000",
		},
		{
			// Beyond 2^128, where udecimal switches to a big.Int and ToHiLo
			// stops answering.
			name:  "beyond 128 bits",
			value: "9999999999999999999999999999999999999999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := udecimal.Parse(tt.value)
			if err != nil {
				t.Fatalf("parsing %s: %v", tt.value, err)
			}

			if _, err := encodeDecimal(d); !errors.Is(err, ErrCorruptArchive) {
				t.Fatalf("encodeDecimal(%s) = %v, want an error wrapping ErrCorruptArchive", tt.value, err)
			}
		})
	}
}

// TestMaxUnscaledConstants checks the hand-written hex bound against arithmetic
// rather than against a second copy of the same literal.
func TestMaxUnscaledConstants(t *testing.T) {
	t.Parallel()

	want := new(big.Int).Exp(big.NewInt(10), big.NewInt(parquetPrecision), nil)

	got := new(big.Int).Lsh(new(big.Int).SetUint64(maxUnscaledHi), 64)
	got.Or(got, new(big.Int).SetUint64(maxUnscaledLo))

	if got.Cmp(want) != 0 {
		t.Errorf("maxUnscaled = %s, want 10^%d = %s", got, parquetPrecision, want)
	}
}

// TestDecodeDecimalRejectsMostNegative covers the one 16-byte pattern whose
// magnitude cannot be taken: -2^127 negates to itself.
func TestDecodeDecimalRejectsMostNegative(t *testing.T) {
	t.Parallel()

	var b [parquetDecimalBytes]byte
	b[0] = 0x80

	if _, err := decodeDecimal(b); !errors.Is(err, ErrCorruptArchive) {
		t.Fatalf("decodeDecimal(-2^127) = %v, want an error wrapping ErrCorruptArchive", err)
	}
}

// TestKlineParquetRoundTrip is the property the whole tier-2 design rests on:
// what comes back is what went in, field for field.
func TestKlineParquetRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		spec decodeSpec
	}{
		{
			name: "millisecond era",
			file: "BTCUSDT-1h-2024-01-15.zip",
			spec: decodeSpec{Interval: Interval1h, Start: utc(2024, 1, 15), End: utc(2024, 1, 16)},
		},
		{
			// Since 2025 the archives are written in microseconds, so a close
			// time ends in .999999. Storing these columns as milliseconds would
			// pass every other test in this file and silently drop three digits
			// from every candle after new year 2025.
			name: "microsecond era",
			file: "BTCUSDT-1h-2025-01-01.zip",
			spec: decodeSpec{Interval: Interval1h, Start: utc(2025, 1, 1), End: utc(2025, 1, 2)},
		},
		{
			// One row, and the row with the largest numbers in the fixture set.
			name: "monthly candles",
			file: "BTCUSDT-1mo-2024-01.zip",
			spec: decodeSpec{Interval: Interval1mo, Start: utc(2024, 1, 1), End: utc(2024, 2, 1)},
		},
		{
			// 1,440 rows: enough to cross several parquet write batches.
			name: "a minute archive",
			file: "BTCUSDT-1m-2025-01-15.zip",
			spec: decodeSpec{Interval: Interval1m, Start: utc(2025, 1, 15), End: utc(2025, 1, 16)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			want := fixtureKlines(t, tt.file, tt.spec)
			stamp := parquetStamp{SourceFile: tt.file, SourceSHA256: testStamp().SourceSHA256}

			var buf bytes.Buffer

			written, err := writeKlines(t.Context(), &buf, collectSeq(want), stamp, len(want))
			if err != nil {
				t.Fatalf("writeKlines: %v", err)
			}

			if len(written) != len(want) {
				t.Fatalf("writeKlines returned %d rows, want %d", len(written), len(want))
			}

			got, err := readKlines(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), stamp)
			if err != nil {
				t.Fatalf("readKlines: %v", err)
			}

			if len(got) != len(want) {
				t.Fatalf("read %d rows, want %d", len(got), len(want))
			}

			for i := range want {
				// Kline.Equal, never ==: a time.Time carries a location
				// pointer and a udecimal.Decimal can carry a big.Int pointer,
				// so == compares addresses for values that are equal.
				if !got[i].Equal(want[i]) {
					t.Fatalf("row %d:\n got %+v\nwant %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// TestWriteKlinesIsReproducible is what makes golden files and cross-machine
// cache diffs possible: nothing in the output depends on when or where it ran.
func TestWriteKlinesIsReproducible(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)

	write := func() []byte {
		var buf bytes.Buffer

		if _, err := writeKlines(t.Context(), &buf, collectSeq(klines), testStamp(), len(klines)); err != nil {
			t.Fatalf("writeKlines: %v", err)
		}

		return buf.Bytes()
	}

	first, second := write(), write()

	if !bytes.Equal(first, second) {
		t.Fatalf("two writes of the same candles differ: %d and %d bytes", len(first), len(second))
	}
}

func TestWriteKlinesRejectsIncompleteStamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stamp parquetStamp
	}{
		{name: "no source file", stamp: parquetStamp{SourceSHA256: testStamp().SourceSHA256}},
		{name: "no hash", stamp: parquetStamp{SourceFile: testStamp().SourceFile}},
		{name: "empty", stamp: parquetStamp{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			_, err := writeKlines(t.Context(), &buf, collectSeq(nil), tt.stamp, 0)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("writeKlines = %v, want an error wrapping ErrInvalidRequest", err)
			}
		})
	}
}

// TestWriteKlinesPropagatesDecodeErrors checks that a corrupt archive stops the
// build rather than producing a short parquet file that looks complete.
func TestWriteKlinesPropagatesDecodeErrors(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)
	boom := errors.New("row 7 is not a candle")

	seq := func(yield func(Kline, error) bool) {
		for _, k := range klines[:3] {
			if !yield(k, nil) {
				return
			}
		}

		yield(Kline{}, fmt.Errorf("%w: %w", boom, ErrCorruptArchive))
	}

	var buf bytes.Buffer

	if _, err := writeKlines(t.Context(), &buf, seq, testStamp(), len(klines)); !errors.Is(err, boom) {
		t.Fatalf("writeKlines = %v, want the decoder's error", err)
	}
}

// rawParquet writes a tier-2 file with the footer keys spelled out, so that a
// test can produce the stale and corrupt files that no version of this code
// would write for itself.
func rawParquet(t *testing.T, klines []Kline, meta map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[parquetRow](&buf, parquetWriterOptions()...)

	rows := make([]parquetRow, 0, len(klines))

	for _, k := range klines {
		row, err := k.toParquetRow()
		if err != nil {
			t.Fatalf("toParquetRow: %v", err)
		}

		rows = append(rows, row)
	}

	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	// Sorted so that the bytes do not depend on map iteration order, which Go
	// deliberately randomises.
	for _, k := range slices.Sorted(maps.Keys(meta)) {
		w.SetKeyValueMetadata(k, meta[k])
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	return buf.Bytes()
}

func TestReadKlinesRejectsUnusableFiles(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)
	stamp := testStamp()

	// The footer a valid file carries, which each case below breaks in exactly
	// one way.
	good := map[string]string{
		metaSourceFile:   stamp.SourceFile,
		metaSourceSHA256: stamp.SourceSHA256,
		metaCodecVersion: strconv.Itoa(CodecVersion),
		metaRows:         strconv.Itoa(len(klines)),
	}

	tests := []struct {
		name    string
		break_  func(m map[string]string)
		wantErr error
	}{
		{
			name:    "no stamp at all",
			break_:  func(m map[string]string) { clear(m) },
			wantErr: errCacheStale,
		},
		{
			// The trigger that fires when Binance republishes a corrected
			// archive: the source changed underneath the derived file.
			name:    "built from a different archive",
			break_:  func(m map[string]string) { m[metaSourceSHA256] = "00" + stamp.SourceSHA256[2:] },
			wantErr: errCacheStale,
		},
		{
			// The trigger that fires on a library upgrade, and the only one
			// that can fire while the cache is byte-for-byte unchanged.
			name:    "built by a newer codec",
			break_:  func(m map[string]string) { m[metaCodecVersion] = strconv.Itoa(CodecVersion + 1) },
			wantErr: errCacheStale,
		},
		{
			name:    "built by an older codec",
			break_:  func(m map[string]string) { m[metaCodecVersion] = strconv.Itoa(CodecVersion - 1) },
			wantErr: errCacheStale,
		},
		{
			name:    "codec version is not a number",
			break_:  func(m map[string]string) { m[metaCodecVersion] = "one" },
			wantErr: errCacheStale,
		},
		{
			name:    "no row count",
			break_:  func(m map[string]string) { delete(m, metaRows) },
			wantErr: errCacheStale,
		},
		{
			// A file that disagrees with itself: the rows are readable, but
			// there are not as many as the footer claims. This is what a
			// truncated or half-rewritten file looks like, and returning its
			// rows would be a short range presented as a complete one.
			name:    "row count disagrees with the rows",
			break_:  func(m map[string]string) { m[metaRows] = strconv.Itoa(len(klines) + 1) },
			wantErr: ErrCorruptArchive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := maps.Clone(good)
			tt.break_(meta)

			b := rawParquet(t, klines, meta)

			_, err := readKlines(t.Context(), bytes.NewReader(b), int64(len(b)), stamp)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("readKlines = %v, want an error wrapping %v", err, tt.wantErr)
			}
		})
	}
}

// TestReadKlinesRejectsNonParquet covers the bytes that are not a parquet file
// at all — a truncated write from an older, non-atomic implementation, or an
// unrelated file that landed at the path.
func TestReadKlinesRejectsNonParquet(t *testing.T) {
	t.Parallel()

	b := []byte("this is not a parquet file, and never was")

	if _, err := readKlines(t.Context(), bytes.NewReader(b), int64(len(b)), testStamp()); err == nil {
		t.Fatal("readKlines accepted a file that is not parquet")
	}
}

func TestReadKlinesHonoursCancellation(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)

	var buf bytes.Buffer

	if _, err := writeKlines(t.Context(), &buf, collectSeq(klines), testStamp(), len(klines)); err != nil {
		t.Fatalf("writeKlines: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := readKlines(ctx, bytes.NewReader(buf.Bytes()), int64(buf.Len()), testStamp())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("readKlines = %v, want context.Canceled", err)
	}
}

func TestWriteKlinesHonoursCancellation(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var buf bytes.Buffer

	_, err := writeKlines(ctx, &buf, collectSeq(klines), testStamp(), len(klines))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("writeKlines = %v, want context.Canceled", err)
	}
}

// TestKlineColumnsMatchTheSchema pins the one thing the column-oriented reader
// takes on faith: that column i of a file is klineColumns[i].
//
// The names come from parquetRow's struct tags and the order from its field
// order, so this compares the table against the schema parquet-go derives from
// the struct rather than against a second hand-written list.
func TestKlineColumnsMatchTheSchema(t *testing.T) {
	t.Parallel()

	columns := parquet.SchemaOf(parquetRow{}).Columns()

	if len(columns) != len(klineColumns) {
		t.Fatalf("the schema has %d columns, klineColumns has %d", len(columns), len(klineColumns))
	}

	for i, path := range columns {
		if len(path) != 1 {
			t.Fatalf("column %d is %v; the schema is meant to be flat", i, path)
		}

		if path[0] != klineColumns[i].name {
			t.Errorf("column %d is %q, klineColumns says %q", i, path[0], klineColumns[i].name)
		}
	}

	// Exactly one setter per column, which is the invariant readPage branches
	// on and which the compiler cannot state.
	for _, col := range klineColumns {
		if (col.setInt == nil) == (col.setDec == nil) {
			t.Errorf("column %q must have exactly one of setInt and setDec", col.name)
		}
	}

	// The eight decimals are one contiguous run starting at
	// firstDecimalColumn. toParquetRow indexes into this table from there to
	// name the field in an error, so a decimal column that moved without the
	// constant moving with it would report the wrong column name.
	for i, col := range klineColumns {
		inRun := i >= firstDecimalColumn && i < firstDecimalColumn+8

		if inRun != (col.setDec != nil) {
			t.Errorf("column %d (%q) has setDec != nil = %v, but firstDecimalColumn says %v",
				i, col.name, col.setDec != nil, inRun)
		}
	}
}

// TestCheckSchemaRejectsAMistypedColumn covers the case a name-only schema
// check lets through: every column named correctly and in the right order, one
// of them stored as the wrong physical type.
//
// It is not a hypothetical. Before checkSchema compared types, this file
// reached readGenericPage's decimal branch with a parquet.Value holding an
// int64, asked it for its bytes, and panicked the goroutine inside
// parquet-go's unsafe.Slice — a crash in a library, where every other
// corruption path here returns an error the cache can rebuild from.
func TestCheckSchemaRejectsAMistypedColumn(t *testing.T) {
	t.Parallel()

	// The eleven real column names in the real order. "high" is an INT64
	// where the schema calls for a FIXED_LEN_BYTE_ARRAY(16).
	type mistyped struct {
		OpenTime  int64 `parquet:"open_time,timestamp(microsecond)"`
		CloseTime int64 `parquet:"close_time,timestamp(microsecond)"`

		Open  [parquetDecimalBytes]byte `parquet:"open,decimal(8:38)"`
		High  int64                     `parquet:"high"`
		Low   [parquetDecimalBytes]byte `parquet:"low,decimal(8:38)"`
		Close [parquetDecimalBytes]byte `parquet:"close,decimal(8:38)"`

		Volume      [parquetDecimalBytes]byte `parquet:"volume,decimal(8:38)"`
		QuoteVolume [parquetDecimalBytes]byte `parquet:"quote_volume,decimal(8:38)"`

		TakerBuyBaseVolume  [parquetDecimalBytes]byte `parquet:"taker_buy_base_volume,decimal(8:38)"`
		TakerBuyQuoteVolume [parquetDecimalBytes]byte `parquet:"taker_buy_quote_volume,decimal(8:38)"`

		Trades int64 `parquet:"trades"`
	}

	stamp := testStamp()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[mistyped](&buf, parquetWriterOptions()...)

	// A nonzero value matters: the panic was unsafe.Slice over a nil pointer
	// with a nonzero length, and the length was the integer's own payload. A
	// row of zeroes would not have reproduced it.
	if _, err := w.Write([]mistyped{{OpenTime: 1, CloseTime: 2, High: 1755561600000000}}); err != nil {
		t.Fatalf("write: %v", err)
	}

	w.SetKeyValueMetadata(metaSourceFile, stamp.SourceFile)
	w.SetKeyValueMetadata(metaSourceSHA256, stamp.SourceSHA256)
	w.SetKeyValueMetadata(metaCodecVersion, strconv.Itoa(CodecVersion))
	w.SetKeyValueMetadata(metaRows, "1")

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := readKlines(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), stamp)
	if !errors.Is(err, errCacheStale) {
		t.Fatalf("readKlines = %v, want an error wrapping errCacheStale", err)
	}
}

// TestReadGenericPageRejectsAMistypedValue is the second half of the same
// defence, at the layer below. checkSchema should mean this is unreachable, so
// the only way to exercise it is to call the reader directly with a column
// whose setter disagrees with the page it is handed.
func TestReadGenericPageRejectsAMistypedValue(t *testing.T) {
	t.Parallel()

	klines := hourlyFixture(t)
	stamp := testStamp()

	var buf bytes.Buffer

	if _, err := writeKlines(t.Context(), &buf, collectSeq(klines), stamp, len(klines)); err != nil {
		t.Fatalf("writeKlines: %v", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	// Column 0 is open_time, an INT64 page. Reading it through a decimal
	// column's setter is what a mistyped file would have produced.
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	defer func() { _ = pages.Close() }()

	page, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}

	dec := klineColumns[firstDecimalColumn]
	pbuf := newPageBuffer(dec)

	got := make([]Kline, len(klines))

	_, err = readGenericPage(got, dec, page.Values(), &pbuf)
	if !errors.Is(err, ErrCorruptArchive) {
		t.Fatalf("readGenericPage = %v, want an error wrapping ErrCorruptArchive", err)
	}
}

// TestReadKlinesRejectsAForeignSchema covers the case the positional read would
// otherwise get silently wrong: a file carrying our stamps whose columns are in
// a different order.
func TestReadKlinesRejectsAForeignSchema(t *testing.T) {
	t.Parallel()

	// The same eleven columns with the first two swapped. Every value still
	// parses; every candle would be wrong.
	type swapped struct {
		CloseTime int64 `parquet:"close_time,timestamp(microsecond)"`
		OpenTime  int64 `parquet:"open_time,timestamp(microsecond)"`

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

	stamp := testStamp()

	var buf bytes.Buffer

	w := parquet.NewGenericWriter[swapped](&buf, parquetWriterOptions()...)

	if _, err := w.Write([]swapped{{OpenTime: 1, CloseTime: 2}}); err != nil {
		t.Fatalf("write: %v", err)
	}

	w.SetKeyValueMetadata(metaSourceFile, stamp.SourceFile)
	w.SetKeyValueMetadata(metaSourceSHA256, stamp.SourceSHA256)
	w.SetKeyValueMetadata(metaCodecVersion, strconv.Itoa(CodecVersion))
	w.SetKeyValueMetadata(metaRows, "1")

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := readKlines(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), stamp)
	if !errors.Is(err, errCacheStale) {
		t.Fatalf("readKlines = %v, want an error wrapping errCacheStale", err)
	}
}

// TestReadGenericPageMatchesTheTypedPath exercises the fallback in readPage.
//
// It cannot be reached by writing a file — parquet-go encodes these columns in
// a form the typed readers handle — so the fallback is called directly and its
// output compared against the path that normally runs. A fallback nobody has
// ever executed is a fallback that does not work.
func TestReadGenericPageMatchesTheTypedPath(t *testing.T) {
	t.Parallel()

	want := hourlyFixture(t)
	stamp := testStamp()

	var buf bytes.Buffer

	if _, err := writeKlines(t.Context(), &buf, collectSeq(want), stamp, len(want)); err != nil {
		t.Fatalf("writeKlines: %v", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	got := make([]Kline, len(want))

	for i, chunk := range f.RowGroups()[0].ColumnChunks() {
		pages := chunk.Pages()

		page, err := pages.ReadPage()
		if err != nil {
			t.Fatalf("column %d: %v", i, err)
		}

		buf := newPageBuffer(klineColumns[i])

		n, err := readGenericPage(got, klineColumns[i], page.Values(), &buf)
		if err != nil {
			t.Fatalf("column %d: %v", i, err)
		}

		if n != len(want) {
			t.Fatalf("column %d: read %d values, want %d", i, n, len(want))
		}

		_ = pages.Close()
	}

	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("candle %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// BenchmarkReadKlines is the measurement the two-tier design has to justify
// itself with. It uses the same 1,440-row archive as BenchmarkDecodeArchiveAll
// in codec_test.go, so the two numbers can be compared directly: this is what a
// cache hit costs, that is what it replaces.
func BenchmarkReadKlines(b *testing.B) {
	start := utc(2025, 1, 15)
	spec := decodeSpec{Interval: Interval1m, Start: start, End: start.AddDate(0, 0, 1)}

	data, err := os.ReadFile(filepath.Join("testdata", "BTCUSDT-1m-2025-01-15.zip"))
	if err != nil {
		b.Fatalf("reading fixture: %v", err)
	}

	klines, err := decodeArchiveAll(b.Context(), bytes.NewReader(data), int64(len(data)), spec)
	if err != nil {
		b.Fatalf("decoding fixture: %v", err)
	}

	stamp := parquetStamp{SourceFile: "BTCUSDT-1m-2025-01-15.zip", SourceSHA256: testStamp().SourceSHA256}

	var buf bytes.Buffer

	if _, err := writeKlines(b.Context(), &buf, collectSeq(klines), stamp, len(klines)); err != nil {
		b.Fatalf("writeKlines: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	size := int64(buf.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		got, err := readKlines(b.Context(), r, size, stamp)
		if err != nil {
			b.Fatalf("readKlines: %v", err)
		}

		if len(got) != 1440 {
			b.Fatalf("row count = %d, want 1440", len(got))
		}
	}
}

// BenchmarkWriteKlines measures the other half: what filling the cache costs,
// which is paid once per archive and never again.
func BenchmarkWriteKlines(b *testing.B) {
	start := utc(2025, 1, 15)
	spec := decodeSpec{Interval: Interval1m, Start: start, End: start.AddDate(0, 0, 1)}

	data, err := os.ReadFile(filepath.Join("testdata", "BTCUSDT-1m-2025-01-15.zip"))
	if err != nil {
		b.Fatalf("reading fixture: %v", err)
	}

	klines, err := decodeArchiveAll(b.Context(), bytes.NewReader(data), int64(len(data)), spec)
	if err != nil {
		b.Fatalf("decoding fixture: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := writeKlines(b.Context(), io.Discard, collectSeq(klines), testStamp(), len(klines)); err != nil {
			b.Fatalf("writeKlines: %v", err)
		}
	}
}

// syntheticMonth builds a month-sized range of candles out of the day-sized
// fixture, so the benchmarks below measure at the size that matters: 44,640
// rows is a month of one-minute candles, the unit docs/caching.md costs
// everything in.
//
// The values repeat every 1,440 rows, which is fine for measuring read and
// write throughput — neither path cares what the numbers are — and is why this
// is a benchmark helper rather than a test fixture.
func syntheticMonth(tb testing.TB, rows int) []Kline {
	tb.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "BTCUSDT-1m-2025-01-15.zip"))
	if err != nil {
		tb.Fatalf("reading fixture: %v", err)
	}

	start := utc(2025, 1, 15)
	spec := decodeSpec{Interval: Interval1m, Start: start, End: start.AddDate(0, 0, 1)}

	base, err := decodeArchiveAll(tb.Context(), bytes.NewReader(data), int64(len(data)), spec)
	if err != nil {
		tb.Fatalf("decoding fixture: %v", err)
	}

	out := make([]Kline, 0, rows)

	for day := 0; len(out) < rows; day++ {
		for _, k := range base {
			if len(out) == rows {
				break
			}

			shift := time.Duration(day) * 24 * time.Hour
			k.OpenTime = k.OpenTime.Add(shift)
			k.CloseTime = k.CloseTime.Add(shift)

			out = append(out, k)
		}
	}

	return out
}

// BenchmarkReadKlinesMonth is the headline number: what one cache hit costs for
// one symbol-month of one-minute candles, against the ~60 ms of CSV parsing
// that decoding the same month from its archive would take.
func BenchmarkReadKlinesMonth(b *testing.B) {
	klines := syntheticMonth(b, 44640)

	var buf bytes.Buffer

	if _, err := writeKlines(b.Context(), &buf, collectSeq(klines), testStamp(), len(klines)); err != nil {
		b.Fatalf("writeKlines: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	size := int64(buf.Len())

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		got, err := readKlines(b.Context(), r, size, testStamp())
		if err != nil {
			b.Fatalf("readKlines: %v", err)
		}

		if len(got) != len(klines) {
			b.Fatalf("read %d rows, want %d", len(got), len(klines))
		}
	}
}

// BenchmarkWriteKlinesMonth measures filling the cache at the same size, which
// is paid once per archive and never again.
func BenchmarkWriteKlinesMonth(b *testing.B) {
	klines := syntheticMonth(b, 44640)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if _, err := writeKlines(b.Context(), io.Discard, collectSeq(klines), testStamp(), len(klines)); err != nil {
			b.Fatalf("writeKlines: %v", err)
		}
	}
}

// TestReadKlinesAcrossRowGroups covers the case none of the fixtures reach: a
// file long enough to be split into several row groups, which is every month of
// 1s candles and none of the committed archives.
func TestReadKlinesAcrossRowGroups(t *testing.T) {
	t.Parallel()

	want := syntheticMonth(t, parquetRowGroupRows+1000)
	stamp := testStamp()

	var buf bytes.Buffer

	if _, err := writeKlines(t.Context(), &buf, collectSeq(want), stamp, len(want)); err != nil {
		t.Fatalf("writeKlines: %v", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	if got := len(f.RowGroups()); got != 2 {
		t.Fatalf("the file has %d row groups, want 2 — the test is not testing what it says", got)
	}

	got, err := readKlines(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), stamp)
	if err != nil {
		t.Fatalf("readKlines: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}

	// The seam is what matters: the last candle of the first row group and the
	// first of the second.
	for _, i := range []int{0, parquetRowGroupRows - 1, parquetRowGroupRows, len(want) - 1} {
		if !got[i].Equal(want[i]) {
			t.Errorf("candle %d:\n got %+v\nwant %+v", i, got[i], want[i])
		}
	}
}

// TestReadKlinesRejectsOversizedRowGroups covers the bound the reader puts on a
// count it takes from the file before allocating for it.
func TestReadKlinesRejectsOversizedRowGroups(t *testing.T) {
	t.Parallel()

	klines := syntheticMonth(t, parquetRowGroupRows+1)
	stamp := testStamp()

	var buf bytes.Buffer

	// A later option wins, so this is the writer's own configuration with the
	// row-group cap raised past what the reader will accept.
	options := append(parquetWriterOptions(), parquet.MaxRowsPerRowGroup(parquetRowGroupRows+1))
	w := parquet.NewGenericWriter[parquetRow](&buf, options...)

	rows := make([]parquetRow, 0, len(klines))

	for _, k := range klines {
		row, err := k.toParquetRow()
		if err != nil {
			t.Fatalf("toParquetRow: %v", err)
		}

		rows = append(rows, row)
	}

	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}

	w.SetKeyValueMetadata(metaSourceFile, stamp.SourceFile)
	w.SetKeyValueMetadata(metaSourceSHA256, stamp.SourceSHA256)
	w.SetKeyValueMetadata(metaCodecVersion, strconv.Itoa(CodecVersion))
	w.SetKeyValueMetadata(metaRows, strconv.Itoa(len(rows)))

	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := readKlines(t.Context(), bytes.NewReader(buf.Bytes()), int64(buf.Len()), stamp)
	if !errors.Is(err, errCacheStale) {
		t.Fatalf("readKlines = %v, want an error wrapping errCacheStale", err)
	}
}
