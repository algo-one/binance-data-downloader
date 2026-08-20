package binancedata

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quagmt/udecimal"
)

// The fixtures in testdata/ are real archives from data.binance.vision, stored
// byte for byte as Binance served them — see testdata/README.md. Nothing here
// touches the network; that is a hard rule for this suite, and it is why the
// files are committed rather than fetched.

// dayRange is the half-open period covering one UTC day, which is what a daily
// archive holds. utc itself lives in request_test.go — one helper per package.
func dayRange(year int, month time.Month, d int) (time.Time, time.Time) {
	start := utc(year, month, d)

	return start, start.AddDate(0, 0, 1)
}

// msBefore and usBefore build the close time Binance actually writes: inclusive,
// one unit before the next candle opens. Which unit that is happens to be the
// whole subject of this stage, so the expectations below name it rather than
// spelling out nanoseconds and leaving the reader to count zeroes.
func msBefore(t time.Time) time.Time { return t.Add(-time.Millisecond) }

func usBefore(t time.Time) time.Time { return t.Add(-time.Microsecond) }

// readFixture loads one committed archive into memory and returns it in the
// shape archive/zip wants: something to read at offsets, plus a length.
func readFixture(t *testing.T, name string) (*bytes.Reader, int64) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	return bytes.NewReader(data), int64(len(data))
}

func TestDecodeArchiveAllFixtures(t *testing.T) {
	t.Parallel()

	janStart, janEnd := utc(2024, 1, 1), utc(2024, 2, 1)
	d15Start, d15End := dayRange(2024, 1, 15)
	nyeStart, nyeEnd := dayRange(2024, 12, 31)
	nydStart, nydEnd := dayRange(2025, 1, 1)
	m15Start, m15End := dayRange(2025, 1, 15)

	tests := []struct {
		name string
		file string
		spec decodeSpec

		wantRows       int
		wantFirstOpen  time.Time
		wantFirstClose time.Time
		wantLastOpen   time.Time
		wantLastClose  time.Time
		wantFirstPrice string
		wantQuoteVol   string
	}{
		{
			// A plain millisecond-era day, 24 hourly candles.
			name: "hourly milliseconds",
			file: "BTCUSDT-1h-2024-01-15.zip",
			spec: decodeSpec{Interval: Interval1h, Start: d15Start, End: d15End},

			wantRows:       24,
			wantFirstOpen:  utc(2024, 1, 15),
			wantFirstClose: msBefore(utc(2024, 1, 15, 1)),
			wantLastOpen:   utc(2024, 1, 15, 23),
			wantLastClose:  msBefore(utc(2024, 1, 16)),
			wantFirstPrice: "41732.35",
			wantQuoteVol:   "102456168.2255989",
		},
		{
			// The last day Binance wrote milliseconds. Its final candle closes
			// at .999 — three decimal places, not six.
			name: "last millisecond day",
			file: "BTCUSDT-1h-2024-12-31.zip",
			spec: decodeSpec{Interval: Interval1h, Start: nyeStart, End: nyeEnd},

			wantRows:       24,
			wantFirstOpen:  utc(2024, 12, 31),
			wantFirstClose: msBefore(utc(2024, 12, 31, 1)),
			wantLastOpen:   utc(2024, 12, 31, 23),
			wantLastClose:  msBefore(utc(2025, 1, 1)),
			wantFirstPrice: "92792.05",
			wantQuoteVol:   "54103943.143722",
		},
		{
			// The first day Binance wrote microseconds. Same fields, same
			// parser, six decimal places on the close.
			name: "first microsecond day",
			file: "BTCUSDT-1h-2025-01-01.zip",
			spec: decodeSpec{Interval: Interval1h, Start: nydStart, End: nydEnd},

			wantRows:       24,
			wantFirstOpen:  utc(2025, 1, 1),
			wantFirstClose: usBefore(utc(2025, 1, 1, 1)),
			wantLastOpen:   utc(2025, 1, 1, 23),
			wantLastClose:  usBefore(utc(2025, 1, 2)),
			wantFirstPrice: "93576",
			wantQuoteVol:   "71068810.5594638",
		},
		{
			// One monthly candle, and the reason this package does not use
			// float64: 60830257410.586478 needs 19 significant digits.
			name: "monthly candle",
			file: "BTCUSDT-1mo-2024-01.zip",
			spec: decodeSpec{Interval: Interval1mo, Start: janStart, End: janEnd},

			wantRows:       1,
			wantFirstOpen:  utc(2024, 1, 1),
			wantFirstClose: msBefore(utc(2024, 2, 1)),
			wantLastOpen:   utc(2024, 1, 1),
			wantLastClose:  msBefore(utc(2024, 2, 1)),
			wantFirstPrice: "42283.58",
			wantQuoteVol:   "60830257410.586478",
		},
		{
			// A full day of one-minute candles: the volume case, and what the
			// benchmark at the bottom of this file measures.
			name: "minute bulk",
			file: "BTCUSDT-1m-2025-01-15.zip",
			spec: decodeSpec{Interval: Interval1m, Start: m15Start, End: m15End},

			wantRows:       1440,
			wantFirstOpen:  utc(2025, 1, 15),
			wantFirstClose: usBefore(utc(2025, 1, 15, 0, 1)),
			wantLastOpen:   utc(2025, 1, 15, 23, 59),
			wantLastClose:  usBefore(utc(2025, 1, 16)),
			wantFirstPrice: "96560.85",
			wantQuoteVol:   "1463343.8317347",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r, size := readFixture(t, tc.file)

			klines, err := decodeArchiveAll(t.Context(), r, size, tc.spec)
			if err != nil {
				t.Fatalf("decodeArchiveAll() error = %v, want nil", err)
			}

			if got := len(klines); got != tc.wantRows {
				t.Fatalf("row count = %d, want %d", got, tc.wantRows)
			}

			first, last := klines[0], klines[len(klines)-1]

			if !first.OpenTime.Equal(tc.wantFirstOpen) {
				t.Errorf("first open time = %s, want %s", first.OpenTime, tc.wantFirstOpen)
			}

			if !first.CloseTime.Equal(tc.wantFirstClose) {
				t.Errorf("first close time = %s, want %s", first.CloseTime, tc.wantFirstClose)
			}

			if !last.OpenTime.Equal(tc.wantLastOpen) {
				t.Errorf("last open time = %s, want %s", last.OpenTime, tc.wantLastOpen)
			}

			if !last.CloseTime.Equal(tc.wantLastClose) {
				t.Errorf("last close time = %s, want %s", last.CloseTime, tc.wantLastClose)
			}

			// String() rather than a float comparison: the whole point of
			// udecimal is that the digits survive intact, so the assertion is
			// on the digits.
			if got := first.Open.String(); got != tc.wantFirstPrice {
				t.Errorf("first open price = %s, want %s", got, tc.wantFirstPrice)
			}

			if got := first.QuoteVolume.String(); got != tc.wantQuoteVol {
				t.Errorf("first quote volume = %s, want %s", got, tc.wantQuoteVol)
			}

			for i, k := range klines {
				if k.OpenTime.Location() != time.UTC {
					t.Fatalf("kline %d: open time location = %v, want UTC", i, k.OpenTime.Location())
				}

				if !aligned(k.OpenTime, tc.spec.Interval) {
					t.Fatalf("kline %d: open time %s is not on the %s grid", i, k.OpenTime, tc.spec.Interval)
				}
			}
		})
	}
}

// TestFixtureSeam is the millisecond-to-microsecond switch, read off two real
// archives: the last candle Binance published in milliseconds and the first it
// published in microseconds are adjacent, and the decoder has to make them meet.
func TestFixtureSeam(t *testing.T) {
	t.Parallel()

	nyeStart, nyeEnd := dayRange(2024, 12, 31)
	nydStart, nydEnd := dayRange(2025, 1, 1)

	r, size := readFixture(t, "BTCUSDT-1h-2024-12-31.zip")

	before, err := decodeArchiveAll(t.Context(), r, size, decodeSpec{Interval: Interval1h, Start: nyeStart, End: nyeEnd})
	if err != nil {
		t.Fatalf("decoding the millisecond day: %v", err)
	}

	r, size = readFixture(t, "BTCUSDT-1h-2025-01-01.zip")

	after, err := decodeArchiveAll(t.Context(), r, size, decodeSpec{Interval: Interval1h, Start: nydStart, End: nydEnd})
	if err != nil {
		t.Fatalf("decoding the microsecond day: %v", err)
	}

	lastBefore, firstAfter := before[len(before)-1], after[0]

	// The candles are adjacent: the last one closes one millisecond before the
	// next opens, and the next opens exactly at the year boundary.
	if !firstAfter.OpenTime.Equal(utc(2025, 1, 1)) {
		t.Errorf("first microsecond candle opens at %s, want the year boundary", firstAfter.OpenTime)
	}

	if !lastBefore.CloseTime.Before(firstAfter.OpenTime) {
		t.Errorf("last millisecond close %s is not before first microsecond open %s",
			lastBefore.CloseTime, firstAfter.OpenTime)
	}

	// Sub-second precision is where the two eras differ visibly, and it is the
	// evidence that each row was read in its own unit rather than one guess
	// being applied to both files.
	if got := lastBefore.CloseTime.Nanosecond(); got != int(999*time.Millisecond) {
		t.Errorf("millisecond-era close has %d ns, want 999ms", got)
	}

	if got := firstAfter.CloseTime.Nanosecond(); got != int(999999*time.Microsecond) {
		t.Errorf("microsecond-era close has %d ns, want 999999us", got)
	}
}

// baseFields is a well-formed row for 2024-12-31T00:00Z at the 1h interval,
// as twelve separate fields so a test can corrupt exactly one of them.
func baseFields() []string {
	return []string{
		"1735603200000", // open time, 2024-12-31T00:00:00Z in milliseconds
		"10.00000000",   // open
		"12.00000000",   // high
		"9.00000000",    // low
		"11.00000000",   // close
		"1.50000000",    // volume
		"1735606799999", // close time, one millisecond before 01:00
		"16.50000000",   // quote volume
		"7",             // trades
		"0.50000000",    // taker buy base volume
		"5.50000000",    // taker buy quote volume
		"0",             // ignore
	}
}

// withField returns the base row with one field replaced.
func withField(col int, value string) string {
	f := baseFields()
	f[col] = value

	return strings.Join(f, ",") + "\n"
}

// baseRow is the unmodified row as CSV text.
func baseRow() string {
	return strings.Join(baseFields(), ",") + "\n"
}

// baseSpec is the period and interval baseFields belongs to.
func baseSpec() decodeSpec {
	start, end := dayRange(2024, 12, 31)

	return decodeSpec{Interval: Interval1h, Start: start, End: end}
}

// decodeString runs the CSV decoder over a string, collecting the result.
func decodeString(t *testing.T, csv string, spec decodeSpec) ([]Kline, error) {
	t.Helper()

	return collectKlines(decodeCSV(t.Context(), strings.NewReader(csv), spec), 8)
}

// TestDecodeCSVMixedTimestampUnits is the regression test for the bug this
// stage exists to not repeat: the ported implementation read the unit from the
// last row of a file and applied it to every row. No real archive mixes units —
// the switch happened at 2025-01-01T00:00Z, which is both a day and a month
// boundary — so the mixed file has to be synthetic. The rule it proves is per
// row, which is what makes the real-world case safe for good.
func TestDecodeCSVMixedTimestampUnits(t *testing.T) {
	t.Parallel()

	// 00:00 written in milliseconds, 01:00 written in microseconds.
	csv := baseRow() +
		strings.Join([]string{
			"1735606800000000", "11.00000000", "13.00000000", "10.00000000", "12.00000000",
			"2.00000000", "1735610399999999", "24.00000000", "9", "1.00000000", "12.00000000", "0",
		}, ",") + "\n"

	klines, err := decodeString(t, csv, baseSpec())
	if err != nil {
		t.Fatalf("decode error = %v, want nil", err)
	}

	if len(klines) != 2 {
		t.Fatalf("row count = %d, want 2", len(klines))
	}

	if want := utc(2024, 12, 31); !klines[0].OpenTime.Equal(want) {
		t.Errorf("millisecond row opens at %s, want %s", klines[0].OpenTime, want)
	}

	if want := utc(2024, 12, 31, 1); !klines[1].OpenTime.Equal(want) {
		t.Errorf("microsecond row opens at %s, want %s", klines[1].OpenTime, want)
	}

	if got := klines[1].CloseTime.Nanosecond(); got != int(999999*time.Microsecond) {
		t.Errorf("microsecond row close has %d ns, want 999999us", got)
	}
}

// TestDecodeCSVHeaderHandling covers the second hardcoded assumption this stage
// removes. Spot archives carry no header and futures archives do; the decoder
// decides from the bytes in front of it.
func TestDecodeCSVHeaderHandling(t *testing.T) {
	t.Parallel()

	header := "open_time,open,high,low,close,volume,close_time,quote_volume,count," +
		"taker_buy_volume,taker_buy_quote_volume,ignore\n"

	tests := []struct {
		name string
		csv  string
		want int
	}{
		{name: "no header, as spot publishes", csv: baseRow(), want: 1},
		{name: "header, as futures publishes", csv: header + baseRow(), want: 1},
		{name: "header and no data rows", csv: header, want: 0},
		{name: "empty file", csv: "", want: 0},

		// A byte-order mark on the first field would make a real data row look
		// like a header, costing exactly one candle in silence.
		{name: "byte-order mark", csv: "\ufeff" + baseRow(), want: 1},

		// The documented ambiguity: a first row where nothing at all parses is
		// read as a header. A row corrupt in every field is not something
		// Binance publishes, while a file of column names is.
		{
			name: "first row unparseable throughout",
			csv:  strings.Join(strings.Split(strings.Repeat("x,", 11)+"x", ","), ",") + "\n" + baseRow(),
			want: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			klines, err := decodeString(t, tc.csv, baseSpec())
			if err != nil {
				t.Fatalf("decode error = %v, want nil", err)
			}

			if len(klines) != tc.want {
				t.Errorf("row count = %d, want %d", len(klines), tc.want)
			}
		})
	}
}

// TestDecodeCSVRejects is the table of things that must not decode quietly.
// Every one of them wraps ErrCorruptArchive: by this point the bytes have
// already passed transport and checksum checks, so anything still wrong with
// them is in the content.
func TestDecodeCSVRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		csv  string
	}{
		{
			name: "too few fields",
			csv:  strings.Join(baseFields()[:11], ",") + "\n",
		},
		{
			name: "too many fields",
			csv:  strings.Join(append(baseFields(), "extra"), ",") + "\n",
		},
		{
			name: "price that is not a number",
			csv:  withField(colOpen, "n/a"),
		},
		{
			name: "timestamp that is not a number",
			csv:  withField(colOpenTime, "2024-12-31T00:00:00Z"),
		},
		{
			name: "timestamp of zero",
			csv:  withField(colOpenTime, "0"),
		},
		{
			// The failure mode the period check exists for: this is the right
			// day read in the wrong unit, which lands in 1970.
			name: "open time before the period",
			csv:  withField(colOpenTime, "1735603200"),
		},
		{
			name: "open time after the period",
			csv:  withField(colOpenTime, "1735689600000"),
		},
		{
			name: "open time off the interval grid",
			csv:  withField(colOpenTime, "1735605000000"),
		},
		{
			// Off the grid by 500 microseconds, written in the microsecond
			// spelling. A millisecond-granularity grid test cannot see this,
			// and since 2025 every archive is written in microseconds.
			name: "open time off the grid by less than a millisecond",
			csv:  withField(colOpenTime, "1735603200000500"),
		},
		{
			name: "close time before open time",
			csv:  withField(colCloseTime, "1735599600000"),
		},
		{
			name: "close time beyond the interval",
			csv:  withField(colCloseTime, "1735610400000"),
		},
		{
			name: "low above high",
			csv:  withField(colLow, "13.00000000"),
		},
		{
			name: "negative volume",
			csv:  withField(colVolume, "-1.50000000"),
		},
		{
			name: "negative quote volume",
			csv:  withField(colQuoteVolume, "-16.50000000"),
		},
		{
			// All four prices negative satisfies low <= open,close <= high,
			// so the ordering rule alone would let this through.
			name: "negative prices",
			csv: strings.Join([]string{
				"1735603200000", "-11.00000000", "-9.00000000", "-12.00000000", "-10.00000000",
				"1.50000000", "1735606799999", "16.50000000", "7", "0.50000000", "5.50000000", "0",
			}, ",") + "\n",
		},
		{
			name: "taker buy base volume exceeding volume",
			csv:  withField(colTakerBuyBaseVolume, "1.50000001"),
		},
		{
			name: "taker buy quote volume exceeding quote volume",
			csv:  withField(colTakerBuyQuoteVolume, "16.50000001"),
		},
		{
			name: "high below close",
			csv:  withField(colHigh, "10.50000000"),
		},
		{
			name: "trades that are not an integer",
			csv:  withField(colTrades, "7.5"),
		},
		{
			name: "negative trades",
			csv:  withField(colTrades, "-1"),
		},
		{
			name: "duplicate open time",
			csv:  baseRow() + baseRow(),
		},
		{
			name: "open times going backwards",
			csv: strings.Join([]string{
				"1735606800000", "10.00000000", "12.00000000", "9.00000000", "11.00000000",
				"1.50000000", "1735610399999", "16.50000000", "7", "0.50000000", "5.50000000", "0",
			}, ",") + "\n" + baseRow(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeString(t, tc.csv, baseSpec())
			if !errors.Is(err, ErrCorruptArchive) {
				t.Errorf("error = %v, want one wrapping ErrCorruptArchive", err)
			}
		})
	}
}

// TestDecodeCSVRejectsBadSpec covers the caller's own mistakes, which are a
// different sentinel: nothing is wrong with the data when the request for it is
// malformed.
func TestDecodeCSVRejectsBadSpec(t *testing.T) {
	t.Parallel()

	start, end := dayRange(2024, 12, 31)

	tests := []struct {
		name string
		spec decodeSpec
	}{
		{name: "zero interval", spec: decodeSpec{Start: start, End: end}},
		{name: "zero start", spec: decodeSpec{Interval: Interval1h, End: end}},
		{name: "zero end", spec: decodeSpec{Interval: Interval1h, Start: start}},
		{name: "end before start", spec: decodeSpec{Interval: Interval1h, Start: end, End: start}},
		{name: "empty period", spec: decodeSpec{Interval: Interval1h, Start: start, End: start}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeString(t, baseRow(), tc.spec)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error = %v, want one wrapping ErrInvalidRequest", err)
			}
		})
	}
}

// zipWith builds an archive in memory from name/content pairs, for the failure
// modes no real archive exhibits.
func zipWith(t *testing.T, members map[string]string) (*bytes.Reader, int64) {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, content := range members {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("creating zip member: %v", err)
		}

		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("writing zip member: %v", err)
		}
	}

	if err := zw.Close(); err != nil {
		t.Fatalf("closing zip: %v", err)
	}

	return bytes.NewReader(buf.Bytes()), int64(buf.Len())
}

func TestDecodeArchiveRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		members map[string]string
		raw     []byte
	}{
		{
			name: "no CSV member",
			members: map[string]string{
				"BTCUSDT-1h-2024-12-31.txt": baseRow(),
			},
		},
		{
			name: "two CSV members",
			members: map[string]string{
				"BTCUSDT-1h-2024-12-31.csv": baseRow(),
				"extra.csv":                 baseRow(),
			},
		},
		{
			name:    "not a zip at all",
			raw:     []byte("this is not a zip file, it is a sentence"),
			members: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				r    *bytes.Reader
				size int64
			)

			if tc.raw != nil {
				r, size = bytes.NewReader(tc.raw), int64(len(tc.raw))
			} else {
				r, size = zipWith(t, tc.members)
			}

			_, err := decodeArchiveAll(t.Context(), r, size, baseSpec())
			if !errors.Is(err, ErrCorruptArchive) {
				t.Errorf("error = %v, want one wrapping ErrCorruptArchive", err)
			}
		})
	}
}

// TestDecodeArchiveAcceptsSingleCSV confirms the member search is by suffix and
// not by name, since a futures or aggTrades archive names its member
// differently and this is the extension point that has to keep working.
func TestDecodeArchiveAcceptsSingleCSV(t *testing.T) {
	t.Parallel()

	r, size := zipWith(t, map[string]string{"something-else-entirely.CSV": baseRow()})

	klines, err := decodeArchiveAll(t.Context(), r, size, baseSpec())
	if err != nil {
		t.Fatalf("decodeArchiveAll() error = %v, want nil", err)
	}

	if len(klines) != 1 {
		t.Errorf("row count = %d, want 1", len(klines))
	}
}

// TestDecodeArchiveEarlyStop is the property that makes the iterator worth
// having: abandoning the loop stops the work, rather than decoding the rest of
// the file into a slice nobody asked for.
func TestDecodeArchiveEarlyStop(t *testing.T) {
	t.Parallel()

	start, end := dayRange(2025, 1, 15)
	r, size := readFixture(t, "BTCUSDT-1m-2025-01-15.zip")

	var seen int

	for _, err := range decodeArchive(t.Context(), r, size, decodeSpec{Interval: Interval1m, Start: start, End: end}) {
		if err != nil {
			t.Fatalf("decode error = %v, want nil", err)
		}

		seen++
		if seen == 3 {
			break
		}
	}

	if seen != 3 {
		t.Errorf("consumed %d rows, want 3", seen)
	}
}

// TestDecodeStopsOnCancelledContext covers the reason the decoder takes a
// context at all. A caller ranging over the iterator can stop it with break,
// but decodeArchiveAll's loop belongs to the library, and a 1s month is several
// seconds of work that a cancelled backtest should not have to sit through.
func TestDecodeStopsOnCancelledContext(t *testing.T) {
	t.Parallel()

	start, end := dayRange(2025, 1, 15)
	r, size := readFixture(t, "BTCUSDT-1m-2025-01-15.zip")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := decodeArchiveAll(ctx, r, size, decodeSpec{Interval: Interval1m, Start: start, End: end})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want one wrapping context.Canceled", err)
	}

	// A cancellation is not a claim about the bytes, so it must not arrive
	// wearing the sentinel that means "these bytes are damaged" — a caller
	// branching on that would discard a perfectly good cached archive.
	if errors.Is(err, ErrCorruptArchive) {
		t.Errorf("error = %v, want it not to wrap ErrCorruptArchive", err)
	}
}

// TestDecodeRunsToCompletionWithLiveContext is the other half: an uncancelled
// context must not cost anything at the stride boundary.
func TestDecodeRunsToCompletionWithLiveContext(t *testing.T) {
	t.Parallel()

	start, end := dayRange(2025, 1, 15)
	r, size := readFixture(t, "BTCUSDT-1m-2025-01-15.zip")

	klines, err := decodeArchiveAll(t.Context(), r, size, decodeSpec{Interval: Interval1m, Start: start, End: end})
	if err != nil {
		t.Fatalf("decodeArchiveAll() error = %v, want nil", err)
	}

	// 1440 rows crosses the 1024-row context-check stride, which is the point.
	if len(klines) != 1440 {
		t.Errorf("row count = %d, want 1440", len(klines))
	}
}

// TestCollectKlinesStopsAtError checks that a failure part-way through does not
// come back as a partial slice with a non-nil error beside it — the shape that
// invites a caller to use the data anyway.
func TestCollectKlinesStopsAtError(t *testing.T) {
	t.Parallel()

	csv := baseRow() + withField(colOpen, "not a price")

	klines, err := decodeString(t, csv, baseSpec())
	if !errors.Is(err, ErrCorruptArchive) {
		t.Fatalf("error = %v, want one wrapping ErrCorruptArchive", err)
	}

	if klines != nil {
		t.Errorf("klines = %v, want nil alongside an error", klines)
	}
}

func TestAligned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval Interval
		t        time.Time
		want     bool
	}{
		{name: "1m on the minute", interval: Interval1m, t: utc(2024, 3, 4, 12, 34), want: true},
		{name: "1m with seconds", interval: Interval1m, t: utc(2024, 3, 4, 12, 34, 30), want: false},
		{name: "1h on the hour", interval: Interval1h, t: utc(2024, 3, 4, 12), want: true},
		{
			// The data has been written in microseconds since 2025, so the
			// grid test has to be at least that fine.
			name: "1h off by 500 microseconds", interval: Interval1h,
			t: utc(2025, 1, 15, 3).Add(500 * time.Microsecond), want: false,
		},
		{
			name: "1h off by a single microsecond", interval: Interval1h,
			t: utc(2025, 1, 15, 3).Add(time.Microsecond), want: false,
		},
		{name: "1h half past", interval: Interval1h, t: utc(2024, 3, 4, 12, 30), want: false},
		{name: "4h on the grid", interval: Interval4h, t: utc(2024, 3, 4, 8), want: true},
		{name: "4h off the grid", interval: Interval4h, t: utc(2024, 3, 4, 9), want: false},
		{name: "1d at midnight", interval: Interval1d, t: utc(2024, 3, 4), want: true},
		{name: "1d at noon", interval: Interval1d, t: utc(2024, 3, 4, 12), want: false},

		// 3d sits on a grid anchored at 1970-01-02, measured against the live
		// archives rather than assumed. 2024-03-01 and 2024-04-03 are real
		// consecutive-archive candles; the grid does not restart each month.
		{name: "3d on the grid", interval: Interval3d, t: utc(2024, 3, 1), want: true},
		{name: "3d across a month boundary", interval: Interval3d, t: utc(2024, 4, 3), want: true},
		{name: "3d one day off", interval: Interval3d, t: utc(2024, 3, 2), want: false},

		// 1w opens on Monday. The epoch was a Thursday, so a "multiple of
		// seven days" test would reject every real weekly candle.
		{name: "1w on Monday", interval: Interval1w, t: utc(2024, 3, 4), want: true},
		{name: "1w on Thursday", interval: Interval1w, t: utc(2024, 3, 7), want: false},
		{name: "1w on the epoch itself", interval: Interval1w, t: utc(1970, 1, 1), want: false},

		{name: "1mo on the first", interval: Interval1mo, t: utc(2024, 3, 1), want: true},
		{name: "1mo on the second", interval: Interval1mo, t: utc(2024, 3, 2), want: false},
		{name: "1mo at noon on the first", interval: Interval1mo, t: utc(2024, 3, 1, 12), want: false},

		{name: "invalid interval", interval: Interval(0), t: utc(2024, 3, 4), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := aligned(tc.t, tc.interval); got != tc.want {
				t.Errorf("aligned(%s, %s) = %v, want %v", tc.t, tc.interval, got, tc.want)
			}
		})
	}
}

func TestIntervalEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		interval Interval
		from     time.Time
		want     time.Time
	}{
		{
			name: "an hour", interval: Interval1h,
			from: utc(2024, 3, 4, 12), want: utc(2024, 3, 4, 13),
		},
		{
			// February 2024 is 29 days, which is why this cannot be a duration.
			name: "a leap February", interval: Interval1mo,
			from: utc(2024, 2, 1), want: utc(2024, 3, 1),
		},
		{
			name: "a 31-day month", interval: Interval1mo,
			from: utc(2024, 1, 1), want: utc(2024, 2, 1),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := intervalEnd(tc.from, tc.interval); !got.Equal(tc.want) {
				t.Errorf("intervalEnd(%s, %s) = %s, want %s", tc.from, tc.interval, got, tc.want)
			}
		})
	}
}

func TestEstimateRows(t *testing.T) {
	t.Parallel()

	janStart, janEnd := utc(2024, 1, 1), utc(2024, 2, 1)
	dayStart, dayEnd := dayRange(2024, 1, 15)

	tests := []struct {
		name string
		spec decodeSpec
		want int
	}{
		{name: "a day of hours", spec: decodeSpec{Interval: Interval1h, Start: dayStart, End: dayEnd}, want: 24},
		{name: "a day of minutes", spec: decodeSpec{Interval: Interval1m, Start: dayStart, End: dayEnd}, want: 1440},
		{name: "a month of hours", spec: decodeSpec{Interval: Interval1h, Start: janStart, End: janEnd}, want: 744},
		{name: "a monthly candle", spec: decodeSpec{Interval: Interval1mo, Start: janStart, End: janEnd}, want: 1},
		{
			// A month of 1s candles is 2.6 million rows; the hint is capped so
			// that a capacity is never reserved on arithmetic alone.
			name: "capped", spec: decodeSpec{Interval: Interval1s, Start: janStart, End: janEnd},
			want: maxRowsHint,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := tc.spec.estimateRows(); got != tc.want {
				t.Errorf("estimateRows() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestDecodeAcceptsPartialPeriod pins the asymmetry in the period check: every
// candle must be inside the file's period, but the period need not be full.
// SHIBUSDT's 2021-05 daily archive holds 22 rows for a 31-day month because the
// pair was listed on the 10th, and rejecting that would reject real data.
func TestDecodeAcceptsPartialPeriod(t *testing.T) {
	t.Parallel()

	// One candle at 00:00 for a day that has room for 24.
	klines, err := decodeString(t, baseRow(), baseSpec())
	if err != nil {
		t.Fatalf("decode error = %v, want nil", err)
	}

	if len(klines) != 1 {
		t.Errorf("row count = %d, want 1", len(klines))
	}
}

// TestKlineValuesAreExact is the assertion the whole udecimal decision exists
// for: the digits Binance published are the digits that come back, including
// the nineteen-significant-digit quote volume that float64 cannot hold.
func TestKlineValuesAreExact(t *testing.T) {
	t.Parallel()

	start, end := utc(2024, 1, 1), utc(2024, 2, 1)
	r, size := readFixture(t, "BTCUSDT-1mo-2024-01.zip")

	klines, err := decodeArchiveAll(t.Context(), r, size, decodeSpec{Interval: Interval1mo, Start: start, End: end})
	if err != nil {
		t.Fatalf("decodeArchiveAll() error = %v, want nil", err)
	}

	want := Kline{
		OpenTime:            utc(2024, 1, 1),
		CloseTime:           msBefore(utc(2024, 2, 1)),
		Open:                udecimal.MustParse("42283.58000000"),
		High:                udecimal.MustParse("48969.48000000"),
		Low:                 udecimal.MustParse("38555.00000000"),
		Close:               udecimal.MustParse("42580.00000000"),
		Volume:              udecimal.MustParse("1403408.84978000"),
		QuoteVolume:         udecimal.MustParse("60830257410.58647800"),
		TakerBuyBaseVolume:  udecimal.MustParse("707223.37514000"),
		TakerBuyQuoteVolume: udecimal.MustParse("30663861045.60908220"),
		Trades:              52549865,
	}

	if !klines[0].Equal(want) {
		t.Errorf("kline =\n%+v\nwant\n%+v", klines[0], want)
	}
}

// BenchmarkDecodeArchiveAll measures the cost the two-tier cache exists to
// avoid. The fixture is a day of one-minute candles (1,440 rows); a month is
// 44,640, so multiply by 31 for the figure quoted in docs/caching.md.
func BenchmarkDecodeArchiveAll(b *testing.B) {
	start := utc(2025, 1, 15)
	spec := decodeSpec{Interval: Interval1m, Start: start, End: start.AddDate(0, 0, 1)}

	data, err := os.ReadFile(filepath.Join("testdata", "BTCUSDT-1m-2025-01-15.zip"))
	if err != nil {
		b.Fatalf("reading fixture: %v", err)
	}

	r := bytes.NewReader(data)
	size := int64(len(data))

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		klines, err := decodeArchiveAll(b.Context(), r, size, spec)
		if err != nil {
			b.Fatalf("decode error: %v", err)
		}

		if len(klines) != 1440 {
			b.Fatalf("row count = %d, want 1440", len(klines))
		}
	}
}

// TestAlignUpAgreesWithAligned checks the two halves of the grid against each
// other rather than against a list written by hand.
//
// [aligned] tests whether an instant sits on the grid; [alignUp] computes the
// next instant that does. Two functions that disagree about where the grid
// lines fall would be a bug nobody would spot — a candle silently treated as
// missing, or a span silently excused for holding none — so the test states the
// relationship instead of enumerating answers:
//
//   - alignUp(t) is on the grid
//   - alignUp(t) is not before t
//   - nothing between t and alignUp(t) is on the grid
//
// The instants swept are deliberately awkward: a leap day, a month boundary,
// the ms→µs switch, a Sunday, and several offsets by a prime number of seconds
// so that nothing lands on a grid line by accident.
func TestAlignUpAgreesWithAligned(t *testing.T) {
	t.Parallel()

	intervals := []Interval{
		Interval1s, Interval1m, Interval3m, Interval5m, Interval15m, Interval30m,
		Interval1h, Interval2h, Interval4h, Interval6h, Interval8h, Interval12h,
		Interval1d, Interval3d, Interval1w, Interval1mo,
	}

	instants := []time.Time{
		utc(2024, 1, 1),
		utc(2024, 2, 29, 13, 47, 3),
		utc(2024, 3, 1),
		utc(2024, 12, 31, 23, 59, 59),
		utc(2025, 1, 1),
		utc(2025, 6, 15, 7, 11, 13),
		utc(2021, 6, 6, 0, 0, 1), // a Sunday, one second in
		utc(2018, 1, 17, 5, 0, 0),
	}

	for _, iv := range intervals {
		for _, when := range instants {
			got, ok := alignUp(when, iv)
			if !ok {
				t.Errorf("%s: alignUp(%s) reported no grid", iv, when.Format(time.RFC3339))

				continue
			}

			if !aligned(got, iv) {
				t.Errorf("%s: alignUp(%s) = %s, which is not on the grid",
					iv, when.Format(time.RFC3339), got.Format(time.RFC3339))
			}

			if got.Before(when) {
				t.Errorf("%s: alignUp(%s) = %s, which is earlier than its input",
					iv, when.Format(time.RFC3339), got.Format(time.RFC3339))
			}

			// Nothing strictly between the input and the answer may be on the
			// grid, or alignUp skipped a candle — and "overshot by exactly one
			// period" is the mistake most likely to survive the two checks
			// above, since it lands on the grid and is not before the input.
			//
			// Checked by stepping back one grid point rather than by walking
			// forward. Walking is what this did first, one second at a time,
			// which is only affordable for intervals of a minute or less — so
			// fourteen of the sixteen were never checked for it at all. The
			// previous grid point must be strictly earlier than the input; that
			// is the same statement, exactly, and it costs one subtraction.
			prev := previousGridPoint(got, iv)

			if !prev.Before(when) {
				t.Errorf("%s: alignUp(%s) = %s overshot — the grid point at %s is also at or after the input",
					iv, when.Format(time.RFC3339), got.Format(time.RFC3339), prev.Format(time.RFC3339))
			}

			if !aligned(prev, iv) {
				t.Errorf("%s: the test's own previous grid point %s is not on the grid",
					iv, prev.Format(time.RFC3339))
			}
		}
	}
}

// TestAlignUpIsIdempotentOnTheGrid: an instant already on the grid is its own
// answer. Rounding it up to the *next* one would make expectsCandles treat the
// first candle of every archive as absent.
func TestAlignUpIsIdempotentOnTheGrid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		iv   Interval
		when time.Time
	}{
		{Interval1h, utc(2024, 1, 15, 13)},
		{Interval1d, utc(2024, 1, 15)},
		{Interval1mo, utc(2024, 2, 1)},
		{Interval1w, utc(2024, 1, 15)}, // a Monday
		{Interval3d, utc(2024, 3, 31)}, // measured against the live 3d grid
		{Interval1s, utc(2025, 1, 1, 0, 0, 7)},
	}

	for _, tt := range tests {
		if !aligned(tt.when, tt.iv) {
			t.Fatalf("%s: the test's own instant %s is not on the grid",
				tt.iv, tt.when.Format(time.RFC3339))
		}

		got, ok := alignUp(tt.when, tt.iv)
		if !ok {
			t.Errorf("%s: alignUp reported no grid", tt.iv)

			continue
		}

		if !got.Equal(tt.when) {
			t.Errorf("%s: alignUp(%s) = %s, want the input unchanged",
				tt.iv, tt.when.Format(time.RFC3339), got.Format(time.RFC3339))
		}
	}
}

// TestAlignUpHasNoGridForAnUnsetInterval mirrors [aligned]: no interval, no
// grid, and the caller is told so rather than handed a plausible instant.
func TestAlignUpHasNoGridForAnUnsetInterval(t *testing.T) {
	t.Parallel()

	if got, ok := alignUp(utc(2024, 1, 15), Interval(0)); ok {
		t.Errorf("alignUp with no interval returned %s and ok", got.Format(time.RFC3339))
	}
}

// previousGridPoint returns the candle open time immediately before t, which
// must itself be on the grid.
//
// It exists so that [TestAlignUpAgreesWithAligned] can state minimality as one
// comparison for every interval, rather than only for those short enough to
// walk. 1mo is the one that cannot be done by subtracting a duration, for the
// same reason [intervalEnd] special-cases it: months have no fixed length.
func previousGridPoint(t time.Time, iv Interval) time.Time {
	if d, fixed := iv.Duration(); fixed {
		return t.Add(-d)
	}

	return t.AddDate(0, -1, 0)
}
