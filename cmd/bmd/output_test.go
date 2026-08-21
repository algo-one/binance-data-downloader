package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// update regenerates the golden files instead of comparing against them:
//
//	go test ./cmd/bmd -update
//
// Golden files are worth the small ceremony here because the thing under test
// is an exact document. A test that asserted "the output contains open_time"
// would pass on output with the columns in the wrong order, a missing row, or a
// price rounded through a float64. A byte comparison against a file somebody
// read once cannot.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// goldenDir is the absolute path of testdata, resolved once at start-up.
//
// Absolute rather than relative, and resolved here rather than at the point of
// use, because several tests call t.Chdir to exercise the "no -out" default —
// which writes into the working directory. A relative "testdata" would then
// resolve inside whichever temporary directory the test had moved to. Package
// variables are initialised before any test runs, and `go test` starts in the
// package's own directory, so this is the one moment the relative path is
// guaranteed to mean what it says.
var goldenDir = func() string {
	wd, err := os.Getwd()
	if err != nil {
		panic("locating the package directory: " + err.Error())
	}

	return filepath.Join(wd, "testdata")
}()

// goldenPath is where a golden file for the given name lives.
func goldenPath(name string) string { return filepath.Join(goldenDir, name) }

// checkGolden compares got with the golden file, or rewrites it under -update.
func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()

	path := goldenPath(name)

	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating testdata: %v", err)
		}

		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}

		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s (run `go test ./cmd/bmd -update` to create it): %v", path, err)
	}

	if !bytes.Equal(got, want) {
		t.Errorf("output does not match %s\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

// seqOf turns a slice of candles into the iterator the encoders consume.
func seqOf(klines []binancedata.Kline) iter.Seq2[binancedata.Kline, error] {
	return func(yield func(binancedata.Kline, error) bool) {
		for _, k := range klines {
			if !yield(k, nil) {
				return
			}
		}
	}
}

// TestEncodersMatchTheirGoldenFiles pins the exact bytes of the text formats.
//
// The candles carry a twenty-digit quote volume, so a comparison against these
// files is also a running check that nothing in the encoding path goes through
// a float64 — the digits would change, and this would say so.
func TestEncodersMatchTheirGoldenFiles(t *testing.T) {
	t.Parallel()

	klines := testKlines(t, 3)

	tests := []struct {
		name   string
		format string
		golden string
	}{
		{name: "csv", format: formatCSV, golden: "download.csv"},
		{name: "json", format: formatJSON, golden: "download.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encode, err := encoderFor(t.Context(), tt.format)
			if err != nil {
				t.Fatalf("encoderFor: %v", err)
			}

			var buf bytes.Buffer

			rows, err := encode(&buf, seqOf(klines))
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			if rows != len(klines) {
				t.Errorf("encoded %d rows, want %d", rows, len(klines))
			}

			checkGolden(t, tt.golden, buf.Bytes())
		})
	}
}

// TestCSVKeepsEveryDigit is the claim the golden file makes, stated on its own
// so that a future reader does not have to diff two files to see it.
//
// 118661604939.99255335 is the widest quote volume measured across 1,751,352
// real values, and is why this project uses udecimal rather than float64. A
// round trip through a float64 turns it into 118661604939.99255 — five digits
// short — and every other assertion in this file would still pass.
func TestCSVKeepsEveryDigit(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	if _, err := writeCSV(&buf, seqOf(testKlines(t, 1))); err != nil {
		t.Fatalf("writeCSV: %v", err)
	}

	if !strings.Contains(buf.String(), "118661604939.99255335") {
		t.Errorf("the quote volume lost digits on the way out:\n%s", buf.String())
	}
}

// TestParquetOutputIsReadable checks the third format the only way that means
// anything: by reading it back with a reader that knows nothing about this
// project, which is what a user pointing DuckDB or pandas at the file is doing.
//
// It has no golden file, because a parquet file is not a document to be read by
// eye and a byte comparison would fail on every parquet-go upgrade for reasons
// having nothing to do with this code. The library's own tests already pin the
// format's reproducibility.
func TestParquetOutputIsReadable(t *testing.T) {
	t.Parallel()

	klines := testKlines(t, 3)

	encode, err := encoderFor(t.Context(), formatParquet)
	if err != nil {
		t.Fatalf("encoderFor: %v", err)
	}

	var buf bytes.Buffer

	rows, err := encode(&buf, seqOf(klines))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if rows != len(klines) {
		t.Errorf("encoded %d rows, want %d", rows, len(klines))
	}

	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("parquet.OpenFile: %v", err)
	}

	if got := f.NumRows(); got != int64(len(klines)) {
		t.Errorf("the file holds %d rows, want %d", got, len(klines))
	}

	// The column names are the same eleven as the CSV header, in the same
	// order. Three formats that agree by coincidence eventually stop agreeing.
	var names []string
	for _, field := range f.Schema().Fields() {
		names = append(names, field.Name())
	}

	if strings.Join(names, ",") != strings.Join(columns, ",") {
		t.Errorf("parquet columns = %v\nCSV columns     = %v", names, columns)
	}
}

// TestEncoderForRejectsAnUnknownFormat keeps a typo in -format from reaching
// the network. It is a usage error, so the process exits 2 rather than 1.
func TestEncoderForRejectsAnUnknownFormat(t *testing.T) {
	t.Parallel()

	_, err := encoderFor(t.Context(), "xlsx")
	if !errors.Is(err, errUsage) {
		t.Errorf("error = %v, want it to wrap errUsage", err)
	}

	if err == nil || !strings.Contains(err.Error(), "csv") {
		t.Errorf("error = %v, want it to list the formats that do work", err)
	}
}

// TestEncodersPropagateAFailedStream is the property that decides whether a
// half-finished download can look like a complete one.
//
// Loader.Stream reports a failure by yielding an error rather than returning
// one, so an encoder that ignored the second value would write the candles it
// had already received, return no error, and produce a short file that nothing
// downstream could tell from a whole one.
func TestEncodersPropagateAFailedStream(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")

	seq := func(yield func(binancedata.Kline, error) bool) {
		for _, k := range testKlines(t, 2) {
			if !yield(k, nil) {
				return
			}
		}

		yield(binancedata.Kline{}, boom)
	}

	for _, format := range []string{formatCSV, formatJSON, formatParquet} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()

			encode, err := encoderFor(t.Context(), format)
			if err != nil {
				t.Fatalf("encoderFor: %v", err)
			}

			var buf bytes.Buffer

			rows, err := encode(&buf, seq)
			if !errors.Is(err, boom) {
				t.Errorf("error = %v, want it to wrap boom", err)
			}

			if rows != 2 {
				t.Errorf("reported %d rows written before the failure, want 2", rows)
			}
		})
	}
}

// TestResolveDestination covers the four spellings of -out.
func TestResolveDestination(t *testing.T) {
	dir := t.TempDir()

	// t.Chdir makes the "no -out" case testable without racing another test
	// over the process's working directory: it restores the old one on
	// cleanup and refuses to run in a parallel test.
	t.Chdir(dir)

	tests := []struct {
		name string
		out  string
		want string
	}{
		{name: "a dash is stdout", out: "-", want: ""},
		{name: "no flag generates a name here", out: "", want: filepath.Join(dir, "generated.csv")},
		{name: "an existing directory takes the name", out: dir, want: filepath.Join(dir, "generated.csv")},
		{name: "anything else is the file", out: "elsewhere.csv", want: "elsewhere.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveDestination(tt.out, "generated.csv")
			if err != nil {
				t.Fatalf("resolveDestination: %v", err)
			}

			if got.path != tt.want {
				t.Errorf("path = %q, want %q", got.path, tt.want)
			}
		})
	}
}

// TestOutputName pins the generated file name, which is a small contract: it is
// what a second run overwrites and what a directory of downloads sorts by.
func TestOutputName(t *testing.T) {
	t.Parallel()

	req := binancedata.Request{
		Symbol:   "BTCUSDT",
		Interval: binancedata.Interval1h,
		Start:    mustDate(t, "2024-01-01"),
		End:      mustDate(t, "2024-03-31"),
	}

	if got, want := outputName(req, formatCSV), "BTCUSDT-1h-2024-01-01_2024-03-31.csv"; got != want {
		t.Errorf("outputName = %q, want %q", got, want)
	}
}

// TestWriteFileAtomicallyLeavesNothingBehindOnFailure is the reason output goes
// through a temporary file at all.
//
// A download that fails halfway must not leave a file that looks finished. The
// stakes differ by format and both are bad: a truncated CSV is silently short,
// and a parquet file whose footer was never written is not a parquet file.
func TestWriteFileAtomicallyLeavesNothingBehindOnFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.csv")

	boom := errors.New("boom")

	err := writeFileAtomically(path, func(w io.Writer) error {
		if _, err := io.WriteString(w, "a partial file\n"); err != nil {
			return err
		}

		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the destination exists after a failed write; err = %v", err)
	}

	// And no temporary file was left in the directory either.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the directory: %v", err)
	}

	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}

		t.Errorf("the directory holds %v, want nothing", names)
	}
}

// TestWriteFileAtomicallyReplacesAnExistingFile covers the case the documented
// smoke test depends on: running the same download twice must work, and the
// second run must not append to or half-overwrite the first.
func TestWriteFileAtomicallyReplacesAnExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "out.csv")

	if err := os.WriteFile(path, []byte("a much longer previous file\n"), 0o644); err != nil {
		t.Fatalf("seeding the file: %v", err)
	}

	write := func(w io.Writer) error {
		_, err := io.WriteString(w, "new\n")

		return err
	}

	if err := writeFileAtomically(path, write); err != nil {
		t.Fatalf("writeFileAtomically: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}

	if string(got) != "new\n" {
		t.Errorf("file = %q, want it fully replaced with %q", got, "new\n")
	}
}

// TestIsTerminal checks the detection used for two decisions: whether progress
// redraws a line, and whether parquet is allowed onto stdout. A regular file is
// the case it must get right — the CI transcript and the redirected download
// are both one.
func TestIsTerminal(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp(t.TempDir(), "not-a-tty")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	defer func() { _ = f.Close() }()

	if isTerminal(f) {
		t.Error("a regular file was reported as a terminal")
	}

	if isTerminal(&bytes.Buffer{}) {
		t.Error("a bytes.Buffer was reported as a terminal")
	}
}
