package main

// Where candles go and what they look like when they get there.
//
// The three encoders share one shape — an io.Writer, an iterator of candles,
// and a count of what was written — because that is what lets download stream a
// range larger than memory into any of them without knowing which one it has.

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"time"

	binancedata "github.com/algo-one/binance-data-downloader"
)

// The values -format accepts.
const (
	formatCSV     = "csv"
	formatJSON    = "json"
	formatParquet = "parquet"
)

// outputFilePerm is what a downloaded file ends up as.
//
// os.CreateTemp makes its file readable only by the owner, which is right for a
// temporary file and wrong for output somebody asked for: market data from a
// public bucket is not a secret, and a CSV that the tool you were going to load
// it with cannot read is not an output. The cache uses the same value for the
// same reason — see cacheFilePerm.
//
// It is applied with os.Chmod, which sets the mode outright, so — unlike a
// shell redirect, and unlike the mode you pass to os.OpenFile — **the process
// umask does not filter it**. Under `umask 077`, where every other file you
// create lands 0600, a downloaded file is still 0644. That is worth knowing
// before assuming otherwise, and it is the state of things rather than an
// oversight: writing atomically means creating a temporary file, os.CreateTemp
// hardcodes 0600, and Go has no portable way to read the umask back — syscall.
// Umask does not exist on Windows and is process-global, so consulting it means
// setting it, which races every other goroutine creating a file.
const outputFilePerm fs.FileMode = 0o644

// stdoutPath is the -out value meaning "write to standard output". It is the
// Unix convention, and it is a value rather than a separate -stdout flag so
// that the destination is always exactly one thing.
const stdoutPath = "-"

// columns is the CSV header, and it is deliberately the same eleven names in
// the same order as the Kline json tags and the parquet schema. A caller who
// converts between the three formats should never have to rename a column, and
// three orders that agree by coincidence eventually stop agreeing.
var columns = []string{
	"open_time",
	"close_time",
	"open",
	"high",
	"low",
	"close",
	"volume",
	"quote_volume",
	"taker_buy_base_volume",
	"taker_buy_quote_volume",
	"trades",
}

// encoder writes candles from seq to w and returns how many it wrote.
type encoder func(w io.Writer, seq iter.Seq2[binancedata.Kline, error]) (int, error)

// encoderFor selects the encoder for a -format value.
//
// The context is taken here rather than by the encoder signature because only
// one of the three needs it: binancedata.WriteParquet checks for cancellation
// before writing its footer, while the CSV and JSON writers get cancellation
// for free from the iterator they are draining, which is Loader.Stream's and
// already stops when the caller's context ends. Capturing it in a closure
// rather than storing it in a struct keeps the usual objection to holding a
// context off the table: this one is created and consumed inside a single call.
func encoderFor(ctx context.Context, format string) (encoder, error) {
	switch format {
	case formatCSV:
		return writeCSV, nil
	case formatJSON:
		return writeJSON, nil
	case formatParquet:
		return func(w io.Writer, seq iter.Seq2[binancedata.Kline, error]) (int, error) {
			// The library owns the schema; this is the whole of the parquet
			// encoder, and it is why the CLI does not have a second copy of the
			// column definitions to keep in step.
			return binancedata.WriteParquet(ctx, w, seq)
		}, nil
	default:
		return nil, usagef("-format %q: want %s, %s or %s", format, formatCSV, formatJSON, formatParquet)
	}
}

// writeCSV writes a header row and one row per candle.
//
// Times are RFC 3339 in UTC, keeping their fractional seconds: a close time
// ends in .999 before 2025 and .999999 after, and truncating to whole seconds
// would make two adjacent candles look like they overlap.
//
// Prices and volumes are written with Decimal.String, never through the
// float64 column helpers. That is the entire reason this project uses a decimal
// type: a quote volume reaching twenty significant digits does not survive a
// float64, and a CSV is exactly the sort of file somebody loads five years
// later expecting the numbers to be the ones Binance published.
func writeCSV(w io.Writer, seq iter.Seq2[binancedata.Kline, error]) (int, error) {
	cw := csv.NewWriter(w)

	if err := cw.Write(columns); err != nil {
		return 0, err
	}

	// One row slice, refilled per candle. csv.Writer copies what it needs
	// before returning, so reusing the backing array is safe and saves an
	// allocation per candle — 2.6 million of them for five years of minutes.
	row := make([]string, len(columns))
	rows := 0

	for k, err := range seq {
		if err != nil {
			return rows, err
		}

		row[0] = k.OpenTime.Format(time.RFC3339Nano)
		row[1] = k.CloseTime.Format(time.RFC3339Nano)
		row[2] = k.Open.String()
		row[3] = k.High.String()
		row[4] = k.Low.String()
		row[5] = k.Close.String()
		row[6] = k.Volume.String()
		row[7] = k.QuoteVolume.String()
		row[8] = k.TakerBuyBaseVolume.String()
		row[9] = k.TakerBuyQuoteVolume.String()
		row[10] = strconv.FormatInt(k.Trades, 10)

		if err := cw.Write(row); err != nil {
			return rows, err
		}

		rows++
	}

	// csv.Writer buffers. Without this the last few kilobytes never reach the
	// file, and the failure looks like a truncated download rather than a
	// missing method call.
	cw.Flush()

	return rows, cw.Error()
}

// writeJSON writes one JSON array, streamed.
//
// An array rather than newline-delimited objects, because `bmd download |
// jq '.[0]'` is the thing people actually do with this. It still streams: the
// brackets and separators are written by hand around each marshalled candle, so
// nothing ever holds more than one at a time. Marshalling the whole []Kline in
// one call would be shorter and would need the entire range resident.
func writeJSON(w io.Writer, seq iter.Seq2[binancedata.Kline, error]) (int, error) {
	// json.Marshal writes small buffers; a bufio.Writer turns a syscall per
	// candle into one per 64 KB.
	bw := bufio.NewWriter(w)
	rows := 0

	if _, err := bw.WriteString("[\n"); err != nil {
		return 0, err
	}

	for k, err := range seq {
		if err != nil {
			return rows, err
		}

		if rows > 0 {
			if _, err := bw.WriteString(",\n"); err != nil {
				return rows, err
			}
		}

		// Marshal rather than an Encoder: Encoder appends a newline to every
		// value it writes, which would put the separator in the wrong place.
		b, err := json.Marshal(k)
		if err != nil {
			return rows, err
		}

		if _, err := bw.WriteString("  "); err != nil {
			return rows, err
		}

		if _, err := bw.Write(b); err != nil {
			return rows, err
		}

		rows++
	}

	if _, err := bw.WriteString("\n]\n"); err != nil {
		return rows, err
	}

	return rows, bw.Flush()
}

// destination is where a command's output goes, resolved from -out.
type destination struct {
	// path is the file to write, or "" for standard output.
	path string
}

// resolveDestination works out where the candles should land.
//
// Three spellings, in the order they are checked:
//
//   - -out -            standard output
//   - -out <directory>  a generated name inside it
//   - -out <file>       exactly that file
//
// and, when -out is not given at all, a generated name in the current working
// directory. That is the default because a download is usually a file you want
// to keep, and a shell that dumps sixty thousand CSV rows into a terminal
// because a flag was forgotten is a worse default than one that writes a file
// with a predictable name.
func resolveDestination(out, name string) (destination, error) {
	if out == stdoutPath {
		return destination{}, nil
	}

	if out == "" {
		// The directory the command was run in. os.Getwd rather than "." so
		// that the message naming the file is absolute and unambiguous.
		wd, err := os.Getwd()
		if err != nil {
			return destination{}, fmt.Errorf("locating the working directory: %w", err)
		}

		return destination{path: filepath.Join(wd, name)}, nil
	}

	// An existing directory takes the generated name; anything else is taken
	// as the file to write. Stat rather than a trailing-separator test,
	// because "-out ./data" is what people type and it has no trailing slash.
	if info, err := os.Stat(out); err == nil && info.IsDir() {
		return destination{path: filepath.Join(out, name)}, nil
	}

	return destination{path: out}, nil
}

// isStdout reports whether this destination is standard output.
func (d destination) isStdout() bool { return d.path == "" }

// describe names the destination for a human.
func (d destination) describe() string {
	if d.isStdout() {
		return "standard output"
	}

	return d.path
}

// outputName builds the file name a download gets when -out did not supply one:
//
//	BTCUSDT-1h-2024-01-01_2024-03-31.csv
//
// Symbol, interval and both dates, so that a directory of these sorts sensibly
// and says what each file holds. The dates are the ones the request resolved
// to, which matters when -end was left off: the name then records the day the
// data actually runs to rather than claiming an open-ended range.
func outputName(req binancedata.Request, format string) string {
	return fmt.Sprintf("%s-%s-%s_%s.%s",
		req.Symbol,
		req.Interval,
		req.Start.Format(dateLayout),
		req.End.Format(dateLayout),
		format,
	)
}

// writeTo sends the candles to this destination and returns how many arrived.
//
// A file is written through a temporary file in the same directory and renamed
// into place only once the encoder has finished. Two reasons, and the second is
// the one that bites: a download interrupted halfway otherwise leaves a CSV
// that looks complete and is silently short, and a parquet file with no footer
// is not a parquet file at all. Renaming last means the path either does not
// exist or holds a whole file.
//
// Standard output gets no such protection, because there is nothing to rename.
func (d destination) writeTo(stdout io.Writer, encode encoder, seq iter.Seq2[binancedata.Kline, error]) (int, error) {
	if d.isStdout() {
		return encode(stdout, seq)
	}

	var rows int

	err := writeFileAtomically(d.path, func(w io.Writer) error {
		var err error
		rows, err = encode(w, seq)

		return err
	})

	return rows, err
}

// writeFileAtomically writes through a temporary file and renames on success.
//
// The temporary file is created in the destination's own directory rather than
// in the system temp directory, because rename is only atomic within a
// filesystem — across one, os.Rename either fails outright or degrades into a
// copy that can be interrupted, which is the guarantee this exists to provide.
//
// The library's cache does the same thing for the same reason; this is a second
// implementation rather than a shared one because the two differ in what they
// are protecting. The cache is defending its own invariants and syncs the
// directory afterwards, and a user's output file does not need a durability
// guarantee that survives losing power.
func writeFileAtomically(path string, write func(io.Writer) error) (err error) {
	dir := filepath.Dir(path)

	// The pattern starts with a dot so the temporary file is hidden, and
	// contains the final name so that a leftover from a killed process says
	// what it was going to be.
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("creating a temporary file in %s: %w", dir, err)
	}

	name := tmp.Name()

	// Named return plus defer: whatever goes wrong below, the temporary file
	// is removed rather than left behind. The remove is unchecked on purpose —
	// it is cleanup on an error path, and a failure to tidy up is not worth
	// replacing the real error with.
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(name)
		}
	}()

	// bufio here rather than in each encoder: parquet-go writes in large
	// blocks and needs no help, but the CSV writer's own buffer flushes
	// straight to the file, so this is what keeps a download from making one
	// write syscall per few rows.
	bw := bufio.NewWriter(tmp)

	if err = write(bw); err != nil {
		return err
	}

	if err = bw.Flush(); err != nil {
		return fmt.Errorf("writing %s: %w", name, err)
	}

	if err = tmp.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", name, err)
	}

	if err = os.Chmod(name, outputFilePerm); err != nil {
		return fmt.Errorf("setting permissions on %s: %w", name, err)
	}

	if err = os.Rename(name, path); err != nil {
		return fmt.Errorf("renaming %s into place: %w", name, err)
	}

	return nil
}

// isTerminal reports whether w is a terminal rather than a file or a pipe.
//
// It is used for two decisions: whether progress should redraw one line or
// print many, and whether writing a parquet file to standard output would
// scribble binary into somebody's session. os.File's mode carries the answer,
// so this needs no dependency — a character device is a terminal for these
// purposes, and anything that is not an *os.File certainly is not one.
//
// It is deliberately not perfectly accurate: /dev/null is a character device
// and is not a terminal. Both callers are choosing how to *format* output, so
// the cost of being wrong there is a redraw sequence written to a sink that
// discards it, which is why a real terminal check, with the dependency it
// needs, has not been worth it.
//
// Nothing in the test suite reaches the terminal branch through this function,
// because a test writes to a bytes.Buffer and a bytes.Buffer is not an
// *os.File. That gap is covered at newProgress instead, which is the seam.
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}

	info, err := f.Stat()
	if err != nil {
		return false
	}

	return info.Mode()&os.ModeCharDevice != 0
}
