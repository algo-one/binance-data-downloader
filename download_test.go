package binancedata

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// fastPolicy is the production retry policy with the waiting removed: the timer
// fires immediately instead of after the backoff.
//
// Nothing that decides *whether* to retry is changed, so a test covering four
// attempts covers the four attempts that ship. The fields are exported on
// vision.Policy precisely so that a test in another package can do this.
func fastPolicy() vision.Policy {
	p := vision.DefaultPolicy()
	p.Jitter = func(time.Duration) time.Duration { return 0 }
	p.After = func(time.Duration) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch
	}

	return p
}

// newFixtureServer serves testdata/ at the bucket paths the real archives live
// at, and returns a Downloader aimed at it along with a count of requests.
//
// The files are the genuine archives Binance published, with the genuine
// .CHECKSUM sidecars — see testdata/README.md, and do not re-zip them. That is
// what makes this an end-to-end test of the thing that matters: the checksums
// being verified here are Binance's own, not values this repository made up for
// itself and would therefore agree with no matter what the code did.
func newFixtureServer(t *testing.T, mutate func(name string, body []byte) ([]byte, int)) (*vision.Downloader, *atomic.Int64) {
	t.Helper()

	var requests atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)

		name := path.Base(r.URL.Path)

		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			// Exactly what the bucket does for an archive that was never
			// published: a 404, not an error document.
			http.NotFound(w, r)

			return
		}

		status := http.StatusOK
		if mutate != nil {
			body, status = mutate(name, body)
		}

		if status != http.StatusOK {
			w.WriteHeader(status)

			return
		}

		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	return vision.NewDownloader(srv.URL, srv.Client(), fastPolicy()), &requests
}

func TestArchiveRefKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ref     archiveRef
		want    string
		wantErr bool
	}{
		{
			name: "daily",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
				Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			want: "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip",
		},
		{
			name: "monthly",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1m,
				Agg: aggMonthly, Period: utc(2024, 1, 1),
			},
			want: "data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01.zip",
		},
		{
			// The archive spelling of monthly candles is "1mo"; the REST API's
			// "1M" would 404 in both the directory and the file name.
			name: "monthly candles",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1mo,
				Agg: aggMonthly, Period: utc(2024, 1, 1),
			},
			want: "data/spot/monthly/klines/BTCUSDT/1mo/BTCUSDT-1mo-2024-01.zip",
		},
		{
			// A day-of-month on a monthly ref is truncated by the layout, not
			// rejected: "2006-01" simply does not render the day.
			name: "a monthly period carrying a day",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
				Agg: aggMonthly, Period: utc(2024, 1, 15),
			},
			want: "data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip",
		},
		{
			name: "unset market",
			ref: archiveRef{
				Symbol: "BTCUSDT", Interval: Interval1h, Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
		{
			name: "unset aggregation",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h, Period: utc(2024, 1, 15),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.ref.key()

			if tt.wantErr {
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatalf("got %q, %v; want an error wrapping ErrInvalidRequest", got, err)
				}

				return
			}

			if err != nil {
				t.Fatalf("key: %v", err)
			}

			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFetchArchiveVerifiesRealArchives is the end-to-end test this stage exists
// to make possible: fetch a real archive over HTTP, verify it against the
// SHA-256 Binance published for it, and parse the result into candles.
//
// Every fixture is byte-for-byte what the bucket served, so a passing run means
// the download path agrees with Binance's own checksums rather than with itself.
func TestFetchArchiveVerifiesRealArchives(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ref      archiveRef
		spec     decodeSpec
		wantRows int
	}{
		{
			name: "an ordinary day in the millisecond era",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
				Agg: aggDaily, Period: utc(2024, 1, 15),
			},
			spec:     decodeSpec{Interval: Interval1h, Start: utc(2024, 1, 15), End: utc(2024, 1, 16)},
			wantRows: 24,
		},
		{
			name: "the last day Binance wrote milliseconds",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
				Agg: aggDaily, Period: utc(2024, 12, 31),
			},
			spec:     decodeSpec{Interval: Interval1h, Start: utc(2024, 12, 31), End: utc(2025, 1, 1)},
			wantRows: 24,
		},
		{
			name: "the first day it wrote microseconds",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
				Agg: aggDaily, Period: utc(2025, 1, 1),
			},
			spec:     decodeSpec{Interval: Interval1h, Start: utc(2025, 1, 1), End: utc(2025, 1, 2)},
			wantRows: 24,
		},
		{
			name: "a monthly archive of monthly candles",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1mo,
				Agg: aggMonthly, Period: utc(2024, 1, 1),
			},
			spec:     decodeSpec{Interval: Interval1mo, Start: utc(2024, 1, 1), End: utc(2024, 2, 1)},
			wantRows: 1,
		},
		{
			name: "a day of one-minute candles",
			ref: archiveRef{
				Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1m,
				Agg: aggDaily, Period: utc(2025, 1, 15),
			},
			spec:     decodeSpec{Interval: Interval1m, Start: utc(2025, 1, 15), End: utc(2025, 1, 16)},
			wantRows: 1440,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dl, requests := newFixtureServer(t, nil)

			var dst bytes.Buffer

			res, err := fetchArchive(t.Context(), dl, tt.ref, &dst)
			if err != nil {
				t.Fatalf("fetchArchive: %v", err)
			}

			// Two requests: the sidecar, then the archive.
			if got := requests.Load(); got != 2 {
				t.Errorf("made %d requests, want 2 (sidecar then archive)", got)
			}

			key, err := tt.ref.key()
			if err != nil {
				t.Fatalf("key: %v", err)
			}

			want, err := os.ReadFile(filepath.Join("testdata", path.Base(key)))
			if err != nil {
				t.Fatalf("reading fixture: %v", err)
			}

			if !bytes.Equal(dst.Bytes(), want) {
				t.Errorf("downloaded %d bytes, want the fixture's %d", dst.Len(), len(want))
			}

			if res.Size != int64(len(want)) {
				t.Errorf("Size = %d, want %d", res.Size, len(want))
			}

			// The hash fetchArchive verified against is the one in the
			// committed sidecar, which is the one Binance published.
			sidecar, err := os.ReadFile(filepath.Join("testdata", path.Base(key)+vision.ChecksumSuffix))
			if err != nil {
				t.Fatalf("reading sidecar: %v", err)
			}

			if published := strings.Fields(string(sidecar))[0]; res.SHA256 != published {
				t.Errorf("SHA256 = %s, published = %s", res.SHA256, published)
			}

			// And the bytes that survived all of that are parseable candles.
			// This is the whole pipeline as far as it has been built: bucket
			// key, HTTP, checksum, ZIP, CSV, Kline.
			klines, err := decodeArchiveAll(t.Context(), bytes.NewReader(dst.Bytes()), int64(dst.Len()), tt.spec)
			if err != nil {
				t.Fatalf("decoding what was downloaded: %v", err)
			}

			if len(klines) != tt.wantRows {
				t.Errorf("decoded %d candles, want %d", len(klines), tt.wantRows)
			}
		})
	}
}

// TestFetchArchiveDetectsCorruption flips one byte of a real archive, leaving
// its real sidecar in place.
//
// This is the case the whole checksum apparatus exists for, and the one the
// Python implementation could not catch: it wrote sidecars to disk and never
// compared anything against them, which makes a checksum a decoration.
func TestFetchArchiveDetectsCorruption(t *testing.T) {
	t.Parallel()

	dl, _ := newFixtureServer(t, func(name string, body []byte) ([]byte, int) {
		if strings.HasSuffix(name, ".zip") {
			// Deep inside the compressed data, so the ZIP structure still
			// looks intact — the failure has to be caught by the hash, not by
			// the parser noticing something else.
			corrupted := bytes.Clone(body)
			corrupted[len(corrupted)/2] ^= 0xFF

			return corrupted, http.StatusOK
		}

		return body, http.StatusOK
	})

	ref := archiveRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
		Agg: aggDaily, Period: utc(2024, 1, 15),
	}

	var dst bytes.Buffer

	_, err := fetchArchive(t.Context(), dl, ref, &dst)
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("got %v, want an error wrapping ErrChecksum", err)
	}

	// Both hashes belong in the message: without them the only way to tell a
	// corrupted transfer from a stale sidecar is to re-run by hand.
	for _, want := range []string{"downloaded", "published"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not report the %s hash", err, want)
		}
	}

	// A single flipped byte must not be masked by a partially written
	// destination looking plausible: the caller is told to discard it.
	if dst.Len() == 0 {
		t.Error("the partial download should still have been written; the caller discards it")
	}
}

func TestFetchArchiveTranslatesTransportErrors(t *testing.T) {
	t.Parallel()

	// A period no fixture covers, so the server answers 404 for both the
	// sidecar and the archive — exactly as the bucket does for a month that
	// has not been published.
	missing := archiveRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
		Agg: aggDaily, Period: utc(2019, 3, 7),
	}

	present := archiveRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
		Agg: aggDaily, Period: utc(2024, 1, 15),
	}

	tests := []struct {
		name         string
		ref          archiveRef
		mutate       func(string, []byte) ([]byte, int)
		wantErr      error
		wantRequests int64
	}{
		{
			// The most common non-200 this library will ever see, and the one
			// callers branch on rather than fail over.
			name: "a month that was never published",
			ref:  missing, wantErr: ErrNotAvailable, wantRequests: 1,
		},
		{
			// The sidecar is fetched first and is 91 bytes, so a missing
			// archive costs one tiny request rather than a 93 MB transfer that
			// ends in a 404.
			name: "the sidecar is missing while the archive is not",
			ref:  present,
			mutate: func(name string, body []byte) ([]byte, int) {
				if strings.HasSuffix(name, vision.ChecksumSuffix) {
					return nil, http.StatusNotFound
				}

				return body, http.StatusOK
			},
			wantErr: ErrNotAvailable, wantRequests: 1,
		},
		{
			// 429 survived the four attempts, so the pipeline as a whole is
			// going too fast — a decision for the layer above.
			name: "rate limited past the retries",
			ref:  present,
			mutate: func(_ string, _ []byte) ([]byte, int) {
				return nil, http.StatusTooManyRequests
			},
			wantErr: ErrRateLimited, wantRequests: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dl, requests := newFixtureServer(t, tt.mutate)

			_, err := fetchArchive(t.Context(), dl, tt.ref, &bytes.Buffer{})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("got %v, want an error wrapping %v", err, tt.wantErr)
			}

			if got := requests.Load(); got != tt.wantRequests {
				t.Errorf("made %d requests, want %d", got, tt.wantRequests)
			}

			// The transport's own message survives alongside the sentinel —
			// fmt.Errorf takes more than one %w — so the error names the key
			// as well as the category.
			if !strings.Contains(err.Error(), "BTCUSDT") {
				t.Errorf("error %q does not name what was being fetched", err)
			}
		})
	}
}

func TestTranslateVisionError(t *testing.T) {
	t.Parallel()

	other := errors.New("something else entirely")

	tests := []struct {
		name string
		in   error
		want error
		// keep asserts that the original error is still reachable, so a
		// message naming the key and status is not thrown away in the
		// translation.
		keep error
	}{
		{"not found", vision.ErrNotFound, ErrNotAvailable, vision.ErrNotFound},
		{"rate limited", vision.ErrRateLimited, ErrRateLimited, vision.ErrRateLimited},
		{"anything else passes through", other, other, other},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := translateVisionError(tt.in)

			if !errors.Is(got, tt.want) {
				t.Errorf("got %v, want an error wrapping %v", got, tt.want)
			}

			if !errors.Is(got, tt.keep) {
				t.Errorf("got %v, which no longer wraps the original %v", got, tt.keep)
			}
		})
	}

	// An error this function does not recognise must not acquire a sentinel it
	// cannot justify.
	if got := translateVisionError(other); errors.Is(got, ErrNotAvailable) {
		t.Error("an unrecognised error was labelled ErrNotAvailable")
	}
}

func TestArchiveRefString(t *testing.T) {
	t.Parallel()

	// The Stringer exists for test failures, where "the daily chunk" is less
	// use than knowing which day. The key is what errors quote instead, since
	// the question there is what was asked of the server.
	ref := archiveRef{
		Market: MarketSpot, Symbol: "BTCUSDT", Interval: Interval1h,
		Agg: aggDaily, Period: utc(2024, 1, 15),
	}

	if got, want := ref.String(), "BTCUSDT 1h daily 2024-01-15T00:00:00Z"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
