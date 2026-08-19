package vision

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// The fixtures in testdata/ are real responses from the live bucket, captured
// on 2026-08-18 and trimmed to a readable number of entries. Only whitespace
// between elements was added; every key, size, ETag and timestamp is Binance's.
//
// No test in this package touches the network. That is not a preference — a
// suite that reaches Binance is slow, flaky, fails on a plane, and quietly
// tests Binance's uptime instead of this code.
const (
	fixtureHole  = "monthly-1mo-hole.xml"
	fixturePage1 = "daily-page1.xml"
	fixturePage2 = "daily-page2.xml"
	fixtureEmpty = "empty.xml"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	// testdata is the one directory name the go tool ignores when looking for
	// packages, so anything may live in it. Reading with a relative path works
	// because `go test` runs each test binary with its own package directory
	// as the working directory.
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	return b
}

// newTestLister starts an httptest.Server running handler and returns a Lister
// aimed at it.
//
// httptest.Server is a real HTTP server on a real loopback port, so this
// exercises the actual net/http client path — connection reuse, header
// handling, status codes — rather than a stubbed RoundTripper that would let a
// transport-level mistake through. t.Cleanup closes it when the test ends, by
// any path including a t.Fatal.
func newTestLister(t *testing.T, handler http.HandlerFunc) *Lister {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	// testPolicy (retry_test.go) retries exactly as production does but with a
	// timer that fires immediately, so a test covering four attempts costs no
	// wall-clock time.
	return NewLister(srv.URL, srv.Client(), testPolicy())
}

func TestListSinglePage(t *testing.T) {
	t.Parallel()

	var gotQuery string

	lister := newTestLister(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write(readFixture(t, fixtureHole))
	})

	const prefix = "data/spot/monthly/klines/BTCUSDT/1mo/"

	objects, err := lister.List(t.Context(), prefix, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// 11 months of 2024, each with a .zip and a .zip.CHECKSUM. The sidecars
	// are returned as-is; filtering them is the caller's job, because only the
	// caller knows what an archive name should look like.
	if want := 22; len(objects) != want {
		t.Errorf("got %d objects, want %d", len(objects), want)
	}

	// The delimiter is what keeps a listing to one directory level. Losing it
	// would still pass every assertion above while walking the whole bucket.
	for _, want := range []string{"delimiter=%2F", "prefix=data%2Fspot%2Fmonthly"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("query %q does not contain %q", gotQuery, want)
		}
	}

	// No marker was asked for, so none should be sent — an empty marker
	// parameter is not the same as an absent one to every S3 implementation.
	if strings.Contains(gotQuery, "marker") {
		t.Errorf("query %q sent a marker when none was requested", gotQuery)
	}
}

// TestListFindsTheMarch2024Hole pins the finding that ruled out computing
// availability from a calendar rule.
//
// BTCUSDT's 1mo archive for March 2024 does not exist, while February and April
// both do. No date arithmetic predicts a missing month in the middle of a
// published range — only asking the bucket reveals it. This fixture is the
// real listing, so the test fails if that ever stops being true, which is
// exactly when the design assumption would need revisiting.
func TestListFindsTheMarch2024Hole(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(readFixture(t, fixtureHole))
	})

	objects, err := lister.List(t.Context(), "data/spot/monthly/klines/BTCUSDT/1mo/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	present := map[string]bool{}
	for _, o := range objects {
		present[o.Key] = true
	}

	const dir = "data/spot/monthly/klines/BTCUSDT/1mo/"

	for _, month := range []string{"2024-02", "2024-04"} {
		if key := dir + "BTCUSDT-1mo-" + month + ".zip"; !present[key] {
			t.Errorf("%s should exist", key)
		}
	}

	if key := dir + "BTCUSDT-1mo-2024-03.zip"; present[key] {
		t.Errorf("%s exists now; the hole this library is designed around has been filled", key)
	}
}

func TestListFollowsPagination(t *testing.T) {
	t.Parallel()

	var markers []string

	lister := newTestLister(t, func(w http.ResponseWriter, r *http.Request) {
		marker := r.URL.Query().Get("marker")
		markers = append(markers, marker)

		if marker == "" {
			_, _ = w.Write(readFixture(t, fixturePage1))

			return
		}

		_, _ = w.Write(readFixture(t, fixturePage2))
	})

	objects, err := lister.List(t.Context(), "data/spot/daily/klines/BTCUSDT/1m/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if want := 24; len(objects) != want {
		t.Errorf("got %d objects across both pages, want %d", len(objects), want)
	}

	if len(markers) != 2 {
		t.Fatalf("made %d requests, want 2: %q", len(markers), markers)
	}

	// The second request must resume from where the first stopped. Sending an
	// empty marker again would loop; sending the wrong one would skip keys.
	if markers[1] == "" {
		t.Error("second request sent no marker, so it would refetch the first page")
	}

	if !strings.HasSuffix(markers[1], "2017-08-22.zip.CHECKSUM") {
		t.Errorf("second request resumed from %q, want the last key of page 1", markers[1])
	}

	// Order is preserved across the page boundary. Callers rely on listings
	// being chronological, which for ISO-dated keys is the same as
	// lexicographic.
	for i := 1; i < len(objects); i++ {
		if objects[i-1].Key > objects[i].Key {
			t.Errorf("objects out of order at %d: %q then %q", i, objects[i-1].Key, objects[i].Key)
		}
	}
}

func TestListSeeksWithStartAfter(t *testing.T) {
	t.Parallel()

	var gotMarker string

	lister := newTestLister(t, func(w http.ResponseWriter, r *http.Request) {
		gotMarker = r.URL.Query().Get("marker")
		_, _ = w.Write(readFixture(t, fixturePage2))
	})

	const startAfter = "data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2017-08-23"

	if _, err := lister.List(t.Context(), "data/spot/daily/klines/BTCUSDT/1m/", startAfter); err != nil {
		t.Fatalf("List: %v", err)
	}

	if gotMarker != startAfter {
		t.Errorf("got marker %q, want %q", gotMarker, startAfter)
	}
}

// TestListEmptyIsNotAnError is the most important test in this file.
//
// Asking the live bucket for a symbol that does not exist returns HTTP 200 and
// a well-formed listing with no Contents — verified 2026-08-18 against
// .../NOSUCHSYM/1h/. There is no 404 to key off. So this function must report
// success with nothing in it, and every caller must keep "the bucket says there
// is nothing here" distinct from "the bucket did not answer".
//
// Collapsing those two is how a range silently comes back empty.
func TestListEmptyIsNotAnError(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(readFixture(t, fixtureEmpty))
	})

	objects, err := lister.List(t.Context(), "data/spot/monthly/klines/NOSUCHSYM/1h/", "")
	if err != nil {
		t.Fatalf("an empty listing must not be an error, got: %v", err)
	}

	if len(objects) != 0 {
		t.Errorf("got %d objects, want 0", len(objects))
	}
}

func TestListStatusErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     int
		body       string
		wantInErr  []string
		wantNotErr string
	}{
		{
			name:   "no such bucket",
			status: http.StatusNotFound,
			body: `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code>` +
				`<Message>The specified bucket does not exist</Message></Error>`,
			wantInErr: []string{"NoSuchBucket", "does not exist"},
		},
		{
			// What a bucket migrated to another region looks like. S3 puts the
			// new endpoint in the body, so quoting it turns an afternoon of
			// confusion into a one-line configuration change.
			name:   "moved to another region",
			status: http.StatusMovedPermanently,
			body: `<?xml version="1.0" encoding="UTF-8"?><Error><Code>PermanentRedirect</Code>` +
				`<Message>The bucket is in this region</Message>` +
				`<Endpoint>s3-eu-west-1.amazonaws.com</Endpoint></Error>`,
			wantInErr: []string{"PermanentRedirect", "s3-eu-west-1.amazonaws.com"},
		},
		{
			// The likeliest cause is a base URL pointing at something that is
			// not S3 at all — a proxy login page, say. The status alone would
			// send the reader looking in the wrong place, so the body leads.
			name:      "not an S3 endpoint at all",
			status:    http.StatusForbidden,
			body:      "<html><body>Access denied by corporate proxy</body></html>",
			wantInErr: []string{"corporate proxy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			})

			_, err := lister.List(t.Context(), "data/spot/monthly/klines/BTCUSDT/1h/", "")
			if err == nil {
				t.Fatal("want an error, got nil")
			}

			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// TestListRejectsAStuckMarker covers a server that keeps saying "there is more"
// without advancing. Without the check this exercises, the client would spin to
// its page limit and then report a plausible-sounding truncation error, sending
// the reader after the wrong problem.
func TestListRejectsAStuckMarker(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult><IsTruncated>true</IsTruncated>`+
			`<NextMarker>always-the-same</NextMarker>`+
			`<Contents><Key>a</Key><Size>1</Size></Contents></ListBucketResult>`)
	})

	_, err := lister.List(t.Context(), "p/", "")
	if err == nil || !strings.Contains(err.Error(), "did not advance") {
		t.Errorf("got %v, want an error about the marker not advancing", err)
	}
}

// TestListStopsAfterMaxPages covers a server that paginates forever with a
// marker that does advance. The bound is what stops one bad response from
// hanging a backtest.
func TestListStopsAfterMaxPages(t *testing.T) {
	t.Parallel()

	page := 0

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		page++
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult><IsTruncated>true</IsTruncated>`+
			`<NextMarker>key-%09d</NextMarker>`+
			`<Contents><Key>key-%09d</Key><Size>1</Size></Contents></ListBucketResult>`, page, page)
	})

	_, err := lister.List(t.Context(), "p/", "")
	if err == nil || !strings.Contains(err.Error(), "still truncated") {
		t.Errorf("got %v, want an error about exceeding the page limit", err)
	}

	if page != maxPages {
		t.Errorf("made %d requests, want exactly maxPages (%d)", page, maxPages)
	}
}

func TestListTruncatedWithNoMarker(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`)
	})

	_, err := lister.List(t.Context(), "p/", "")
	if err == nil || !strings.Contains(err.Error(), "no marker") {
		t.Errorf("got %v, want an error about the missing continuation marker", err)
	}
}

// TestListFallsBackToLastKey covers the case S3 documents but this client never
// triggers: a truncated response with no NextMarker, which happens when no
// delimiter was requested. The last key returned is itself a valid marker,
// since marker is exclusive.
func TestListFallsBackToLastKey(t *testing.T) {
	t.Parallel()

	var markers []string

	lister := newTestLister(t, func(w http.ResponseWriter, r *http.Request) {
		markers = append(markers, r.URL.Query().Get("marker"))

		if len(markers) == 1 {
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
				`<ListBucketResult><IsTruncated>true</IsTruncated>`+
				`<Contents><Key>aaa</Key><Size>1</Size></Contents>`+
				`<Contents><Key>bbb</Key><Size>2</Size></Contents></ListBucketResult>`)

			return
		}

		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`+
			`<ListBucketResult><IsTruncated>false</IsTruncated>`+
			`<Contents><Key>ccc</Key><Size>3</Size></Contents></ListBucketResult>`)
	})

	objects, err := lister.List(t.Context(), "p/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(objects) != 3 {
		t.Errorf("got %d objects, want 3", len(objects))
	}

	if len(markers) != 2 || markers[1] != "bbb" {
		t.Errorf("markers %q, want the second to be the last key of page 1 (\"bbb\")", markers)
	}
}

func TestListMalformedXML(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>yes please</IsTruncated>`)
	})

	_, err := lister.List(t.Context(), "p/", "")
	if err == nil || !strings.Contains(err.Error(), "parsing response") {
		t.Errorf("got %v, want a parse error", err)
	}
}

// TestListHonoursContextCancellation is what makes a cancelled backtest stop
// downloading rather than finish and discard the result. The whole API takes a
// context for this reason, and an untested context is usually an unwired one.
func TestListHonoursContextCancellation(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(readFixture(t, fixtureHole))
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel before the call, so the result cannot be a race

	_, err := lister.List(ctx, "p/", "")
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Errorf("got %v, want a context cancellation error", err)
	}
}

func TestNewListerDefaults(t *testing.T) {
	t.Parallel()

	lister := NewLister("", nil, Policy{})

	if lister.baseURL != DefaultBaseURL {
		t.Errorf("baseURL = %q, want %q", lister.baseURL, DefaultBaseURL)
	}

	if lister.client == http.DefaultClient {
		t.Fatal("client defaulted to http.DefaultClient, whose MaxIdleConnsPerHost is 2 — that is bug 8")
	}

	if lister.client != defaultClient() {
		t.Error("client should default to the process-wide client from NewHTTPClient")
	}

	// One client, not one per constructor. Several correct-looking clients each
	// holding their own connection pool is the same fragmentation as none at
	// all, in a form that passes every test asserting on transport settings.
	if NewDownloader("", nil, Policy{}).client != lister.client {
		t.Error("Lister and Downloader defaulted to different clients; the pool is per-client")
	}
}

// TestListRejectsAnythingThatIsNotAListing is the regression test for the
// quietest failure this package can have.
//
// A struct without an XMLName field accepts *any* well-formed XML: encoding/xml
// matches child elements by name and ignores the root entirely, so a document
// with none of the expected children unmarshals successfully into a zero value.
// A zero listBucketResult is a listing that is not truncated and holds no keys
// — which is to say, "Binance has published no archives here", returned with a
// nil error and an HTTP 200 to vouch for it.
//
// That is exactly the "a failed listing read as an empty one" conflation the
// package comment forbids, and it is how the ported implementation returned
// ranges with days missing and nothing to show for it. One struct field turns
// every case below into a parse error, which is the honest answer: we do not
// know what is there.
func TestListRejectsAnythingThatIsNotAListing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			// A proxy or captive portal answering 200 with a login page.
			name: "an HTML page served as XML",
			body: `<html><head><title>Sign in</title></head><body>Please log in</body></html>`,
		},
		{
			// An S3 error document that arrived with the wrong status.
			name: "an error document with a 200",
			body: `<?xml version="1.0"?><Error><Code>NoSuchBucket</Code></Error>`,
		},
		{
			// The shape that makes this dangerous: every child element the
			// decoder wants, under a root that is not a listing. Without the
			// XMLName check this parses into a perfectly plausible listing.
			name: "the right children under the wrong root",
			body: `<?xml version="1.0"?><SomethingElse><IsTruncated>false</IsTruncated>` +
				`<Contents><Key>data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip</Key>` +
				`<Size>1</Size></Contents></SomethingElse>`,
		},
		{
			name: "an empty document",
			body: `<?xml version="1.0"?><nothing/>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(tt.body))
			})

			objects, err := lister.List(t.Context(), "data/spot/monthly/klines/BTCUSDT/1h/", "")
			if err == nil {
				t.Fatalf("got %d objects and no error; a body that is not a listing must not read as an empty one", len(objects))
			}

			if objects != nil {
				t.Errorf("got %d objects alongside the error, want nil", len(objects))
			}
		})
	}
}

// TestListBoundsTheResponseItBuffers is the resource half of the test above.
//
// The XMLName check rejects a body that is not a listing — but it can only run
// once the whole body is in memory, so until the read was bounded the rejection
// came *after* the cost it was meant to avoid. A misconfigured baseURL aimed at
// a host answering 200 with something large (a CDN mirror, a proxy streaming a
// file, a bucket replaced by something else) was buffered in full before
// anything got the chance to say it was not a listing.
//
// Every other body read in this package is capped — 8 KiB in s3StatusError,
// 64 KiB in drain, 4 KiB in Checksum. This was the one that was not, and it is
// the one that runs twice per symbol now that the two granularities are
// concurrent, and once per worker at Stage 7.
func TestListBoundsTheResponseItBuffers(t *testing.T) {
	t.Parallel()

	var served atomic.Int64

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		// Well-formed XML that never ends: the failure has to be the size, not
		// a parse error, or the test would pass for the wrong reason.
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated>`))

		chunk := []byte(`<Contents><Key>` + strings.Repeat("k", 1024) + `</Key><Size>1</Size></Contents>`)
		for range (maxListingPage / len(chunk)) + 64 {
			n, err := w.Write(chunk)
			served.Add(int64(n))

			if err != nil {
				return // the client gave up, which is the outcome under test
			}
		}
	})

	objects, err := lister.List(t.Context(), "data/spot/monthly/klines/BTCUSDT/1h/", "")
	if err == nil {
		t.Fatalf("got %d objects and no error; an unbounded body must not read as a listing", len(objects))
	}

	if objects != nil {
		t.Errorf("got %d objects alongside the error, want nil", len(objects))
	}

	if !strings.Contains(err.Error(), "larger than") {
		t.Errorf("error %q does not name the size as the problem", err)
	}
}

// TestListAcceptsTheRealNamespacedRoot is the other half of the test above: the
// XMLName tag carries no namespace, and S3's real root element does
// (xmlns="http://s3.amazonaws.com/doc/2006-03-01/").
//
// encoding/xml compares only the local name when the tag names no namespace, so
// the real thing still parses — but that is a property of the standard library
// worth pinning rather than remembering, since tightening the tag to include
// the namespace would reject every genuine listing.
func TestListAcceptsTheRealNamespacedRoot(t *testing.T) {
	t.Parallel()

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(readFixture(t, fixtureEmpty))
	})

	objects, err := lister.List(t.Context(), "data/spot/monthly/klines/NOSUCHSYM/1h/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(objects) != 0 {
		t.Errorf("got %d objects, want none", len(objects))
	}
}

// TestListRetriesATransientFailure covers why listings share the downloader's
// retry policy. A listing that fails is a listing that must not be read as
// empty, so a 503 nobody retried turns a half-second hiccup into a failed
// request for the whole range.
func TestListRetriesATransientFailure(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	lister := newTestLister(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write(readFixture(t, fixtureHole))
	})

	objects, err := lister.List(t.Context(), "data/spot/monthly/klines/BTCUSDT/1mo/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(objects) != 22 {
		t.Errorf("got %d objects, want 22", len(objects))
	}

	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d requests, want 3", got)
	}
}
