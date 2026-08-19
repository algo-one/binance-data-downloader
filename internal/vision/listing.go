// Package vision talks to the bucket behind data.binance.vision.
//
// # What the bucket is
//
// data.binance.vision is not a Binance service. It is an ordinary Amazon S3
// bucket, literally named "data.binance.vision", in the ap-northeast-1 region,
// which Binance pointed a domain at and left publicly readable. That detail is
// invisible when downloading a file and unavoidable when asking what files
// exist, because the two go through different front doors:
//
//	https://data.binance.vision/data/spot/monthly/klines/...zip
//	    fetches one object. Binance's domain, Binance's problem to keep working.
//
//	https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=...
//	    lists objects. S3's own API, and it bakes in the provider, the region
//	    and the bucket name.
//
// Only the second one can answer "what exists?". Verified on 2026-08-18:
// requesting the listing query string from data.binance.vision returns the HTML
// file-browser page — a static page that calls this same S3 API from JavaScript
// — with HTTP 200 and content-type text/html. There is no way to list through
// Binance's domain.
//
// # Why that is acceptable anyway
//
// Because listing is an optimisation and never the source of truth. If Binance
// migrates the bucket, this package starts returning errors and the layers
// above degrade to fetching what they were going to fetch anyway; what they
// must never do is read a failed listing as "no data exists". That is why
// [Lister.List] distinguishes three outcomes rather than two, and why the empty
// listing is the one that needs explaining — see [Lister.List].
//
// The base URL is a field rather than a constant so that a consumer can
// re-point it without waiting for a release, and so that every test in this
// package can aim it at an httptest.Server.
package vision

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// DefaultBaseURL is the S3 REST endpoint for the bucket Binance publishes to.
//
// Path-style rather than virtual-host style — the bucket name is the first path
// segment rather than a subdomain — because the bucket name contains dots, and
// a virtual-host URL would put those dots into the hostname where they would
// break TLS certificate matching.
const DefaultBaseURL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

// maxPages caps how many times [Lister.List] will follow pagination.
//
// S3 returns at most 1000 keys per response. Archives come in pairs — every
// .zip has a .zip.CHECKSUM sidecar beside it — so one page is only 500 days,
// and the full daily history of a symbol listed in 2017 runs to six or seven
// pages. Twenty is comfortable headroom for that, and still a bound: a server
// that kept answering "there is more" would otherwise loop forever.
const maxPages = 20

// maxListingPage bounds how large a single ListObjects response may be before
// it is rejected as not being one.
//
// A full page is 1000 entries. Each is a key of roughly 75 characters wrapped
// in the Contents element S3 sends — Key, Size, LastModified, ETag,
// StorageClass — for about 250 bytes, so a real maximal page is around 250 KB.
// A mebibyte is four times that: comfortable headroom for a key layout that
// grows, and still a bound rather than a hope.
const maxListingPage = 1 << 20 // 1 MiB

// Lister reads object listings from the bucket.
//
// It holds an *http.Client rather than creating one, because connection reuse
// only happens if the same client is used for every request in the process —
// a fresh client per call reopens the TLS connection each time, which is the
// eighth bug in the implementation this library replaces.
type Lister struct {
	baseURL string
	client  *http.Client
	policy  Policy
}

// NewLister returns a Lister reading from baseURL using client.
//
// An empty baseURL means [DefaultBaseURL], a nil client means the process-wide
// client from [NewHTTPClient], and the zero Policy means [DefaultPolicy].
// Accepting zero values for all three keeps tests short while leaving the
// production wiring explicit.
//
// The nil client deliberately does not mean http.DefaultClient — see
// [defaultClient] for why that default would quietly reintroduce the connection
// churn this package exists to remove.
//
// Listings are retried on the same policy as downloads, and it matters more
// here than it looks: a listing that fails is a listing that cannot be read as
// empty, so every transient 503 that is not retried becomes a request that
// fails outright rather than one that costs an extra half second.
func NewLister(baseURL string, client *http.Client, p Policy) *Lister {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	if client == nil {
		client = defaultClient()
	}

	return &Lister{baseURL: baseURL, client: client, policy: p.withDefaults()}
}

// listBucketResult is the subset of S3's ListObjects response this package
// reads. Fields S3 sends that nothing here needs — ETag, StorageClass, Name,
// MaxKeys — are simply absent: encoding/xml ignores elements with no matching
// field, so a struct is a projection rather than a schema.
//
// The `xml:"..."` strings are struct tags: metadata attached to a field and
// read at runtime through reflection. They are how the standard library's
// encoders learn the wire name of a field without a code generator.
type listBucketResult struct {
	// XMLName pins the name of the root element. A field of type xml.Name
	// makes encoding/xml check the document's root against the tag and fail
	// the unmarshal when it differs, instead of the default behaviour of
	// accepting any root and filling in whichever children happen to match.
	//
	// Without it this struct accepts *any* well-formed XML. Every field would
	// simply stay at its zero value, which is a listing that is not truncated
	// and contains nothing — and that is precisely the "a failed listing read
	// as an empty one" conflation the package comment says must never happen,
	// arriving with a nil error and an HTTP 200 to vouch for it. A proxy login
	// page, an error document from a CDN or a bucket replaced by something
	// else would all be reported as "Binance has published no archives".
	//
	// One field turns all of those into a parse error, which is the honest
	// answer: we do not know what is there.
	XMLName xml.Name `xml:"ListBucketResult"`

	// IsTruncated is S3 saying "there are more keys than fit in this
	// response". encoding/xml parses "true"/"false" text into a bool.
	IsTruncated bool `xml:"IsTruncated"`

	// NextMarker is where to resume. S3 only sends it when a delimiter was
	// requested, which this package always does — but the fallback in
	// nextMarker below does not rely on that.
	NextMarker string `xml:"NextMarker"`

	// Contents is one entry per object. The slice is nil when the prefix
	// matches nothing, which is the case worth reading carefully.
	Contents []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

// s3Error is the XML body S3 returns with a non-2xx status. Reading it turns
// "HTTP 301" into "PermanentRedirect: the bucket is in another region", which
// is the difference between a five-minute diagnosis and an afternoon.
type s3Error struct {
	Code     string `xml:"Code"`
	Message  string `xml:"Message"`
	Endpoint string `xml:"Endpoint"` // set on a region redirect
}

// Object is one entry from a listing: the object's full key and its size.
//
// Size is carried because it is free — S3 sends it whether or not it is wanted
// — and because it is the cheapest way to notice a placeholder file. Binance
// has published archives that are a few hundred bytes of nothing.
type Object struct {
	Key  string
	Size int64
}

// List returns every object directly under prefix, in lexicographic order.
//
// # The three outcomes
//
// Go's two-value return already provides the three states this needs, and the
// distinction is the whole reason this function exists:
//
//	objects, nil    the bucket answered; these are the objects (possibly none)
//	nil, err        the bucket did not answer; nothing is known
//
// The trap is the first case with an empty slice. Asking for a symbol that does
// not exist returns HTTP 200 and a well-formed listing with no Contents at all
// — verified 2026-08-18 against prefix .../NOSUCHSYM/1h/. There is no 404. So
// "this symbol was never listed" and "this interval has no archives yet" and
// "you typed the prefix wrong" are one indistinguishable answer, and it looks
// exactly like success.
//
// What callers must not do is collapse that into the error case, or treat a
// failed listing as an empty one. An empty listing means "Binance has nothing
// here", which is a fact worth acting on; an error means "we do not know",
// which is never a reason to return zero candles and a nil error. That
// conflation is how the ported implementation lost days at the end of a range.
//
// # startAfter
//
// Listings resume rather than restart. Keys sort lexicographically and Binance
// names archives with ISO dates — BTCUSDT-1m-2024-06-01.zip — so lexicographic
// order is chronological order, and passing the key a range begins at seeks
// straight to it. Without that, listing the daily archives of a symbol listed
// in 2017 costs seven round trips before reaching 2024; with it, one.
//
// Pass the empty string to list from the beginning.
func (l *Lister) List(ctx context.Context, prefix, startAfter string) ([]Object, error) {
	var (
		objects []Object
		marker  = startAfter
	)

	for page := 0; page < maxPages; page++ {
		result, err := l.fetchPage(ctx, prefix, marker)
		if err != nil {
			return nil, err
		}

		for _, c := range result.Contents {
			objects = append(objects, Object{Key: c.Key, Size: c.Size})
		}

		if !result.IsTruncated {
			return objects, nil
		}

		next := nextMarker(result)
		if next == "" {
			return nil, fmt.Errorf("listing %q: truncated response gave no marker to continue from", prefix)
		}

		// A marker that does not advance means the next request would return
		// this same page. Without this check the loop would spin until
		// maxPages and then report a plausible-looking truncation error;
		// with it, the actual problem is named.
		if next <= marker {
			return nil, fmt.Errorf("listing %q: marker did not advance (%q after %q)", prefix, next, marker)
		}

		marker = next
	}

	return nil, fmt.Errorf("listing %q: still truncated after %d pages", prefix, maxPages)
}

// nextMarker decides where the next page starts.
//
// S3 sends NextMarker only when the request included a delimiter. This package
// always sends one, so the field is always present in practice — but relying on
// "in practice" for a value that silently truncates a listing is a poor trade,
// and the fallback is one line: with no NextMarker, the last key returned is
// itself a valid marker, since marker is exclusive.
func nextMarker(result *listBucketResult) string {
	if result.NextMarker != "" {
		return result.NextMarker
	}

	if n := len(result.Contents); n > 0 {
		return result.Contents[n-1].Key
	}

	return ""
}

// fetchPage performs one ListObjects request.
func (l *Lister) fetchPage(ctx context.Context, prefix, marker string) (*listBucketResult, error) {
	// url.Values builds and escapes the query string. Doing it by hand with
	// string concatenation is how a prefix containing a "+" or a space turns
	// into a listing of something else entirely.
	query := url.Values{}
	// delimiter=/ keeps the listing to this one directory level. Archive
	// directories are flat, so nothing is lost, and it guarantees a mistyped
	// prefix cannot start walking the whole bucket.
	query.Set("delimiter", "/")
	query.Set("prefix", prefix)

	if marker != "" {
		query.Set("marker", marker)
	}

	endpoint := l.baseURL + "?" + query.Encode()

	// NewRequestWithContext, never NewRequest. The context is what makes a
	// cancelled backtest actually stop the transfer in flight rather than
	// finishing it and discarding the result; the noctx linter fails the build
	// on the version without it.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("listing %q: building request: %w", prefix, err)
	}

	// doWithRetry rather than client.Do: a listing is one request whose failure
	// costs the whole plan, so a transient 503 is worth half a second to ride
	// out. What it does not do is decide what a status means — a 404 here is a
	// misconfigured base URL and is reported as such below, where a 404 on an
	// archive is routine.
	resp, err := doWithRetry(ctx, l.client, req, l.policy)
	if err != nil {
		// A transport error is already wrapped with the URL by net/http, and
		// it is the one place a context cancellation surfaces, so it is passed
		// up rather than reshaped.
		return nil, fmt.Errorf("listing %q: %w", prefix, err)
	}

	// defer runs this when the function returns, by any path including a panic.
	// Closing the body is what returns the connection to the pool for reuse;
	// leaking one leaks a connection, and enough of them stall the downloader.
	// The error is explicitly discarded rather than ignored: there is nothing
	// useful to do about a failure to close a body that has already been read,
	// and the assignment makes that a visible decision rather than an
	// oversight the errcheck linter would flag.
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, s3StatusError(prefix, resp)
	}

	// io.ReadAll rather than streaming into the decoder, because the body has
	// to be read to completion for the connection to be reusable anyway, and a
	// listing page is bounded at 1000 entries — a few hundred kilobytes.
	//
	// That bound is enforced rather than assumed. It holds for S3, and this is
	// the one read in the package that reaches a body nothing has yet
	// established came from S3: the XMLName check that rejects a proxy login
	// page or a CDN error document runs on the bytes *after* they are all in
	// memory. A misconfigured baseURL aimed at a host that answers 200 with a
	// large payload — the case TestListRejectsAnythingThatIsNotAListing exists
	// because it happens — would otherwise be buffered whole before anything
	// got the chance to say it was not a listing, twice per symbol now that the
	// two granularities run concurrently, and once per pool worker at Stage 7.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListingPage+1))
	if err != nil {
		return nil, fmt.Errorf("listing %q: reading response: %w", prefix, err)
	}

	// Reading one byte past the cap is what makes "too large" detectable at
	// all: a read stopping exactly at the limit cannot tell a body of exactly
	// that size from one that was cut short, and silently truncated XML is the
	// worst of the available failures — it parses as a short listing.
	if len(body) > maxListingPage {
		return nil, fmt.Errorf(
			"listing %q: response is larger than %d bytes, which is not a listing", prefix, maxListingPage)
	}

	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("listing %q: parsing response: %w", prefix, err)
	}

	return &result, nil
}

// s3StatusError turns a non-200 listing response into an error that says what
// to do about it. The response body is XML describing the failure, and S3's
// diagnostics are genuinely good — worth the twenty lines to surface rather
// than discard.
//
// # It drains but does not close
//
// The caller's deferred Close owns that half. This is the opposite of
// [Downloader.statusError], which drains *and* closes because its own callers
// have no defer to rely on — and the two contracts being opposite is exactly
// why this function no longer shares that name.
//
// It was previously a method on *Lister that never touched its receiver, so two
// functions called statusError sat in one package, distinguished only by
// whether a call site wrote a dot. A reader could not tell which body contract
// applied without going to look, and moving a call from one to the other would
// have leaked a connection or closed one twice, in silence, on the error path
// that runs during an outage. Different jobs get different names.
func s3StatusError(prefix string, resp *http.Response) error {
	// The body is read whole rather than through bodySnippet, because it has to
	// be parsed as XML before it can be judged unreadable. The read is bounded
	// at 8 KiB, so drain takes care of an error document larger than that:
	// without it, net/http closes the connection instead of pooling it, and
	// every non-200 answer costs a fresh TCP and TLS handshake — during an
	// outage, which is when they arrive in bursts.
	//
	// Closing is the caller's deferred job, not this function's.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	drain(resp)

	if err != nil {
		return fmt.Errorf("listing %q: unexpected status %s", prefix, resp.Status)
	}

	var s3e s3Error
	if err := xml.Unmarshal(body, &s3e); err != nil || s3e.Code == "" {
		// Not an S3 error document. The likeliest cause is that baseURL points
		// somewhere that is not S3 at all, so the first line of the body is far
		// more useful than the status alone. snippetOf returns it quoted and
		// escaped, which is what keeps a body that is not text at all — and a
		// body that is not an S3 error document is exactly the candidate — from
		// putting raw control bytes on someone's terminal.
		snippet := snippetOf(body)
		if snippet == "" {
			return fmt.Errorf("listing %q: unexpected status %s", prefix, resp.Status)
		}

		return fmt.Errorf("listing %q: unexpected status %s: %s", prefix, resp.Status, snippet)
	}

	// PermanentRedirect carries the correct endpoint in the body. This is what
	// a bucket migrated to another region looks like, and quoting the endpoint
	// turns the fix into a one-line configuration change.
	if s3e.Endpoint != "" {
		return fmt.Errorf("listing %q: %s: %s (bucket now at %s)",
			prefix, s3e.Code, s3e.Message, s3e.Endpoint)
	}

	return fmt.Errorf("listing %q: %s: %s", prefix, s3e.Code, s3e.Message)
}
