package vision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// DefaultDownloadBaseURL is where the archives themselves are served.
//
// Note that this is Binance's own domain, not the S3 endpoint [DefaultBaseURL]
// uses. Fetching an object and listing objects genuinely go through different
// front doors — see the package comment — and only listing is tied to AWS.
// Downloads keep working through a CDN, a mirror or a migration.
const DefaultDownloadBaseURL = "https://data.binance.vision"

// ChecksumSuffix is appended to an archive's key to get its sidecar's key.
//
//	data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip
//	data/spot/monthly/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01.zip.CHECKSUM
const ChecksumSuffix = ".CHECKSUM"

// MaxChecksumSize bounds how much of a sidecar [ReadChecksum] will read. The
// real files are 91 bytes — 64 hex digits, two spaces and a file name — so
// anything approaching this is not a checksum and there is no reason to buffer
// it.
//
// Exported so the cache's own tests can name the boundary they are crossing
// rather than repeating the number and hoping the two stay equal.
const MaxChecksumSize = 4 << 10

// Errors this package reports as sentinels, for the root package to translate
// into its own. They exist because internal/vision cannot import the root — the
// root imports it — so binancedata.ErrNotAvailable is out of reach from here.
// The translation happens at the boundary, in the root's download.go.
var (
	// ErrNotFound reports a 404: the object is not there. For an archive this
	// is routine rather than exceptional — a month Binance has not published
	// yet, or a day before the symbol was listed — which is why it is a value
	// to branch on and not a failure.
	ErrNotFound = errors.New("object not found")

	// ErrRateLimited reports a 429 that survived the retry policy. Reaching
	// this means backing off inside one request was not enough and the caller
	// should slow the whole pipeline down.
	//
	// Errors carrying it are [*RateLimitError], which also reports how long the
	// server asked for; errors.Is finds this sentinel through that type's
	// Unwrap, so a caller that only wants to know "was it a 429?" needs no
	// knowledge of the struct.
	ErrRateLimited = errors.New("rate limited")
)

// RateLimitError is a 429 that outlived the retry policy, together with the
// server's own estimate of when it will be ready again.
//
// # Why the duration needs a type
//
// The retry loop reads Retry-After to schedule its own backoff and then throws
// the value away, because within one request it has no further use for it. But
// a 429 that survives every attempt is not a fact about one request — it is the
// pipeline as a whole going too fast, and the layer that owns the worker pool
// is the only one that can slow it down. Handing that layer a bare "rate
// limited" tells it to back off without telling it for how long, so the number
// the server actually supplied would have to be guessed at one frame above
// where it arrived.
//
// # Reading it
//
// A caller that only branches on the condition uses errors.Is and never sees
// this type. A caller that wants the duration uses errors.As:
//
//	var rl *vision.RateLimitError
//	if errors.As(err, &rl) && rl.RetryAfter > 0 {
//	    pool.PauseFor(rl.RetryAfter)
//	}
//
// The pointer receiver on Error is why errors.As wants **RateLimitError: the
// method set containing Error belongs to *RateLimitError, so that — not the
// bare struct — is what implements error.
type RateLimitError struct {
	// Key is the object whose request was throttled.
	Key string

	// RetryAfter is what the server's Retry-After header asked for, or 0 when
	// it sent none or sent one that had already elapsed.
	//
	// It is reported exactly as received, not clamped. maxRetryAfter bounds
	// what this package will itself wait inside one request; how long a whole
	// pipeline should pause is the pool's policy to set, and quietly rounding a
	// misconfigured proxy's 24 hours down to 30 seconds here would hide the
	// misconfiguration rather than surface it. Callers should bound it.
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%q: %s, retry after %s", e.Key, ErrRateLimited, e.RetryAfter)
	}

	return fmt.Sprintf("%q: %s", e.Key, ErrRateLimited)
}

// Unwrap is what makes errors.Is(err, ErrRateLimited) true for this type.
// Returning the sentinel rather than embedding it keeps the two ways of asking
// — the condition and the detail — from needing to agree about anything beyond
// this one method.
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

// Result describes what one download produced.
type Result struct {
	// SHA256 is the hash of the bytes as they were received, in lowercase hex
	// — the same form Binance writes into the .CHECKSUM sidecar, so the two
	// compare with ==.
	SHA256 string

	// Size is how many bytes were written to the destination.
	Size int64
}

// Downloader fetches single objects out of the bucket.
//
// Like [Lister] it holds a client rather than creating one, so that every
// request in the process shares one connection pool. The two types are separate
// because they talk to different hosts, and collapsing them would hide that.
type Downloader struct {
	baseURL string
	client  *http.Client
	policy  Policy
}

// NewDownloader returns a Downloader reading from baseURL using client.
//
// An empty baseURL means [DefaultDownloadBaseURL], a nil client means the
// process-wide client from [NewHTTPClient], and the zero Policy means
// [DefaultPolicy]. Every test in this package aims baseURL at an
// httptest.Server.
//
// The nil client deliberately does not mean http.DefaultClient: that would hand
// back a transport keeping two idle connections per host, which is the very bug
// correctness requirement 8 names. See [defaultClient].
func NewDownloader(baseURL string, client *http.Client, p Policy) *Downloader {
	if baseURL == "" {
		baseURL = DefaultDownloadBaseURL
	}

	if client == nil {
		client = defaultClient()
	}

	return &Downloader{baseURL: baseURL, client: client, policy: p.withDefaults()}
}

// Download writes the object at key into dst and returns its SHA-256 and size.
//
// # The hash is computed in the same pass as the write
//
// io.MultiWriter feeds each block of the body to both the destination and the
// hasher, so the bytes are hashed as they stream past rather than by reading
// the file back afterwards. A 93 MB archive is therefore hashed for free, in
// terms of both I/O and memory: nothing is buffered, and the peak footprint is
// one 32 KiB copy buffer regardless of how large the archive is. This is what
// makes verifying every download affordable enough to be non-optional.
//
// # What dst holds after an error
//
// Whatever arrived before the failure. A partially written destination is
// unavoidable when streaming — the alternative is buffering the whole archive
// in memory to keep the write atomic, which is the cost this design exists to
// avoid — so the contract is that the caller discards dst unless Download
// returned nil. That is no burden on the only caller that matters: the cache
// writes to a temporary file and renames it into place only on success, so a
// failed download leaves a temp file it was going to delete anyway.
//
// Retries inside this call therefore cover the request and status phases, which
// is where nearly all failures happen. A connection that dies mid-body is
// reported to the caller rather than restarted, because restarting would need
// to rewind dst and an io.Writer cannot be rewound.
func (d *Downloader) Download(ctx context.Context, key string, dst io.Writer) (Result, error) {
	resp, err := d.get(ctx, key)
	if err != nil {
		return Result{}, err
	}
	defer drainAndClose(resp)

	h := sha256.New()

	// io.Copy uses the reader's WriteTo or the writer's ReadFrom when either
	// is available, and otherwise a 32 KiB buffer. Either way it streams.
	n, err := io.Copy(io.MultiWriter(dst, h), resp.Body)
	if err != nil {
		// Naming the byte count turns "connection reset" into something a
		// reader can act on: reset after 0 bytes is a different problem from
		// reset after 90 MB.
		return Result{}, fmt.Errorf("downloading %q: after %d bytes: %w", key, n, err)
	}

	// A body shorter than its declared length means a truncated transfer.
	// net/http usually reports that itself as an unexpected EOF, but "usually"
	// is not a property to rest a checksum on, and the check is one comparison.
	// ContentLength is -1 when the length was not declared, which is legal.
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return Result{}, fmt.Errorf("downloading %q: got %d bytes, Content-Length said %d", key, n, resp.ContentLength)
	}

	return Result{SHA256: hex.EncodeToString(h.Sum(nil)), Size: n}, nil
}

// Checksum fetches the .CHECKSUM sidecar for archiveKey and returns the
// SHA-256 it contains, in lowercase hex.
//
// archiveKey is the archive's own key; the suffix is appended here so that no
// caller has to remember it, and so the sidecar's file name can be checked
// against the archive it claims to describe. A sidecar naming a different file
// means a misrouted request or a mirror serving stale content, and trusting it
// would compare an archive against another archive's hash — which fails as a
// checksum mismatch and sends whoever investigates hunting for a corruption
// that never happened.
func (d *Downloader) Checksum(ctx context.Context, archiveKey string) (string, error) {
	key := archiveKey + ChecksumSuffix

	resp, err := d.get(ctx, key)
	if err != nil {
		return "", err
	}
	defer drainAndClose(resp)

	sum, err := ReadChecksum(resp.Body, path.Base(archiveKey))
	if err != nil {
		return "", fmt.Errorf("reading %q: %w", key, err)
	}

	return sum, nil
}

// ReadChecksum reads a .CHECKSUM sidecar from r and returns the hash in it.
//
// It is the bounded read that has to happen before [ParseChecksum] can be
// handed a byte slice, and it is exported for the same reason the parser is:
// the cache reads sidecars off disk while this package reads them off the
// network, and the *reading* half of that has the same two decisions in it as
// the parsing half. Keeping them together means the size limit is one constant
// and the "too large" error is one sentence, rather than two of each drifting
// apart in two packages.
//
// The caller supplies the context — a URL here, a file name in the cache — by
// wrapping what comes back, so nothing in this function has to know which it
// was reading.
func ReadChecksum(r io.Reader, wantName string) (string, error) {
	// Reading one byte past the maximum is what makes "too large" detectable:
	// a read that stops exactly at the limit cannot tell a file of exactly
	// that size from one that was cut short.
	b, err := io.ReadAll(io.LimitReader(r, MaxChecksumSize+1))
	if err != nil {
		return "", err
	}

	if len(b) > MaxChecksumSize {
		return "", fmt.Errorf("sidecar is larger than %d bytes", MaxChecksumSize)
	}

	return ParseChecksum(b, wantName)
}

// get performs one GET, with retries, and returns a response whose status is
// 200. Every other status has already been turned into an error.
func (d *Downloader) get(ctx context.Context, key string) (*http.Response, error) {
	endpoint, err := url.JoinPath(d.baseURL, key)
	if err != nil {
		return nil, fmt.Errorf("downloading %q: building URL: %w", key, err)
	}

	// url.JoinPath, not string concatenation: it normalises away a doubled or
	// missing slash between the base and the key, which is otherwise a 404
	// that only appears when someone configures a base URL with a trailing
	// slash.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("downloading %q: building request: %w", key, err)
	}

	resp, err := doWithRetry(ctx, d.client, req, d.policy)
	if err != nil {
		return nil, fmt.Errorf("downloading %q: %w", key, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, d.statusError(key, resp)
	}

	return resp, nil
}

// statusError turns a non-200 into an error carrying the right sentinel, and
// consumes the response body in the process.
//
// It takes ownership of resp precisely because every path here ends with the
// body unread; leaving that to the caller is how a connection gets dropped on
// the error path, which is the path that runs during an outage.
//
// The receiver is here for the clock: reading a date-form Retry-After needs to
// know what time it is, and this package takes that from the policy rather than
// from time.Now so that a test asserting on one is not asserting on when the
// suite happened to run.
func (d *Downloader) statusError(key string, resp *http.Response) error {
	switch resp.StatusCode {
	case http.StatusNotFound:
		drainAndClose(resp)

		return fmt.Errorf("%q: %w", key, ErrNotFound)

	case http.StatusTooManyRequests:
		// Read the header before the body is consumed. Nothing about draining
		// touches the headers, so the order is not load-bearing — but reading
		// what is wanted off a response before disposing of it is the habit
		// that stays correct when the disposal grows a step.
		hint, _ := retryAfter(resp.Header.Get("Retry-After"), d.policy.Now())

		drainAndClose(resp)

		return &RateLimitError{Key: key, RetryAfter: hint}

	default:
		// Anything else is unexpected enough that the body is worth quoting:
		// a 403 from a bucket policy change and a 502 from a proxy look
		// identical without it. bodySnippet drains and closes.
		if snippet := bodySnippet(resp); snippet != "" {
			return fmt.Errorf("%q: unexpected status %s: %s", key, resp.Status, snippet)
		}

		return fmt.Errorf("%q: unexpected status %s", key, resp.Status)
	}
}

// ParseChecksum extracts the hash from a .CHECKSUM sidecar.
//
// The format is the one `sha256sum` writes, which the real files follow
// exactly — 64 hex digits, two spaces, the file name, and, in the files Binance
// publishes, no trailing newline:
//
//	ea666c96c34414515 ... 4d99  BTCUSDT-1h-2024-01-15.zip
//
// The parse accepts more than that: any run of whitespace as the separator, an
// optional trailing newline, and coreutils' "*" binary-mode marker before the
// name. Being liberal about whitespace costs nothing and survives a future
// where the sidecar gains a newline. Being liberal about the *hash* would cost
// everything, so its length and alphabet are checked exactly.
//
// wantName, when non-empty, must match the name the sidecar carries.
//
// It is exported for the cache, which reads sidecars back off disk rather than
// off the network. Two parsers for one format is one parser that gets fixed and
// one that does not, so the local path and the network path share this.
func ParseChecksum(b []byte, wantName string) (string, error) {
	// Only the first line is read. A sidecar with more than one entry is not
	// something Binance publishes, and guessing which line applies would be
	// exactly the kind of silent assumption this library avoids.
	line, _, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")

	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", errors.New("checksum sidecar is empty")
	}

	sum := strings.ToLower(fields[0])

	// hex.DecodeString validates the alphabet; the length check pins it to
	// SHA-256 specifically, so a sidecar that switched to another hash is
	// rejected loudly rather than compared against and failing as a mismatch.
	if len(sum) != hex.EncodedLen(sha256.Size) {
		return "", fmt.Errorf("checksum %q is %d characters, want %d", sum, len(sum), hex.EncodedLen(sha256.Size))
	}

	if _, err := hex.DecodeString(sum); err != nil {
		return "", fmt.Errorf("checksum %q is not hexadecimal: %w", sum, err)
	}

	if wantName == "" || len(fields) < 2 {
		return sum, nil
	}

	// coreutils marks a file hashed in binary mode with a leading asterisk.
	gotName := strings.TrimPrefix(fields[1], "*")
	if gotName != wantName {
		return "", fmt.Errorf("checksum sidecar describes %q, want %q", gotName, wantName)
	}

	return sum, nil
}

// FormatChecksum renders a sidecar in the form Binance publishes: the hash, two
// spaces, the archive's file name, and no trailing newline.
//
// The cache writes one of these beside every archive it stores, so that tier 1
// on disk is the pair of files Binance served rather than the archive plus a
// hash in some format of this project's own invention. Two consequences follow
// from matching the published format byte for byte: `sha256sum -c` verifies a
// cache directory with no tooling from here, and the round trip through
// [ParseChecksum] is exercised against the real fixtures rather than against a
// convention this package agreed with itself.
//
// It lives next to the parser deliberately. A writer and a reader of the same
// format in two different files is how they drift.
func FormatChecksum(sum, name string) string {
	return sum + "  " + name
}
