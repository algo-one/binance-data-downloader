package vision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// This file talks to the REST mirror, which is a different service from the
// bucket the rest of this package reads.
//
// # Why there is a REST half at all
//
// The archives lag real time by roughly a day: verified on 2026-08-17, the
// 2026-08-16 daily archive existed and the 2026-08-17 one did not. Anything
// newer than the frontier has never been published as a file and can only be
// paginated out of the API. The same endpoint is also the last rung of the
// planner's fallback ladder, because 3d, 1w and 1mo have no daily archives —
// so a hole in their monthly ones, like the real missing BTCUSDT 1mo 2024-03,
// has nowhere else to go.
//
// # Why it is in this package rather than a new one
//
// It needs the retry loop, the shared http.Client and the body draining that
// already live here, and none of those are worth having twice. What it does
// not need is any domain type — it takes strings and returns strings — so it
// satisfies the rule in docs/architecture.md that decides what may live under
// internal/. The half that speaks Kline is restapi.go in the root package.
//
// # Everything here is a string
//
// A row comes back as a JSON array mixing numbers and quoted decimals, and the
// obvious decode into []any would turn every price into a float64 on the way
// past. That is the one thing this library exists not to do: a real BTCUSDT
// monthly quote volume is 118661604939.99255335, which float64 rounds. So the
// numbers are never decoded as numbers here at all. They are lifted out as the
// exact characters Binance sent and parsed once, by the same code that parses
// the CSV archives.

// DefaultAPIBaseURL is Binance's read-only market-data mirror.
//
// It is the public half of the trading API — no key, no signature, market data
// only — and it is a genuinely different service from [DefaultDownloadBaseURL]
// despite the shared domain. In particular it enforces the trading API's rate
// limits, which the static bucket does not; see limiter.go.
const DefaultAPIBaseURL = "https://data-api.binance.vision"

// klinesPath is the endpoint, relative to the base URL.
const klinesPath = "api/v3/klines"

// KlineFields is how many columns one kline row carries.
//
// Twelve, the same twelve in the same order as the CSV inside an archive —
// verified against the live endpoint on 2026-08-20. That is what lets the root
// package feed a REST row and an archive row to one decoder instead of
// maintaining two that must agree.
const KlineFields = 12

// MaxKlinesLimit is the largest page the endpoint will serve. Documented as
// "Default: 500; Maximum: 1000".
const MaxKlinesLimit = 1000

// maxKlinesResponse bounds a response body before it is read.
//
// A maximal page is 1000 rows of roughly 180 bytes, so about 180 KB. Four
// mebibytes is twenty times that: room for a column layout that grows, and
// still a limit rather than a hope that whatever answered the request was the
// endpoint we asked for.
const maxKlinesResponse = 4 << 20

// ErrIPBanned reports an HTTP 418: the address is banned, not merely throttled.
//
// Binance escalates a 429 that the client keeps ignoring into this, and the ban
// runs from two minutes to three days depending on how often it has happened.
// It is kept distinct from [ErrRateLimited] because the correct response is
// different in kind — there is no backoff short enough to ride it out, and
// retrying is what earns the next, longer one. Errors carrying it are
// [*RateLimitError], so a caller that only asks "should I slow down?" still
// finds ErrRateLimited through the same value.
var ErrIPBanned = errors.New("ip banned")

// ErrBadRequest reports a 4xx: the request was understood and refused. An
// unknown symbol and an unsupported interval both land here.
//
// Retrying cannot help, which is why it is separate from the statuses
// [retryableStatus] lists. When the body carried Binance's own explanation the
// error is an [*APIError] naming the code it returned; when it did not, the
// status alone is the verdict. That distinction deliberately does not change
// the sentinel — whose fault a refusal is depends on the status class, not on
// whether the server chose to explain itself.
var ErrBadRequest = errors.New("request rejected")

// ErrServerError reports a 5xx: Binance's side failed, and nothing about the
// request needs changing.
//
// It exists because the alternative is worse than having no sentinel at all.
// [retryableStatus] already retries 500, 502, 503 and 504, and when those
// attempts are exhausted [doWithRetry] hands back the response so the caller
// can report what the server actually said. Binance answers a 5xx with the same
// {"code","msg"} document it uses for a 400 — {"code":-1001,"msg":"Internal
// error; unable to process your request."} — so a status-blind reading of the
// body reports an outage as the caller's own bug, and the root package's
// ErrInvalidRequest documents itself as "always the caller's to fix". A worker
// pool told that would refuse to retry or fall back precisely when both would
// have worked.
//
// It is not translated into a root-package sentinel. The public vocabulary in
// errors.go says nothing about server-side failure, and an error that arrives
// unrecognised is a smaller lie than one that arrives mislabelled.
var ErrServerError = errors.New("server error")

// ErrMalformedResponse reports that a 200 carried bytes this package could not
// read as a klines response: not JSON, not an array of arrays, a row with the
// wrong number of columns, or a column holding something no number can be
// recovered from.
//
// It is the JSON counterpart of a corrupt archive, and the root package maps it
// onto exactly that sentinel. Without it the two halves of one condition part
// company: a price that will not parse as a decimal is caught by codec.go and
// reported as ErrCorruptArchive, while a body that is not JSON at all reaches
// the caller untyped — the same "Binance sent bytes this library cannot
// understand" arriving as two different answers depending on which layer
// noticed first.
var ErrMalformedResponse = errors.New("malformed response")

// APIError is a refusal the REST API described in its body, in the shape it
// documents:
//
//	{"code":-1121,"msg":"Invalid symbol."}
//
// Verified against the live endpoint on 2026-08-20, which answers exactly that
// with HTTP 400 for a symbol that does not exist.
//
// The code is carried rather than interpreted. This package has no way to know
// whether -1121 means the caller typed the symbol wrong or the pair was
// delisted last week, and inventing a distinction here would put a guess where
// the root package can make an informed decision.
type APIError struct {
	// Status is the HTTP status line, kept because a 400 and a 403 with the
	// same body mean different things.
	Status string

	// StatusCode is the same status as a number, and it decides which sentinel
	// [APIError.Unwrap] reports. Binance uses one body shape for refusals and
	// for its own failures, so the status class is the only thing in the
	// response that says whose fault it is.
	StatusCode int

	// Code and Msg are Binance's own. Code is negative in every documented
	// case; a zero Code means the body did not parse as an error document.
	Code int
	Msg  string
}

func (e *APIError) Error() string {
	if e.Code != 0 {
		return fmt.Sprintf("%s: %s (code %d): %s", e.Status, e.Msg, e.Code, e.cause())
	}

	return fmt.Sprintf("%s: %s", e.Status, e.cause())
}

// Unwrap is what makes errors.Is(err, ErrBadRequest) true for this type — or
// errors.Is(err, ErrServerError), for the statuses where the failure is
// Binance's rather than ours.
func (e *APIError) Unwrap() error { return e.cause() }

// cause picks the sentinel from the status class, so that the message and the
// value a caller branches on cannot disagree: both ask this one function.
func (e *APIError) cause() error { return statusCause(e.StatusCode) }

// statusCause is the rule for whose fault a failed request was, in the one
// place both users of it can read.
//
// The rule is the status class and nothing else. A 4xx means the server
// understood the request and refused it; a 5xx means the server failed. Binance
// describes both in the same {"code","msg"} document, so the body cannot be
// asked this question — and the two callers here are the typed error and the
// untyped fallback, which have to answer it identically or an endpoint's choice
// of formatting starts deciding what a caller is told.
func statusCause(code int) error {
	if code >= http.StatusInternalServerError {
		return ErrServerError
	}

	return ErrBadRequest
}

// RawKline is one row exactly as Binance sent it: twelve fields, every one of
// them the original characters.
//
// An array rather than a slice, so the length is part of the type and a short
// row cannot be constructed by accident. Numeric columns hold their literal
// text — "1704067200000", "47134" — and quoted columns hold their contents with
// the quotes and any escapes resolved.
type RawKline [KlineFields]string

// KlineQuery is one page of klines to ask for.
//
// Start and End are half-open, [Start, End), matching the rest of this project
// — and deliberately not matching the endpoint, whose startTime and endTime are
// both inclusive. The conversion happens in one place, in [API.Klines], rather
// than at every call site where a forgotten adjustment would duplicate or drop
// a single candle at the seam.
type KlineQuery struct {
	// Symbol is the pair in Binance's own spelling, "BTCUSDT". It is not
	// normalised here; the root package does that before building a query.
	Symbol string

	// Interval is the REST spelling of the interval, which differs from the
	// archive spelling for exactly one value: a month is "1M" here and "1mo"
	// in a bucket path. See binancedata.Interval.RESTParam.
	Interval string

	// Start and End bound the page. A zero End means "no upper bound", which
	// the endpoint reads as "up to the present".
	Start time.Time
	End   time.Time

	// Limit is how many rows to return, at most [MaxKlinesLimit]. Zero means
	// the endpoint's own default of 500.
	Limit int
}

// values renders the query as the endpoint's parameters.
//
// # The two off-by-one conversions
//
// startTime and endTime are inclusive and expressed in whole milliseconds,
// while Start and End are half-open and carry nanosecond precision. Both
// mismatches can move a candle across the boundary, and both are handled here
// so that no caller has to think about it:
//
//   - End is exclusive, so the last instant to ask for is the millisecond
//     before it. Subtracting a nanosecond and then truncating gives the
//     largest millisecond strictly below End, whether or not End sits on a
//     millisecond boundary.
//   - Start is inclusive, and truncating it downwards would ask for a candle
//     opening before the range — one that the decoder would then correctly
//     reject as outside the period it was told to expect. Rounding up asks
//     for exactly the candles the range contains.
func (q KlineQuery) values() url.Values {
	v := url.Values{}
	v.Set("symbol", q.Symbol)
	v.Set("interval", q.Interval)

	if !q.Start.IsZero() {
		startMillis := q.Start.UnixMilli()
		if q.Start.After(time.UnixMilli(startMillis)) {
			startMillis++
		}

		v.Set("startTime", strconv.FormatInt(startMillis, 10))
	}

	if !q.End.IsZero() {
		v.Set("endTime", strconv.FormatInt(q.End.Add(-time.Nanosecond).UnixMilli(), 10))
	}

	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}

	return v
}

// validate rejects a query that cannot describe a page, before a request is
// spent discovering it.
func (q KlineQuery) validate() error {
	if q.Symbol == "" {
		return errors.New("klines: symbol is required")
	}

	if q.Interval == "" {
		return errors.New("klines: interval is required")
	}

	if q.Limit < 0 || q.Limit > MaxKlinesLimit {
		return fmt.Errorf("klines: limit %d is outside 1..%d", q.Limit, MaxKlinesLimit)
	}

	if !q.Start.IsZero() && !q.End.IsZero() && !q.Start.Before(q.End) {
		return fmt.Errorf("klines: start %s is not before end %s",
			q.Start.Format(time.RFC3339), q.End.Format(time.RFC3339))
	}

	return nil
}

// KlinesPage is what one call returned.
type KlinesPage struct {
	// Klines are the rows, in the order the endpoint sent them — which is
	// ascending by open time, though nothing here relies on that. The root
	// package verifies the ordering itself, because a page that repeated or
	// reversed a candle would otherwise stall the pagination loop.
	Klines []RawKline

	// UsedWeight is the X-MBX-USED-WEIGHT-1M header: how much of the
	// [WeightLimitPerMinute] quota this IP has spent in the current minute,
	// counting this request. Zero when the header was absent or unreadable.
	//
	// It is decoded rather than acted on. The limiter's job is to keep this
	// number low, and this is the only way to notice when that is not working
	// — a second process on the same address spending the same quota, most
	// likely, which no amount of local accounting can detect.
	//
	// Nothing reads it yet. restapi.go drops the field, because a fetcher
	// returning candles has nowhere to put a diagnostic; Stage 7 owns progress
	// reporting and is where it becomes visible. Recording that here rather
	// than claiming the diagnosis already works is the difference between a
	// decoded value with a known consumer and one that is quietly ignored.
	UsedWeight int
}

// API reads klines from the REST mirror.
//
// Like [Lister] and [Downloader] it holds its client rather than making one, so
// that every request in the process shares a connection pool. Unlike them it is
// paced, for the reason limiter.go opens with: this endpoint has a quota and
// the bucket does not.
//
// The limiter is not a field here. It is captured by the Reserve closure on the
// policy, which is the only thing that reads it — a second reference on the
// struct would be state that looks consultable and never is.
type API struct {
	baseURL string
	client  *http.Client
	policy  Policy
}

// NewAPI returns an API reading from baseURL using client, retrying per p and
// pacing itself with lim.
//
// An empty baseURL means [DefaultAPIBaseURL], a nil client means the
// process-wide client from [NewHTTPClient], the zero Policy means
// [DefaultPolicy], and a nil limiter means the process-wide limiter — which is
// the one default that is load-bearing rather than convenient. The quota is per
// IP address, so two APIs each pacing themselves correctly still exceed it
// together; sharing one limiter is what makes the accounting add up.
func NewAPI(baseURL string, client *http.Client, p Policy, lim *rate.Limiter) *API {
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}

	if client == nil {
		client = defaultClient()
	}

	if lim == nil {
		lim = defaultLimiter()
	}

	policy := p.withDefaults()

	// The limiter is installed on the policy rather than consulted here,
	// because [doWithRetry] is the layer that knows how many requests one call
	// becomes. Set unconditionally: pacing this endpoint is the API's own
	// business, and a caller who supplied a Policy with its own Reserve would
	// be quietly opting out of the quota that earns IP bans.
	policy.Reserve = func(ctx context.Context) error {
		// WaitN blocks until KlinesWeight units of budget are available, or
		// ctx ends. It takes its tokens before waiting rather than after, so
		// ten goroutines arriving together leave in an orderly queue instead
		// of all observing the same empty bucket and firing at the same
		// instant — and it puts them back if the wait is cancelled.
		return lim.WaitN(ctx, KlinesWeight)
	}

	return &API{baseURL: baseURL, client: client, policy: policy}
}

// Klines fetches one page.
//
// The rate limiter is not consulted here. It is installed on the policy by
// [NewAPI] and spent inside [doWithRetry], once per HTTP request rather than
// once per call — the difference being that a call which retries makes several,
// and does so exactly when the quota is under pressure. See Policy.Reserve.
//
// The validation below runs first, so a query that could never be sent costs no
// budget: the cheapest request is the one refused before it exists.
func (a *API) Klines(ctx context.Context, q KlineQuery) (KlinesPage, error) {
	if err := q.validate(); err != nil {
		return KlinesPage{}, err
	}

	endpoint, err := url.JoinPath(a.baseURL, klinesPath)
	if err != nil {
		return KlinesPage{}, fmt.Errorf("klines: building URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"?"+q.values().Encode(), nil)
	if err != nil {
		return KlinesPage{}, fmt.Errorf("klines: building request: %w", err)
	}

	resp, err := doWithRetry(ctx, a.client, req, a.policy)
	if err != nil {
		return KlinesPage{}, fmt.Errorf("klines %s %s: %w", q.Symbol, q.Interval, err)
	}

	if resp.StatusCode != http.StatusOK {
		return KlinesPage{}, a.statusError(q, resp)
	}

	defer drainAndClose(resp)

	rows, err := decodeKlines(resp.Body)
	if err != nil {
		return KlinesPage{}, fmt.Errorf("klines %s %s: %w", q.Symbol, q.Interval, err)
	}

	return KlinesPage{Klines: rows, UsedWeight: usedWeight(resp.Header)}, nil
}

// statusError turns a non-200 into an error carrying the right sentinel, and
// consumes the body on every path — the same contract as the downloader's, and
// for the same reason: the error path is the one that runs during an outage,
// and a body left unread there costs the connection.
func (a *API) statusError(q KlineQuery, resp *http.Response) error {
	label := fmt.Sprintf("klines %s %s", q.Symbol, q.Interval)

	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusTeapot:
		hint, _ := retryAfter(resp.Header.Get("Retry-After"), a.policy.Now())

		drainAndClose(resp)

		// 418 is the escalation of 429 and is reported as its own condition.
		// A caller that only wants to know whether to slow down finds
		// ErrRateLimited either way; one that can tell the difference should,
		// because no backoff rides out a ban measured in hours.
		if resp.StatusCode == http.StatusTeapot {
			return &RateLimitError{Key: label, RetryAfter: hint, Banned: true}
		}

		return &RateLimitError{Key: label, RetryAfter: hint}

	case http.StatusNotFound:
		drainAndClose(resp)

		return fmt.Errorf("%s: %w", label, ErrNotFound)

	default:
		// Everything else — every 4xx this package has no specific handling
		// for, and every 5xx that outlived the retry loop. Binance explains
		// both in the body, so the body is where the diagnosis is; the status
		// class is what decides whose fault it is. readAPIError drains and
		// closes.
		return fmt.Errorf("%s: %w", label, readAPIError(resp))
	}
}

// readAPIError builds the error for a status this package has no specific
// handling for, preferring Binance's own explanation when the body carries one.
//
// It takes ownership of resp.
//
// # The body decides the detail, the status decides the verdict
//
// Those are two questions and it is worth keeping them apart. Whether the
// result is an [*APIError] naming a code, or a quoted snippet of whatever
// arrived, depends on what the body turned out to be. Which sentinel it carries
// does not: a 400 is the caller's problem and a 503 is Binance's, whether or not
// either explained itself. Deciding the sentinel from the body instead — the
// shape this started as — meant an endpoint that answered a 400 with HTML
// produced an untyped error, so whether a caller could tell "this is my bug"
// came down to how the server felt like formatting its refusal.
func readAPIError(resp *http.Response) error {
	// Read a bounded prefix before deciding what it is: the body has to be
	// consumed either way, and reading it once serves both the JSON parse and
	// the fallback snippet. The limit is the document's rather than the
	// snippet's, because a document truncated to snippet length is not a
	// shorter document, it is invalid JSON — see [maxErrorDoc].
	b := readBodyPrefix(resp, maxErrorDoc)

	var doc struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}

	// A code of zero means the body was valid JSON but not an error document —
	// an empty object, or something else entirely — so it falls through to the
	// snippet rather than being reported as "code 0".
	if err := json.Unmarshal(b, &doc); err == nil && doc.Code != 0 {
		return &APIError{Status: resp.Status, StatusCode: resp.StatusCode, Code: doc.Code, Msg: doc.Msg}
	}

	// No usable document, so the status is the whole diagnosis. It still
	// carries the sentinel its class implies.
	cause := statusCause(resp.StatusCode)

	if snippet := snippetOf(b); snippet != "" {
		return fmt.Errorf("unexpected status %s: %s: %w", resp.Status, snippet, cause)
	}

	return fmt.Errorf("unexpected status %s: %w", resp.Status, cause)
}

// decodeKlines reads the response body into rows of exact strings.
//
// # Why json.RawMessage and not []any
//
// A row is a JSON array mixing two types: numbers for the two timestamps and
// the trade count, quoted strings for the eight decimals. Decoding it into
// []any is the one-line version, and it converts every one of those numbers to
// a float64 — including, if the shape ever changed, a price. This package's
// whole reason for existing is that float64 cannot hold what Binance publishes.
//
// json.RawMessage is the escape hatch: it implements json.Unmarshaler by
// copying the raw bytes of its element and doing nothing else. So the numbers
// are never interpreted here at all, and the strings that come out are the
// exact characters that arrived — ready for the same decimal parser the
// archives go through.
func decodeKlines(r io.Reader) ([]RawKline, error) {
	var raw [][]json.RawMessage

	// The limit is what stops a misrouted request from streaming an unbounded
	// body into memory. json.Decoder rather than ReadAll plus Unmarshal so
	// that the bytes are never held twice.
	if err := json.NewDecoder(io.LimitReader(r, maxKlinesResponse)).Decode(&raw); err != nil {
		// %w twice: the decoder's own message says where in the body it gave
		// up, which is the diagnostic, while [ErrMalformedResponse] is the
		// condition a caller branches on. Losing either would be a loss.
		return nil, fmt.Errorf("decoding response: %w: %w", err, ErrMalformedResponse)
	}

	rows := make([]RawKline, 0, len(raw))

	for i, fields := range raw {
		if len(fields) != KlineFields {
			return nil, fmt.Errorf("row %d has %d fields, want %d: %w",
				i+1, len(fields), KlineFields, ErrMalformedResponse)
		}

		var row RawKline

		for j, f := range fields {
			s, err := jsonScalar(f)
			if err != nil {
				return nil, fmt.Errorf("row %d field %d: %w: %w", i+1, j+1, err, ErrMalformedResponse)
			}

			row[j] = s
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// jsonScalar renders one JSON value as the text it stands for.
//
// A quoted string yields its contents with escapes resolved, which is what
// makes "0.01634790" arrive as 0.01634790. Anything unquoted — a number, in
// practice — yields its literal characters untouched, which is the point: the
// digits Binance sent are the digits that get parsed, with no numeric type in
// between to round them.
//
// Anything that is not a number or a string is refused rather than rendered:
// null, an object, an array, and the two booleans. None appears in a kline row,
// and turning one into a plausible-looking string would hand the decimal parser
// something to fail on two layers away from the cause — `open "true": invalid
// syntax`, reported against a column that was never a number to begin with.
func jsonScalar(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("empty value")
	}

	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", fmt.Errorf("%w", err)
		}

		return s, nil

	case '{', '[', 'n', 't', 'f':
		return "", fmt.Errorf("expected a number or a string, got %s", snippetOf(raw))

	default:
		return string(raw), nil
	}
}

// usedWeight reads the quota Binance reports it has counted against this IP in
// the current minute.
//
// Absent or unparseable is reported as zero rather than as an error. The header
// is a courtesy — informative when present, never load-bearing — and failing a
// perfectly good response over a missing diagnostic would be the wrong trade.
func usedWeight(h http.Header) int {
	v := h.Get("X-MBX-USED-WEIGHT-1M")
	if v == "" {
		return 0
	}

	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}

	return n
}
