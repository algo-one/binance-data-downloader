package vision

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// This file holds the retry loop shared by every request this package makes —
// object listings and object downloads alike.
//
// # Why retry at all
//
// The bucket is a static file server, so the failures worth retrying are not
// Binance's logic going wrong; they are the network in between. A TCP reset
// mid-transfer, a TLS handshake that times out, a 503 from a load balancer
// shedding load, a 429 because forty goroutines all started at once. Every one
// of those succeeds on a second attempt, and every one of them fails a whole
// backtest if it does not get one.
//
// # Why a helper and not a RoundTripper
//
// Retrying inside a custom http.RoundTripper would give every request in the
// process retries for free, which sounds strictly better. It is not, for two
// reasons that matter here. A transport sees bytes, not meaning: it cannot know
// that a 404 on an archive is a routine fact needing no retry while a 404 on a
// listing is a misconfiguration. And a retry that happens invisibly inside the
// transport cannot be tested by counting calls at the call site, which is
// exactly how the tests in this package pin the behaviour.
//
// So the loop is an ordinary function that callers pass a request to, and the
// decisions it makes are visible where they are made.

// Retry tuning that is not worth a knob.
const (
	// maxRetryAfter caps how long a Retry-After header can park us.
	//
	// The header is the server's own instruction and is normally honoured
	// exactly — it knows better than any local guess when it will be ready.
	// The cap exists because a misconfigured proxy answering "Retry-After:
	// 86400" would otherwise hang a backtest for a day. Beyond this, failing
	// and letting the caller decide is the better answer.
	maxRetryAfter = 30 * time.Second
)

// Policy is how [doWithRetry] behaves: how many attempts, how long between
// them, and — for tests — where the clock and the randomness come from.
//
// The zero Policy is not usable directly; every function taking one calls
// [Policy.withDefaults] first, so a caller that wants the defaults can pass
// Policy{} and a caller that wants to change one field can set just that one.
// This is the same convention [NewLister] uses for its base URL and client, and
// it is the lightweight cousin of the functional-options pattern the public
// API uses: fine for an internal struct, too rigid for an exported one.
type Policy struct {
	// MaxAttempts is the total number of tries, not the number of retries. 1
	// means "no retrying"; 0 means "use the default".
	MaxAttempts int

	// BaseDelay is the first backoff interval. Each subsequent attempt doubles
	// it, up to MaxDelay.
	BaseDelay time.Duration

	// MaxDelay caps the exponential growth. Without a cap, attempt eight of an
	// eight-attempt policy would wait over a minute.
	MaxDelay time.Duration

	// Jitter maps the computed delay ceiling to the delay actually waited.
	//
	// This is not decoration. Without it, a bounded pool that starts forty
	// downloads at once and receives forty 503s retries all forty at exactly
	// the same instant, and keeps doing so in lockstep — a thundering herd
	// that recreates the overload it is backing off from. The default is
	// "full jitter": a uniform draw from [0, ceiling), which spreads the herd
	// across the whole window.
	//
	// Tests set this to the identity function to make delays exact.
	Jitter func(time.Duration) time.Duration

	// After is the timer. It exists as a field so that tests can advance time
	// without spending it: the suite asserts on the delays that were requested
	// rather than sleeping through them.
	//
	// Defaulting to time.After is safe from Go 1.23 onward — before that, the
	// timer it created stayed alive until it fired, so an abandoned time.After
	// in a select leaked until its deadline passed. This module's floor is
	// 1.24, so the timer is garbage-collected as soon as nothing references it.
	After func(time.Duration) <-chan time.Time

	// Now reads the wall clock, and is used for exactly one thing: converting
	// an HTTP-date Retry-After header into a duration. It is injected for the
	// same reason as After — a test asserting on a date-form Retry-After must
	// not depend on what time it is when the suite runs.
	Now func() time.Time

	// Reserve, when set, is called before every attempt and must not return
	// until the request it is about to permit fits inside whatever budget it
	// is keeping. Nil means unmetered, which is what the bucket endpoints use:
	// a static file server publishes no quota.
	//
	// # Why the reservation lives here rather than at the call site
	//
	// Because the thing being counted is a request, and this is the only
	// function that knows how many of those an attempt-carrying call makes.
	// [API.Klines] reserved once, before calling in, and the number looked
	// right — one call, one reservation — right up until the endpoint started
	// failing. A retryable status turns one reservation into as many as
	// MaxAttempts requests, and it does so precisely when the budget matters
	// most, because 429 and every retryable 5xx are the statuses that trigger
	// the extra attempts. The limiter that exists to pre-empt an IP ban was
	// what permitted the burst that earns one.
	//
	// # And why this is not a second backoff
	//
	// The obvious objection to reserving before a *retry* is that the attempt
	// is already waiting out its own delay, so a limiter wait on top of it
	// applies two delays for one problem. It does not, in the case that
	// matters: the reservation returns immediately whenever the budget has
	// room, which after several hundred milliseconds of backoff it normally
	// does. When it does not have room, waiting is not a duplicated penalty —
	// it is the pipeline being over budget, which is the one condition this
	// hook exists to notice.
	Reserve func(context.Context) error
}

// DefaultPolicy is the retry behaviour used when a caller passes the zero
// Policy: four attempts, 500 ms doubling to a 8 s ceiling, full jitter.
//
// Four attempts with these delays spans at most 500 ms + 1 s + 2 s ≈ 3.5 s of
// waiting, which is long enough to ride out a load balancer restarting and
// short enough that a genuinely dead endpoint is reported while someone is
// still watching the terminal.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 4,
		BaseDelay:   500 * time.Millisecond,
		MaxDelay:    8 * time.Second,
		Jitter:      fullJitter,
		After:       time.After,
		Now:         time.Now,
	}
}

// withDefaults returns p with every zero field replaced by its default.
//
// The receiver is a value and the result is returned rather than mutated in
// place, so this is safe to call on a Policy the caller still holds: they get
// their struct back unchanged, we get a complete one. A pointer receiver that
// filled the caller's struct in would work too, and would surprise anyone who
// passed the same Policy to two Listers.
func (p Policy) withDefaults() Policy {
	d := DefaultPolicy()

	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}

	if p.BaseDelay <= 0 {
		p.BaseDelay = d.BaseDelay
	}

	if p.MaxDelay <= 0 {
		p.MaxDelay = d.MaxDelay
	}

	if p.Jitter == nil {
		p.Jitter = d.Jitter
	}

	if p.After == nil {
		p.After = d.After
	}

	if p.Now == nil {
		p.Now = d.Now
	}

	return p
}

// fullJitter draws a uniform delay from [0, ceiling).
//
// "Full jitter" rather than the more obvious "ceiling ± 10%": AWS measured both
// and found that partial jitter still leaves the herd clustered, because every
// client's delay is drawn from a narrow band around the same centre. Spreading
// across the entire window is what actually decorrelates them.
//
// The randomness is math/rand rather than crypto/rand: this picks a delay, not
// a key. A predictable jitter is not a vulnerability, and the cryptographic
// generator would buy nothing here but syscalls.
//
//nolint:gosec // G404: see the paragraph above.
func fullJitter(ceiling time.Duration) time.Duration {
	if ceiling <= 0 {
		return 0
	}

	// rand.N is math/rand/v2's generic uniform draw over any integer-like
	// type, so it works on a time.Duration directly rather than needing a
	// conversion to int64 and back. The global functions in math/rand/v2 are
	// safe for concurrent use, which matters: every goroutine in the pool
	// calls this.
	return rand.N(ceiling)
}

// backoff returns the delay ceiling before the given attempt number, where
// attempt 1 is the first try.
//
// The shift is guarded rather than trusted: 1<<62 nanoseconds overflows a
// time.Duration into a negative number, and a negative delay is a timer that
// fires immediately — a retry storm produced by the code meant to prevent one.
// Capping the exponent before shifting removes that whole class of bug.
func (p Policy) backoff(attempt int) time.Duration {
	const maxShift = 16 // 500 ms << 16 ≈ 9 hours; MaxDelay clamps far below

	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}

	if shift > maxShift {
		shift = maxShift
	}

	d := p.BaseDelay << shift
	if d > p.MaxDelay || d <= 0 {
		d = p.MaxDelay
	}

	return p.Jitter(d)
}

// retryAfterDelay turns the server's Retry-After hint into a delay actually
// worth waiting.
//
// The header is an instruction and is honoured as a floor — retrying earlier
// than the server asked is the one thing that would be rude — but it is not
// taken as the whole answer, for two reasons the raw value gets wrong.
//
// # A bare header is a lockstep retry
//
// Forty pool workers hitting the rate limiter at once each receive the same
// "Retry-After: 1", and taking it literally makes all forty re-fire at the same
// instant. That is precisely the thundering herd [Policy.Jitter] exists to
// break up, reassembled by the code meant to defer to the server. Jitter is
// therefore added *on top of* the header's value rather than replacing it, so
// the herd spreads forward across a window and nobody retries early.
//
// The spread is drawn against BaseDelay rather than the header's own value: the
// server said when it expects to be ready, and stretching that by a factor of
// its own size would turn a 30 s hint into a minute of extra waiting.
//
// # A zero is not "do not wait"
//
// retryAfter reports (0, true) for a literal "Retry-After: 0" and for any
// HTTP-date already in the past — which a clock a couple of seconds fast
// produces from a perfectly ordinary "two seconds from now". Taken literally
// that is a zero delay, and [Policy.wait] short-circuits on anything at or
// below zero, so every remaining attempt would fire back to back at a server
// that had just said it was overloaded. Flooring at BaseDelay keeps "retry
// promptly" from becoming "retry immediately, three more times".
func (p Policy) retryAfterDelay(d time.Duration) time.Duration {
	// Order matters: floor first so a zero becomes BaseDelay, then cap, so a
	// misconfigured proxy's "Retry-After: 86400" cannot park a backtest for a
	// day. The jitter is added after the cap and can exceed it by up to one
	// BaseDelay, which is the point — a cap every worker lands on exactly is
	// the same herd in slower motion.
	d = max(d, p.BaseDelay)
	d = min(d, maxRetryAfter)

	return d + p.Jitter(p.BaseDelay)
}

// wait blocks for d, or until ctx is done.
//
// This is the shape every "sleep" in a context-aware program takes. A bare
// time.Sleep cannot be interrupted, so a cancelled backtest would still sit out
// the full backoff before noticing — the select makes cancellation immediate.
//
// The context deadline also does the work of bounding Retry-After for free: a
// server asking for 30 s inside a request with 5 s left returns the context's
// error at 5 s rather than overrunning it.
func (p Policy) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		// Still a cancellation point. Returning nil here without checking
		// would let a cancelled context perform one more request.
		return ctx.Err()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.After(d):
		return nil
	}
}

// doWithRetry performs req, retrying transport failures and retryable statuses
// according to p.
//
// # What it returns
//
//	resp, nil   a response was received; its status may still be an error
//	nil, err    no response could be obtained, or the context ended
//
// The caller, not this function, decides what a status means. That split is
// deliberate: a 404 means "this archive was never published" to the downloader
// and "the prefix is wrong" to the lister, and only they can say so. What this
// function guarantees is that a *retryable* status has already been retried as
// many times as the policy allows, so a 503 arriving here is a 503 that
// persisted.
//
// # The request must be replayable
//
// Retrying means sending the same request again, which is only sound if the
// request can be sent again. Every request in this package is a bodyless GET,
// so this is free; the check below states the requirement rather than assuming
// it, because a future POST with a one-shot body would otherwise be retried as
// an empty request and fail in a way that looks like a server bug.
func doWithRetry(ctx context.Context, client *http.Client, req *http.Request, p Policy) (*http.Response, error) {
	p = p.withDefaults()

	if req.Body != nil && req.GetBody == nil {
		return nil, fmt.Errorf("retrying %s: request body cannot be replayed", req.URL)
	}

	var lastErr error

	for attempt := 1; ; attempt++ {
		// Before the request, every time, including the first. See
		// Policy.Reserve for why the count belongs to attempts rather than to
		// calls.
		if p.Reserve != nil {
			if err := p.Reserve(ctx); err != nil {
				return nil, fmt.Errorf("%s: %w", req.URL, err)
			}
		}

		// Clone per attempt rather than reusing the same *http.Request. The
		// transport is allowed to modify a request it is given (it sets Host
		// and connection headers), and a request that has been through one
		// round trip is not documented to be reusable. Cloning also re-binds
		// the context, which costs nothing here since it is the same one.
		attemptReq := req.Clone(ctx)

		// Clone is a deep copy of everything *except* the body: it copies the
		// Body field, which is an io.ReadCloser, so both requests hold the same
		// reader. After one round trip that reader is at EOF and net/http has
		// closed it, so a second attempt would send a zero-length payload — the
		// failure the guard above exists to prevent, arriving anyway.
		//
		// GetBody is the standard library's answer: net/http.NewRequest
		// installs one for the body types it can rewind, and it returns a fresh
		// reader over the same bytes. Calling it is what makes the guard's
		// requirement true rather than merely stated.
		//
		// Only on a retry. Attempt 1 sends the caller's own body, which is
		// already positioned at the start, and swapping it out there would
		// leave the original reader unread and unclosed.
		if attempt > 1 && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("retrying %s: rewinding request body: %w", req.URL, err)
			}

			attemptReq.Body = body
		}

		resp, err := client.Do(attemptReq)
		switch {
		case err != nil:
			// A cancelled or expired context is not a failure to retry — it
			// is an instruction to stop. Checking ctx rather than the error's
			// type catches every wrapping net/http applies on the way out.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			lastErr = err

		case !retryableStatus(resp.StatusCode):
			// Success, or a failure no amount of waiting will fix. Either way
			// the response belongs to the caller now, including its body.
			return resp, nil

		default:
			// A retryable status. If this was the last attempt the caller gets
			// the response anyway, body unread, so it can report what the
			// server actually said — returning a synthesised error here would
			// throw away the one diagnostic that matters.
			if attempt >= p.MaxAttempts {
				return resp, nil
			}
		}

		if attempt >= p.MaxAttempts {
			return nil, fmt.Errorf("%s: after %d attempts: %w", req.URL, attempt, lastErr)
		}

		delay := p.backoff(attempt)

		if resp != nil {
			if d, ok := retryAfter(resp.Header.Get("Retry-After"), p.Now()); ok {
				delay = p.retryAfterDelay(d)
			}

			// The body has to be dealt with before the next attempt, and
			// draining it is what lets the connection be reused instead of
			// torn down and re-dialled. Retries are exactly when connection
			// reuse matters most: the server is already struggling.
			drainAndClose(resp)
		}

		if err := p.wait(ctx, delay); err != nil {
			return nil, err
		}
	}
}

// retryableStatus reports whether waiting and asking again could plausibly
// produce a different answer.
//
// The list is short on purpose. Everything absent from it — 404 above all — is
// a fact about the request rather than a hiccup, and retrying it wastes time to
// arrive at the same answer while looking, in a log, exactly like a network
// problem.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout, // 408: the server gave up waiting for us
		http.StatusTooEarly,            // 425: replay protection; try again
		http.StatusTooManyRequests,     // 429: slow down, then continue
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503: the common one under load
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}

// retryAfter interprets the Retry-After header, which RFC 9110 defines in two
// forms that look nothing alike:
//
//	Retry-After: 120                                (delta seconds)
//	Retry-After: Wed, 18 Aug 2026 07:28:00 GMT      (an HTTP date)
//
// Both are in live use, so both are handled. A header that parses as neither is
// reported as absent rather than as an error: a malformed hint should fall back
// to the ordinary backoff, never fail the request.
func retryAfter(header string, now time.Time) (time.Duration, bool) {
	if header == "" {
		return 0, false
	}

	if secs, err := strconv.Atoi(header); err == nil {
		if secs < 0 {
			return 0, false
		}

		return time.Duration(secs) * time.Second, true
	}

	// http.ParseTime accepts all three date formats HTTP allows, which is why
	// it exists rather than a bare time.Parse with one layout.
	t, err := http.ParseTime(header)
	if err != nil {
		return 0, false
	}

	// A date already in the past means "retry now", not "retry in the past".
	if d := t.Sub(now); d > 0 {
		return d, true
	}

	return 0, true
}
