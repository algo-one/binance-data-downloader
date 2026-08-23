package vision

import (
	"sync"

	"golang.org/x/time/rate"
)

// This file holds the rate limiter, which exists for exactly one endpoint and
// would be pointless anywhere else in this package.
//
// # Why the bucket needs no limiter and the API does
//
// data.binance.vision is a static file server. Listing objects and downloading
// archives are reads of immutable files, Binance documents no quota on them,
// and Stages 2 through 5 never needed one. The REST mirror at
// data-api.binance.vision is a different service that happens to share the
// domain: it is the read-only half of the trading API, and it enforces the
// trading API's quota.
//
// Measured against the live endpoint on 2026-08-20, via
// /api/v3/exchangeInfo?symbol=BTCUSDT:
//
//	REQUEST_WEIGHT   6000 per 1 minute, per IP address
//
// and one klines call costs 2 of it. So the ceiling is 3000 calls a minute,
// shared by every program running on the machine — not per API key, per IP.
//
// # Why exceeding it is worse than slow
//
// Binance's documented escalation is that a 429 you do not back off from
// becomes an HTTP 418, and a 418 is an IP ban lasting from two minutes to three
// days, scaling with repeat offences. That punishes the address, not the
// process: a ban earned by a backtest downloading history also locks out a live
// trading bot on the same host. Retrying after the fact cannot undo it, which
// is why this limiter is preventative rather than reactive. The Retry-After
// handling in retry.go is the reactive half, and by the time it runs the damage
// is already partly done.
//
// # What a token bucket is
//
// The bucket holds up to Burst tokens and refills continuously at Rate tokens
// per second. Spending weight takes that many tokens out; when the bucket is
// empty a caller waits for it to refill enough. So a burst of requests goes out
// at once and a sustained stream settles to exactly Rate.
//
// Nothing is refilled on a timer. The level is computed on demand from the
// instant it was last touched and how much time has passed, which is why the
// type needs no goroutine, no ticker and no cleanup.
//
// # Why x/time/rate rather than fifty lines of our own
//
// This file did originally hold a hand-rolled bucket, for one reason that
// turned out not to survive: the rest of this package injects its clock so that
// tests assert on delays instead of spending them, and rate.Limiter reads
// time.Now() internally.
//
// testing/synctest removes that objection entirely. A test inside a bubble gets
// a fake clock for the whole time package, so rate.Limiter's internal
// time.Now() and time.NewTimer() are already virtual, and the test asserts on
// exact durations while running instantly — see limiter_test.go. With the
// reason for hand-rolling gone, what was left was fifty lines of arithmetic we
// maintain against a canonical implementation by the people who wrote the
// runtime, whose cancellation path is more careful than ours was: it restores
// only the tokens later reservations have not already claimed.
//
// That is the trade this project's `go` directive was moved to 1.25 for.
// See go.mod.

// Rate limiting constants, all measured rather than assumed. See the file
// comment for where the numbers come from.
const (
	// WeightLimitPerMinute is the quota Binance publishes for REQUEST_WEIGHT
	// over a one-minute window, per IP address.
	WeightLimitPerMinute = 6000

	// KlinesWeight is what one /api/v3/klines call costs against that quota.
	KlinesWeight = 2

	// DefaultWeightPerSecond is the sustained rate this library allows itself,
	// in weight units per second.
	//
	// The quota works out to 100 weight per second. This takes 40 of them —
	// 20 klines calls a second, enough to page a two-day tail of 1s candles in
	// about nine seconds — and deliberately leaves the majority unspent.
	//
	// The headroom is not timidity. The quota is per IP, so anything else on
	// the machine draws from the same 6000: a live trading bot, another
	// backtest, a second copy of this library. Consuming the whole budget
	// because we are entitled to it is how a history download gets a trading
	// process banned, and the failure would appear in the trading process's
	// logs rather than in ours.
	DefaultWeightPerSecond = 40

	// DefaultBurst is how much weight may be spent instantaneously after an
	// idle period, in weight units.
	//
	// Ten klines calls. A burst exists so that a short pipeline is not paced
	// artificially — the first ten pages go out immediately and only then does
	// the sustained rate bind — and is kept small so that a saturated worker
	// pool cannot open with a spike large enough to matter.
	DefaultBurst = 20
)

// defaultLimiter is the process-wide limiter, shared by every [API] handed a
// nil one.
//
// Sharing is not an optimisation here, it is the requirement. The quota is
// enforced per IP address, so two limiters in one process each allowing 40
// weight per second permit 80 — a limiter that is correct in isolation and
// wrong in aggregate. sync.OnceValue builds it at most once, on first use,
// exactly as [defaultClient] does for the http.Client.
var defaultLimiter = sync.OnceValue(func() *rate.Limiter {
	return NewLimiter(DefaultWeightPerSecond, DefaultBurst)
})

// NewLimiter returns a limiter allowing ratePerSecond weight per second, with a
// bucket holding burst weight.
//
// A non-positive rate or burst is replaced by the default, so the zero-ish call
// NewLimiter(0, 0) yields the standard policy rather than a limiter that blocks
// forever — which is the shape of mistake that only shows up in production.
// rate.NewLimiter itself has no such guard: a zero rate is rate.Limit(0), which
// refills nothing, and a zero burst rejects every call outright.
//
// It returns *rate.Limiter rather than a wrapper type. A wrapper would exist
// only to rename WaitN, and this package's own constants already carry the
// domain meaning: the unit is weight, and one klines call is [KlinesWeight] of
// it.
func NewLimiter(ratePerSecond float64, burst int) *rate.Limiter {
	if ratePerSecond <= 0 {
		ratePerSecond = DefaultWeightPerSecond
	}

	if burst <= 0 {
		burst = DefaultBurst
	}

	// rate.Limit is a float64 of "events per second", and the events here are
	// weight units rather than requests — so a klines call draws KlinesWeight
	// of them at a time. Counting weight rather than calls is what keeps the
	// accounting right if a second endpoint with a different cost is ever
	// added.
	return rate.NewLimiter(rate.Limit(ratePerSecond), burst)
}

// BurstFor returns the burst that pairs with a sustained rate, in weight units.
//
// The rule is half a second's worth of the rate, never less than one klines
// call. At [DefaultWeightPerSecond] that is exactly [DefaultBurst], which is
// the point: the shipped policy is not a pair of unrelated constants but one
// number and a rule, so a caller who lowers the rate gets a proportionally
// smaller spike rather than the shipped burst on top of their slower refill.
//
// The floor matters more than it looks. rate.Limiter.WaitN returns an error
// rather than waiting when n exceeds the burst, so a bucket smaller than
// [KlinesWeight] would fail every klines call forever instead of pacing it —
// the failure mode a caller asking for a very low rate is least expecting.
func BurstFor(ratePerSecond float64) int {
	burst := int(ratePerSecond / 2)

	return max(burst, KlinesWeight)
}
