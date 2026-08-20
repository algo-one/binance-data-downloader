package binancedata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/algo-one/binance-data-downloader/internal/vision"
)

// This file is the seam between the transport in internal/vision and the domain
// this package speaks. Three things happen here and nowhere else:
//
//   - a symbol, interval, granularity and period become a bucket key;
//   - a downloaded archive is verified against the checksum Binance published
//     for it, so no unverified bytes reach the parser;
//   - internal/vision's errors become this package's sentinels.
//
// The last one exists because internal/vision cannot import this package — the
// dependency runs the other way — so it cannot return [ErrNotAvailable] itself.
// See docs/architecture.md for why the boundary is drawn where it is.

// archiveRef names one archive Binance publishes.
//
// It is a struct rather than five parameters because these five values travel
// together everywhere below, and because a positional call with two strings and
// two enums next to each other is a swap waiting to happen.
type archiveRef struct {
	Market   Market
	Symbol   string // already normalised: "BTCUSDT", never "BTC/USDT"
	Interval Interval
	Agg      aggregation
	Period   time.Time // the month or day the archive covers, at midnight UTC
}

// key returns the archive's full path within the bucket:
//
//	data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-15.zip
//
// Both halves validate, and between them every one of the five fields is
// checked, so a bad ref is caught here rather than becoming a plausible-looking
// URL that 404s — a 404 this package would then report as [ErrNotAvailable], a
// statement about Binance's calendar standing in for a bug in the caller.
//
// The only field with no check of its own is Period, which needs none: a
// time.Time always formats through the layout, and an out-of-range date names
// an archive that genuinely does not exist. That is a 404 meaning exactly what
// it says.
func (r archiveRef) key() (string, error) {
	prefix, err := archivePrefix(r.Market, r.Symbol, r.Interval, r.Agg)
	if err != nil {
		return "", err
	}

	name, err := archiveName(r.Symbol, r.Interval, r.Agg, r.Period)
	if err != nil {
		return "", err
	}

	// Plain concatenation rather than path.Join: prefix is documented to end
	// in a slash and name is documented to contain none, and path.Join would
	// quietly hide a violation of either by cleaning it away.
	return prefix + name, nil
}

// String implements fmt.Stringer so that a ref reads as itself in a test
// failure. Errors quote the key instead, which is the more useful string when
// the question is "what did it ask the server for?".
func (r archiveRef) String() string {
	return fmt.Sprintf("%s %s %s %s", r.Symbol, r.Interval, r.Agg, r.Period.UTC().Format(time.RFC3339))
}

// fetchArchive downloads one archive into dst and verifies it against the
// SHA-256 Binance published beside it.
//
// # The sidecar is fetched first
//
// It is 91 bytes, and fetching it before the archive buys two things. A month
// that was never published is discovered for the price of one tiny request
// rather than by starting a 93 MB transfer that 404s — and every archive that
// does arrive is compared against a hash that was already in hand, so there is
// no path where bytes are written, the sidecar then fails to load, and the
// caller is left holding an unverified file that looks finished.
//
// A missing sidecar is therefore reported as [ErrNotAvailable], the same as a
// missing archive. The two are published together, so in practice the sidecar's
// absence means the archive is absent too; and in the case where it somehow is
// not, an archive that cannot be verified is one this library will not parse.
//
// # What dst holds afterwards
//
// On success, exactly the bytes Binance served, whose hash matched. On any
// error — including a checksum mismatch — whatever arrived before the failure,
// which the caller must discard. See [vision.Downloader.Download] for why
// streaming makes that the only honest contract, and note that it costs the
// cache nothing: it writes to a temporary file and renames it into place only
// after this function returns nil.
func fetchArchive(ctx context.Context, dl *vision.Downloader, ref archiveRef, dst io.Writer) (vision.Result, error) {
	key, err := ref.key()
	if err != nil {
		return vision.Result{}, err
	}

	published, err := dl.Checksum(ctx, key)
	if err != nil {
		return vision.Result{}, translateVisionError(err)
	}

	res, err := dl.Download(ctx, key, dst)
	if err != nil {
		return vision.Result{}, translateVisionError(err)
	}

	// Both sides are lowercase hex of the same 32 bytes, so a plain string
	// comparison is the whole check. A constant-time comparison would be the
	// habit if this were authenticating anything, but nothing here is secret
	// and the value is public: this detects corruption, not forgery.
	if res.SHA256 != published {
		return vision.Result{}, fmt.Errorf(
			"%s: downloaded %d bytes hashing to %s, published checksum is %s: %w",
			key, res.Size, res.SHA256, published, ErrChecksum)
	}

	return res, nil
}

// translateVisionError re-labels an error from internal/vision with this
// package's vocabulary.
//
// # Two %w verbs
//
// fmt.Errorf accepts more than one %w since Go 1.20, and the resulting error
// wraps both: errors.Is walks to either. That is what is wanted here. The
// transport's message survives — it names the key and the status, which is what
// anyone debugging needs — while the sentinel a caller branches on is attached
// alongside it:
//
//	err := fetchArchive(...)
//	errors.Is(err, ErrNotAvailable)   // true; this is what callers test
//
// The alternative, discarding the original and returning the bare sentinel,
// produces "data not available" with no indication of what was unavailable.
//
// Anything unrecognised is passed through untouched. Inventing a sentinel for
// an error this function does not understand would be a claim it cannot back.
func translateVisionError(err error) error {
	switch {
	case errors.Is(err, vision.ErrNotFound):
		// A 404 is a fact about the calendar, not a failure: the month is not
		// published yet, or the symbol had not been listed on that day.
		return fmt.Errorf("%w: %w", err, ErrNotAvailable)

	case errors.Is(err, vision.ErrRateLimited):
		// The retry policy already backed off inside the request and the 429
		// outlived it. The whole pipeline needs to slow down, which is a
		// decision for the layer that owns the worker pool.
		//
		// An HTTP 418 — the ban Binance escalates a persistent 429 into —
		// arrives here too, because vision.RateLimitError wraps both sentinels
		// and this case tests the coarser one. The distinction is not lost: it
		// stays readable through errors.Is(err, vision.ErrIPBanned) and in the
		// message, which is where a caller that can act on it will look.
		return fmt.Errorf("%w: %w", err, ErrRateLimited)

	case errors.Is(err, vision.ErrBadRequest):
		// A 4xx the REST API explained: an unknown symbol, an interval it does
		// not serve. The server understood the request and refused it, so the
		// fault is on this side and no amount of retrying or falling back to
		// another source changes the answer.
		//
		// Deliberately not ErrNotAvailable. "Binance has not published this"
		// and "you asked for something that does not exist" send whoever is
		// debugging to different places, and only the second one is fixable by
		// the caller.
		return fmt.Errorf("%w: %w", err, ErrInvalidRequest)

	default:
		return err
	}
}
