// Package lastfm fetches a user's listening history from the Last.fm API and
// keeps it on disk as one JSON file per day.
package lastfm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// DateFormat is the layout of both the day file names and the -from/-to flags.
const DateFormat = "2006-01-02"

const (
	endpoint = "https://ws.audioscrobbler.com/2.0/"
	// The API caps a page at 200; fewer, bigger pages means fewer round trips,
	// and the day file is the concatenation of the pages either way.
	pageSize = 200
	maxPages = 1000
	// Last.fm allows roughly five requests a second per key.
	minInterval = 200 * time.Millisecond
)

// Client reads one user's recent tracks.
type Client struct {
	HTTP    *http.Client
	APIKey  string
	User    string
	Retries int
	// BaseURL overrides the API endpoint; tests point it at a local server.
	BaseURL string

	rate *time.Ticker
}

// NewClient returns a Client that reuses connections and gives up on a single
// request after a minute, matching the old curl --max-time budget.
func NewClient(apiKey, user string) *Client {
	return &Client{
		HTTP:    &http.Client{Timeout: 60 * time.Second},
		APIKey:  apiKey,
		User:    user,
		Retries: 5,
		rate:    time.NewTicker(minInterval),
	}
}

// Close releases the rate limiter. It does not cancel requests in flight.
func (c *Client) Close() {
	if c.rate != nil {
		c.rate.Stop()
	}
}

// wait spends one request from the rate limiter, which is shared by every
// goroutine scraping through this client.
func (c *Client) wait(ctx context.Context) error {
	if c.rate == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.rate.C:
		return nil
	}
}

// apiError is the shape Last.fm returns instead of a payload. Its codes 11, 16
// and 29 are the transient ones (service down, backend error, rate limit).
type apiError struct {
	Code    int    `json:"error"`
	Message string `json:"message"`
}

func (e *apiError) Error() string {
	return fmt.Sprintf("last.fm error %d: %s", e.Code, e.Message)
}

func (e *apiError) retryable() bool {
	return e.Code == 11 || e.Code == 16 || e.Code == 29
}

// Day returns every track the user scrobbled on date, as the raw JSON objects
// the API sent. Keeping them raw means fields we do not model still reach disk.
func (c *Client) Day(ctx context.Context, date time.Time) ([]json.RawMessage, error) {
	from := date.UTC().Truncate(24 * time.Hour)
	to := from.Add(24 * time.Hour)

	var tracks []json.RawMessage
	for page, total := 1, 1; page <= total; page++ {
		res, err := c.page(ctx, from, to, page)
		if err != nil {
			return nil, fmt.Errorf("%s page %d: %w", date.Format(DateFormat), page, err)
		}
		tracks = append(tracks, res.tracks...)
		if total = res.totalPages; total > maxPages {
			return nil, fmt.Errorf("%s: implausible totalPages %d", date.Format(DateFormat), total)
		}
	}
	return tracks, nil
}

type pageResult struct {
	tracks     []json.RawMessage
	totalPages int
}

func (c *Client) page(ctx context.Context, from, to time.Time, page int) (pageResult, error) {
	q := url.Values{
		"method":   {"user.getrecenttracks"},
		"user":     {c.User},
		"api_key":  {c.APIKey},
		"format":   {"json"},
		"from":     {strconv.FormatInt(from.Unix(), 10)},
		"to":       {strconv.FormatInt(to.Unix(), 10)},
		"limit":    {strconv.Itoa(pageSize)},
		"extended": {"1"},
		"page":     {strconv.Itoa(page)},
	}

	var last error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<attempt) * time.Second
			select {
			case <-ctx.Done():
				return pageResult{}, ctx.Err()
			case <-time.After(delay):
			}
		}
		base := c.BaseURL
		if base == "" {
			base = endpoint
		}
		res, err := c.get(ctx, base+"?"+q.Encode())
		if err == nil {
			return res, nil
		}
		var retry retryableError
		if !errors.As(err, &retry) {
			return pageResult{}, err
		}
		last = err
	}
	return pageResult{}, fmt.Errorf("giving up after %d attempts: %w", c.Retries+1, last)
}

type retryableError struct{ err error }

func (e retryableError) Error() string { return e.err.Error() }
func (e retryableError) Unwrap() error { return e.err }

func (c *Client) get(ctx context.Context, u string) (pageResult, error) {
	if err := c.wait(ctx); err != nil {
		return pageResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return pageResult{}, err
	}
	req.Header.Set("User-Agent", "lastfm-scraper (+https://lastfm.jamesst.one)")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return pageResult{}, retryableError{err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return pageResult{}, retryableError{err}
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return pageResult{}, retryableError{fmt.Errorf("http %s", resp.Status)}
	}
	if resp.StatusCode != http.StatusOK {
		return pageResult{}, fmt.Errorf("http %s: %s", resp.Status, truncate(body))
	}

	var apiErr apiError
	if err := json.Unmarshal(body, &apiErr); err == nil && apiErr.Code != 0 {
		if apiErr.retryable() {
			return pageResult{}, retryableError{&apiErr}
		}
		return pageResult{}, &apiErr
	}
	return parsePage(body)
}

func parsePage(body []byte) (pageResult, error) {
	var payload struct {
		RecentTracks struct {
			// The API drops the array when a window holds a single track, and
			// omits the key entirely when it holds none.
			Track json.RawMessage `json:"track"`
			Attr  struct {
				TotalPages string `json:"totalPages"`
			} `json:"@attr"`
		} `json:"recenttracks"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return pageResult{}, fmt.Errorf("decode: %w: %s", err, truncate(body))
	}

	res := pageResult{totalPages: 1}
	if s := payload.RecentTracks.Attr.TotalPages; s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			return pageResult{}, fmt.Errorf("totalPages %q: %w", s, err)
		}
		res.totalPages = max(n, 1)
	}

	raw := bytes.TrimSpace(payload.RecentTracks.Track)
	switch {
	case len(raw) == 0 || bytes.Equal(raw, []byte("null")):
	case raw[0] == '[':
		if err := json.Unmarshal(raw, &res.tracks); err != nil {
			return pageResult{}, fmt.Errorf("decode tracks: %w", err)
		}
	default:
		res.tracks = []json.RawMessage{raw}
	}
	return res, nil
}

func truncate(b []byte) string {
	const limit = 200
	if len(b) > limit {
		return string(b[:limit]) + "…"
	}
	return string(b)
}

// Store is a directory of <date>.json day files.
type Store struct{ Dir string }

// Path is the day file for date, whether or not it exists.
func (s Store) Path(date time.Time) string {
	return filepath.Join(s.Dir, date.UTC().Format(DateFormat)+".json")
}

// Has reports whether the day has already been scraped.
func (s Store) Has(date time.Time) bool {
	_, err := os.Stat(s.Path(date))
	return err == nil
}

// Read returns the tracks stored for a day.
func (s Store) Read(date time.Time) ([]json.RawMessage, error) {
	raw, err := os.ReadFile(s.Path(date))
	if err != nil {
		return nil, err
	}
	var tracks []json.RawMessage
	if err := json.Unmarshal(raw, &tracks); err != nil {
		return nil, fmt.Errorf("%s: %w", s.Path(date), err)
	}
	return tracks, nil
}

// Write saves a day's tracks as a JSON array, replacing the file atomically so
// a failed or interrupted scrape can never leave a truncated day behind.
func (s Store) Write(date time.Time, tracks []json.RawMessage) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, t := range tracks {
		if i > 0 {
			buf.WriteByte(',')
		}
		if err := json.Compact(&buf, t); err != nil {
			return err
		}
	}
	buf.WriteByte(']')

	path := s.Path(date)
	tmp, err := os.CreateTemp(s.Dir, ".day-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(buf.Bytes()); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// Remove deletes a day file if it is there.
func (s Store) Remove(date time.Time) error {
	if err := os.Remove(s.Path(date)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Days returns every date from start to end inclusive.
func Days(start, end time.Time) []time.Time {
	var days []time.Time
	for d := start.UTC().Truncate(24 * time.Hour); !d.After(end); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	return days
}

// Archive is the scrape on disk, split at the day it stops changing.
//
// Settled days are written once and never rewritten, which is what lets the
// database be a build output: the directory changes when a day is added, not
// every time the tail is re-scraped. Pending holds the days still gaining
// plays, and Settle moves them across once they are over.
//
// A date lives in exactly one of the two. musicdb reads both and refuses to
// build if that stops being true, because a day present twice would count
// every one of its listens twice.
type Archive struct {
	Settled Store
	Pending Store
	// Cutoff is the first day considered still in progress.
	Cutoff time.Time
}

// store returns the half of the archive a day belongs in.
func (a Archive) store(date time.Time) Store {
	if date.UTC().Before(a.Cutoff.UTC()) {
		return a.Settled
	}
	return a.Pending
}

func (a Archive) other(date time.Time) Store {
	if date.UTC().Before(a.Cutoff.UTC()) {
		return a.Pending
	}
	return a.Settled
}

// Has reports whether the day has been scraped into either half.
func (a Archive) Has(date time.Time) bool {
	return a.Settled.Has(date) || a.Pending.Has(date)
}

// Change is what an Update did.
type Change struct {
	Written bool
	Scraped int // dated plays the scrape returned
	OnDisk  int // dated plays already in the archive
}

// Update writes a day into the half it belongs in, unless doing so would drop
// dated plays that are already on disk.
//
// A scrape can come back short — the API answers 200 with a partial window
// often enough to matter — and a day that exists is never scraped again, so
// overwriting with the shorter answer would lose those plays for good.
func (a Archive) Update(date time.Time, tracks []json.RawMessage) (Change, error) {
	change := Change{Scraped: Dated(tracks)}
	for _, s := range []Store{a.Settled, a.Pending} {
		if !s.Has(date) {
			continue
		}
		old, err := s.Read(date)
		if err != nil {
			return change, err
		}
		change.OnDisk = max(change.OnDisk, Dated(old))
	}
	if change.Scraped < change.OnDisk {
		return change, nil
	}

	if err := a.store(date).Write(date, tracks); err != nil {
		return change, err
	}
	change.Written = true
	// The day may have been settled under an earlier cutoff, or scraped into
	// the archive before this split existed.
	return change, a.other(date).Remove(date)
}

// Settle moves every pending day that is over into the settled half, and
// reports the days it moved.
func (a Archive) Settle() ([]time.Time, error) {
	paths, err := filepath.Glob(filepath.Join(a.Pending.Dir, "*.json"))
	if err != nil {
		return nil, err
	}
	slices.Sort(paths)

	var settled []time.Time
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		date, err := time.ParseInLocation(DateFormat, name, time.UTC)
		if err != nil {
			return nil, fmt.Errorf("%s: not a day file: %w", path, err)
		}
		if !date.Before(a.Cutoff.UTC()) {
			continue
		}
		if a.Settled.Has(date) {
			return nil, fmt.Errorf("%s is both settled and pending; one of them is wrong", name)
		}
		if err := os.MkdirAll(a.Settled.Dir, 0o755); err != nil {
			return nil, err
		}
		if err := os.Rename(path, a.Settled.Path(date)); err != nil {
			return nil, err
		}
		settled = append(settled, date)
	}
	return settled, nil
}

// Dated counts the tracks carrying a scrobble time. Now-playing entries are
// excluded: they are a snapshot of what was on at the moment of the scrape, so
// a day legitimately loses one between scrapes.
func Dated(tracks []json.RawMessage) int {
	var n int
	for _, t := range tracks {
		var play struct {
			Date json.RawMessage `json:"date"`
		}
		if err := json.Unmarshal(t, &play); err != nil {
			continue
		}
		if len(play.Date) > 0 {
			n++
		}
	}
	return n
}
