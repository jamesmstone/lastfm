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
	"strconv"
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

// Days returns every date from start to end inclusive.
func Days(start, end time.Time) []time.Time {
	var days []time.Time
	for d := start.UTC().Truncate(24 * time.Hour); !d.After(end); d = d.AddDate(0, 0, 1) {
		days = append(days, d)
	}
	return days
}
