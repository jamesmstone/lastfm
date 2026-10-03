// Command lastfm scrapes a Last.fm listening history into one JSON file per
// day and builds the SQLite database the Datasette site serves from them.
//
//	lastfm fetch    # settle the days that are over, fill gaps, refresh the tail
//	lastfm build    # rebuild music.db from the days on disk
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"jamesst.one/lastfm/internal/lastfm"
	"jamesst.one/lastfm/internal/musicdb"
)

const (
	// The settled archive: days that are over and will not be written again.
	defaultDataDir = "data/lastfm"
	// The tail still gaining plays. Kept out of the settled archive so a build
	// over that archive alone is reproducible between scrapes.
	defaultPendingDir = "data/pending"
	defaultDB         = "music.db"
	defaultUser       = "jamesmstone711"
	// The first day with any scrobbles on the account.
	firstDay = "2012-11-01"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("lastfm: ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: lastfm (fetch|build) [flags]")
	}
	switch cmd := args[0]; cmd {
	case "fetch":
		return fetch(ctx, args[1:])
	case "build":
		return build(args[1:])
	default:
		return fmt.Errorf("unknown command %q: want fetch or build", cmd)
	}
}

func build(args []string) error {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir, "directory of settled day files")
	pendingDir := fs.String("pending", defaultPendingDir, "directory of day files still being scraped")
	path := fs.String("db", defaultDB, "database to write")
	if err := fs.Parse(args); err != nil {
		return err
	}

	start := time.Now()
	stats, err := musicdb.Build([]string{*dataDir, *pendingDir}, *path)
	if err != nil {
		return err
	}
	log.Printf("built %s from %s in %s", *path, stats, time.Since(start).Round(time.Millisecond))
	return nil
}

func fetch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir, "directory of settled day files")
	pendingDir := fs.String("pending", defaultPendingDir, "directory of day files still being scraped")
	user := fs.String("user", defaultUser, "Last.fm username")
	from := fs.String("from", firstDay, "first day to scrape (YYYY-MM-DD)")
	to := fs.String("to", "", "last day to scrape (YYYY-MM-DD, default today)")
	refresh := fs.Int("refresh", 2, "treat this many of the most recent days as still in progress and re-scrape them")
	workers := fs.Int("workers", 4, "days to scrape at once")
	if err := fs.Parse(args); err != nil {
		return err
	}

	start, err := time.ParseInLocation(lastfm.DateFormat, *from, time.UTC)
	if err != nil {
		return fmt.Errorf("-from: %w", err)
	}
	end := time.Now().UTC().Truncate(24 * time.Hour)
	if *to != "" {
		if end, err = time.ParseInLocation(lastfm.DateFormat, *to, time.UTC); err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}

	archive := lastfm.Archive{
		Settled: lastfm.Store{Dir: *dataDir},
		Pending: lastfm.Store{Dir: *pendingDir},
		// A day file written while the day was still running is missing
		// everything played since, so the last few days are re-scraped and
		// held out of the settled archive until they are over.
		Cutoff: end.AddDate(0, 0, 1-max(*refresh, 0)),
	}

	settled, err := archive.Settle()
	if err != nil {
		return err
	}
	if len(settled) > 0 {
		log.Printf("settled %d days, through %s", len(settled), settled[len(settled)-1].Format(lastfm.DateFormat))
	}

	var todo []time.Time
	for _, day := range lastfm.Days(start, end) {
		if !archive.Has(day) || !day.Before(archive.Cutoff) {
			todo = append(todo, day)
		}
	}
	if len(todo) == 0 {
		log.Print("nothing to scrape")
		return nil
	}

	// Checked here rather than up front so a run with nothing to do needs no
	// credential, which is what lets the scraper be tested without one.
	apiKey := os.Getenv("LASTFM_API_KEY")
	if apiKey == "" {
		return errors.New("LASTFM_API_KEY is not set")
	}

	client := lastfm.NewClient(apiKey, *user)
	defer client.Close()

	log.Printf("scraping %d days as %s", len(todo), *user)
	var short int
	var mu sync.Mutex
	if err := eachDay(ctx, todo, *workers, func(day time.Time) error {
		tracks, err := client.Day(ctx, day)
		if err != nil {
			return err
		}
		change, err := archive.Update(day, tracks)
		if err != nil {
			return err
		}
		if !change.Written {
			mu.Lock()
			short++
			mu.Unlock()
			log.Printf("WARNING: %s came back with %d plays but %d are on disk; leaving it alone",
				day.Format(lastfm.DateFormat), change.Scraped, change.OnDisk)
		}
		return nil
	}); err != nil {
		return err
	}
	log.Printf("scraped %d days, %d left alone", len(todo)-short, short)
	return nil
}

// eachDay runs fn over days with at most workers in flight, stopping at the
// first failure so a transient API outage does not fill the archive with
// half-scraped days.
func eachDay(ctx context.Context, days []time.Time, workers int, fn func(time.Time) error) error {
	workers = max(workers, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queue := make(chan time.Time)
	var wg sync.WaitGroup
	var once sync.Once
	var failure error

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for day := range queue {
				if err := fn(day); err != nil {
					once.Do(func() { failure = err; cancel() })
					return
				}
			}
		}()
	}

	for _, day := range days {
		select {
		case queue <- day:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(queue)
	wg.Wait()

	if failure != nil {
		return failure
	}
	return ctx.Err()
}
