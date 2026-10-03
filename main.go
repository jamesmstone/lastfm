// Command lastfm scrapes a Last.fm listening history into one JSON file per
// day and builds the SQLite database the Datasette site serves from them.
//
//	lastfm fetch    # fill in any missing days, then refresh the most recent
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
	defaultDataDir = "data/lastfm"
	defaultDB      = "music.db"
	defaultUser    = "jamesmstone711"
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
	dataDir := fs.String("data", defaultDataDir, "directory of scraped day files")
	path := fs.String("db", defaultDB, "database to write")
	if err := fs.Parse(args); err != nil {
		return err
	}

	start := time.Now()
	stats, err := musicdb.Build(*dataDir, *path)
	if err != nil {
		return err
	}
	log.Printf("built %s from %s in %s", *path, stats, time.Since(start).Round(time.Millisecond))
	return nil
}

func fetch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir, "directory of scraped day files")
	user := fs.String("user", defaultUser, "Last.fm username")
	from := fs.String("from", firstDay, "first day to scrape (YYYY-MM-DD)")
	to := fs.String("to", "", "last day to scrape (YYYY-MM-DD, default today)")
	refresh := fs.Int("refresh", 2, "re-scrape this many of the most recent days, which may have gained plays")
	workers := fs.Int("workers", 4, "days to scrape at once")
	if err := fs.Parse(args); err != nil {
		return err
	}

	apiKey := os.Getenv("LASTFM_API_KEY")
	if apiKey == "" {
		return errors.New("LASTFM_API_KEY is not set")
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

	client := lastfm.NewClient(apiKey, *user)
	defer client.Close()
	store := lastfm.Store{Dir: *dataDir}

	// The last few days are re-scraped unconditionally: a day file written
	// while the day was still running is missing everything played since.
	days := lastfm.Days(start, end)
	stale := days[max(len(days)-*refresh, 0):]
	var todo []time.Time
	for _, day := range days {
		if !store.Has(day) {
			todo = append(todo, day)
		}
	}
	for _, day := range stale {
		if store.Has(day) {
			todo = append(todo, day)
		}
	}
	if len(todo) == 0 {
		log.Print("nothing to scrape")
		return nil
	}

	log.Printf("scraping %d of %d days as %s", len(todo), len(days), *user)
	if err := eachDay(ctx, todo, *workers, func(day time.Time) error {
		tracks, err := client.Day(ctx, day)
		if err != nil {
			return err
		}
		return store.Write(day, tracks)
	}); err != nil {
		return err
	}
	log.Printf("scraped %d days", len(todo))
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
