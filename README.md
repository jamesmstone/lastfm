# lastfm

Tracking my lastfm listen history

Inspired by this project: [Git scraping: track changes over time by scraping to a Git repository](https://simonwillison.net/2020/Oct/9/git-scraping/)

The scrape is the source of truth, and it is split at the point a day stops
changing:

- `data/lastfm/<date>.json` — settled days, written once and never rewritten.
- `data/pending/<date>.json` — the most recent days (`-refresh`, or
  `services.lastfm-scraper.refresh` in the module), still gaining plays.
  `lastfm fetch` re-scrapes these every run and moves them across once they are
  over.

What the site serves is built by the scraper, which has both halves, and
installed at `services.lastfm-scraper.publish`.

This started as a git scrape, with the scraper committing every run back to
this repository. It no longer does: the deployed scraper keeps the archive in
`services.lastfm-scraper.stateDir`, which is the only live copy and is backed
up by the host, and it never clones, commits or pushes. Every day up to that
change is still in this repository's history, under `data/`.

```bash
LASTFM_API_KEY=... nix run . -- fetch   # settle, backfill, refresh the tail
nix run . -- build                      # rebuild music.db from both halves
./datasette.sh                          # browse it with Datasette
nix flake check                         # build, test, gofmt, music.db over the fixture, the unit check and the scraper VM test
```

`nixosModules.scraper` runs the whole thing from a systemd timer as its own
system user, sandboxed to its state directory and the directory it publishes
into — see `nix/scrape.sh` for what one run does. Give it a key and somewhere to
publish:

```nix
services.lastfm-scraper = {
  enable = true;
  apiKeyFile = config.sops.secrets.lastfm-api-key.path;
  publish = "/var/lib/dbs/lastfm/music.db";
};
```

`nix develop` gives you a shell with Go, a C toolchain for go-sqlite3, and the
`sqlite3` CLI. The binary needs the `sqlite_fts5` build tag, so build it with
`nix build` rather than a bare `go build`.
