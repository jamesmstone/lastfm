# lastfm

Tracking my lastfm listen history

Inspired by this project: [Git scraping: track changes over time by scraping to a Git repository](https://simonwillison.net/2020/Oct/9/git-scraping/)

The scrape is the source of truth, and it is split at the point a day stops
changing:

- `data/lastfm/<date>.json` — settled days, written once and never rewritten.
- `data/pending/<date>.json` — the last couple of days, still gaining plays.
  `lastfm fetch` re-scrapes these every run and moves them across once they are
  over.

That split is what lets `nix build .#musicdb` be a real build output: the
settled half changes when a day is added, not every time the tail is
re-scraped. The database itself is not committed — what lastfm.jamesst.one
serves is built by the scraper, which also has the pending days, and installed
into `/var/lib/datasette-sites/lastfm/music.db`.

```bash
LASTFM_API_KEY=... nix run . -- fetch   # settle, backfill, refresh the tail
nix run . -- build                      # rebuild music.db from both halves, ~4s
nix build .#musicdb                     # the settled half alone, reproducible
./datasette.sh                          # browse it on :8001
nix flake check                         # build, test, gofmt, and the scraper VM test
```

`nixosModules.scraper` runs the whole thing from a systemd timer — see
`nix/scrape.sh` for what one run does. Point it at the archive and give it a key:

```nix
services.lastfm-scraper = {
  enable = true;
  remote = "ssh://forgejo@hermanii/james/lastfm";
  apiKeyFile = config.sops.secrets.lastfm-api-key.path;
  publish = "/var/lib/datasette-sites/lastfm/music.db";
};
```

`nix develop` gives you a shell with Go, a C toolchain for go-sqlite3, and the
`sqlite3` CLI. The binary needs the `sqlite_fts5` build tag, so build it with
`nix build` rather than a bare `go build`.
