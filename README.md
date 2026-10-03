# lastfm

Tracking my lastfm listen history

Inspired by this project: [Git scraping: track changes over time by scraping to a Git repository](https://simonwillison.net/2020/Oct/9/git-scraping/)

`data/lastfm/<date>.json` is the raw scrape and the source of truth. `lastfm
fetch` fills in any day that is missing and re-scrapes the last couple, which
may still be gaining plays; `lastfm build` rebuilds `music.db` from every day on
disk. The database itself is not committed — the scrape publishes it to
`/var/lib/datasette-sites/lastfm/music.db`, which lastfm.jamesst.one serves.

```bash
LASTFM_API_KEY=... nix run . -- fetch   # scrape into data/lastfm
nix run . -- build                      # rebuild music.db, ~5s for the whole archive
./datasette.sh                          # browse it on :8001
nix flake check                         # build, vet and test
```

`nix develop` gives you a shell with Go, a C toolchain for go-sqlite3, and the
`sqlite3` CLI. The binary needs the `sqlite_fts5` build tag, so build it with
`nix build` rather than a bare `go build`.
