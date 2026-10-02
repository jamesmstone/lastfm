# lastfm

Tracking my lastfm listen history

Inspired by this project: [Git scraping: track changes over time by scraping to a Git repository](https://simonwillison.net/2020/Oct/9/git-scraping/)

`data/lastfm/<date>.json` is the raw scrape and the source of truth. `./run.sh`
refreshes it, rebuilds `music.db` from every day on disk, and commits the JSON
back. The database itself is not committed — the scrape publishes it to
`/var/lib/datasette-sites/lastfm/music.db`, which lastfm.jamesst.one serves.

```bash
nix shell nixpkgs#sqlite-utils nixpkgs#jq nixpkgs#findutils --command ./run.sh   # needs $LASTFM_API_KEY
nix shell nixpkgs#sqlite-utils nixpkgs#jq nixpkgs#findutils --command ./run.sh makeDB music.db
./datasette.sh                                                                  # browse it on :8001
```
