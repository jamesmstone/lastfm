#!/bin/bash
set -e # Exit with nonzero exit code if anything fails
set -o pipefail
set -o errexit

downloadDir="data/lastfm"
dockerSQLUtil="sqlite-utils"

TZ=UTC



fetch(){
  curl --compressed \
       -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:141.0) Gecko/20100101 Firefox/141.0' \
       -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' \
       -H 'Accept-Language: en-US,en;q=0.5' \
       -H 'Accept-Encoding: gzip, deflate, br, zstd' \
       -H 'Referer: https://html.duckduckgo.com/' \
       -H 'Connection: keep-alive' \
       -H 'Upgrade-Insecure-Requests: 1' \
       -H 'Sec-Fetch-Dest: document' \
       -H 'Sec-Fetch-Mode: navigate' \
       -H 'Sec-Fetch-Site: cross-site' \
       -H 'Sec-Fetch-User: ?1' \
       -H 'DNT: 1' \
       -H 'Sec-GPC: 1' \
       -H 'Priority: u=0, i' \
       -H 'Pragma: no-cache' \
       -H 'Cache-Control: no-cache' \
       "$@"
}

get_lyrics() {
    local artist="$1"
    local title="$2"

    artist_url=$(echo "$artist" | tr '[:upper:]' '[:lower:]' \
                 | sed 's/^the //; s/[^a-z0-9]//g')
    title_url=$(echo "$title" | tr '[:upper:]' '[:lower:]' \
                | sed 's/[^a-z0-9]//g')

    url="https://www.azlyrics.com/lyrics/${artist_url}/${title_url}.html"

    if [[ -z "$url" ]]; then
        echo "No results found."
        return 1
    fi

    fetch "$url" | sed -n '/<!-- Usage of azlyrics.com content/,/-->/ { /<!-- Usage/ d; /-->/ d; p }'
}


add_lyrics_to_tracks() {
    set -x
    local db="$1"

    # Ensure columns exist
    sql-utils "$db" "alter table tracks add column lyrics text;"
    sql-utils "$db" "alter table tracks add column attempts integer;"

    # Select only tracks that need lyrics
    sql-utils "$db" \
        "select id, artists_id, name
         from tracks
         where (attempts is null or attempts < 3)
           and (lyrics is null or lyrics = '')" |
    while IFS=$'\t' read -r track_id artist_id track_name; do
        # Get artist name
        artist_name=$(sql-utils "$db" "select name from artists where id=$artist_id" --csv --no-headers)

        echo "Fetching lyrics for: $artist_name - $track_name" >&2

        # Get lyrics (may fail)
        lyrics=$(get_lyrics "$artist_name" "$track_name" || true)

        # Escape quotes for SQL
        lyrics_escaped=$(printf "%s" "$lyrics" | sed "s/'/''/g")

        # Update lyrics & attempts
        if [ -n "$lyrics" ]; then
            sql-utils "$db" \
                "update tracks
                 set lyrics='$lyrics_escaped',
                     attempts=coalesce(attempts,0)+1
                 where id=$track_id"
        else
            sql-utils "$db" \
                "update tracks
                 set attempts=coalesce(attempts,0)+1
                 where id=$track_id"
        fi
        sleep 2
    done
}



function buildDocker() {
  docker build --tag "$dockerSQLUtil" --file sqlite-utils.Dockerfile .
}


function sql-utils() {
  docker run \
    -i \
    -u"$(id -u):$(id -g)" \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerSQLUtil" \
    "$@"
}

function ensureDownloadDir() {
  mkdir -p "$downloadDir"
}

function download() {
  curl --connect-timeout 40 \
    --max-time 300 \
    --retry 5 \
    --retry-delay 5 \
    --retry-max-time 40 \
    --silent \
    --show-error \
    --fail \
    --proto =http,https \
    "$@"
}

function downloadDate() {
  local yesterday=$(date -d@"$(($(date +%s) - (24 * 60 * 60)))" +"%Y-%m-%d")

  local start=${1:-$yesterday}

  local startUnix=$(date -d"$start" "+%s")
  local endUnix=$(date -d@"$(($startUnix + (24 * 60 * 60)))" "+%s")

  local page="1"
  local totalPages="2"
  local recenttracks="[]"
  while [ $page -le "$totalPages" ]; do
    local url="https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user=jamesmstone711&api_key=$LASTFM_API_KEY&format=json&from=$startUnix&to=$endUnix&limit=20&extended=1&page=$page"
    local res=$(download "$url")
    totalPages=$(echo $res | jq -r ".recenttracks[\"@attr\"].totalPages")

    local tracks=$(echo $res | jq -r ".recenttracks.track")
    page=$(($page + 1))
    local recenttracks=$(jq -s '.[0] + [.[1]] | flatten' <(echo "$recenttracks") <(echo "$tracks"))
    sleep 0.2
  done
  echo $recenttracks
}

downloadDateToFile() {
  local date=${1}
  local file="$downloadDir/$date.json"
  ensureDownloadDir
  downloadDate "$date" >"$file"
}

function ensureHaveDate() {
  local date=${1}
  local file="$downloadDir/$date.json"
  if [ -f "$file" ]; then
    return 0
  fi
  downloadDateToFile "$date"
}

function ensureHaveAllSinceDate() {
  local today=$(date "+%Y-%m-%d")
  local startDate=${1}
  local endDate=${2:-$today}

  local curUnix=$(date -d"$startDate" "+%s")
  local endUnix=$(date -d"$endDate" "+%s")

  while [[ $curUnix -le $endUnix ]]; do
    local curDate=$(date -d@"$curUnix" "+%Y-%m-%d")
    echo "ensuring have: $curDate" >&2
    ensureHaveDate "$curDate"

    curUnix=$(date -d@"$(($curUnix + (24 * 60 * 60)))" "+%s")

  done
}

function addAll() {
     find "$downloadDir" -name '*.json' -exec jq .[] -c {} \; |
       jq -s |
       sql-utils insert "$db" "listens" - \
        --flatten \
        --alter
}

makeDB() {
  local db="$1"
  rm -rf "$db" || true
  addAll "$db"
  sql-utils extract "$db" listens --table tracks artist_name artist_url artist_image artist_mbid 'album_#text' album_mbid mbid name image streamable url
  sql-utils extract "$db" tracks --table artists artist_name artist_url artist_image artist_mbid
  sql-utils extract "$db" tracks --table albums 'album_#text' album_mbid artists_id

  sql-utils transform "$db" albums \
    --rename 'album_#text' name \
    --rename album_mbid mbid

  sql-utils transform "$db" artists \
    --rename artist_name name \
    --rename artist_url url \
    --rename artist_image image \
    --rename artist_mbid mbid

  sql-utils "$db" "create table avg_estimated_duration as
                   with next_song_listen as (select tracks_id,
                                                    date_uts                                      as start_date_uts,
                                                    lead(date_uts) over ( order by date_uts asc ) as next_listen_uts
                                             from listens
                                             where date_uts is not null),
                        listen_length as (select tracks_id,
                                                 start_date_uts,
                                                 case
                                                     when next_listen_uts - start_date_uts > 10 * 60 -- 10 mins
                                                         then null
                                                     else next_listen_uts - start_date_uts end as estimated_duration
                                          from next_song_listen),
                        avg_track_duration as (select tracks_id,
                                                         avg(estimated_duration) as avg_estimated_duration,
                                                         count(tracks_id) as plays
                                                  from listen_length
                                                  group by 1)
                   select tracks_id, avg_estimated_duration, plays, plays * avg_estimated_duration as total_estimated_listen_time
                   from avg_track_duration mtd
                            inner join tracks t on t.id = mtd.tracks_id;"

  sql-utils "$db" "create table tracks_with_avg_estimated_duration as
                  select * from tracks t inner join avg_estimated_duration med on t.id = med.tracks_id;"
  sql-utils "$db" "drop table tracks ;"
  sql-utils "$db" "drop table avg_estimated_duration;"
  sql-utils "$db" "alter table tracks_with_avg_estimated_duration rename to tracks;"

  sql-utils enable-fts "$db" tracks name
  sql-utils enable-fts "$db" artists name
  sql-utils enable-fts "$db" albums name
  sql-utils create-index --if-not-exists "$db" listens date_uts

  sql-utils "$db" "alter table albums add column plays"
  sql-utils "$db" "update albums set plays=( select sum(t.plays) from tracks t where t.albums_id = albums.id )"
  
  sql-utils "$db" "alter table artists add column plays"
  sql-utils "$db" "update artists set plays=( select sum(a.plays) from albums a where a.artists_id = artists.id )"
  
  sql-utils create-view "$db" listen_details "select
  l.*,
  t.*,
  album.*,
  artist.*
from
  listens l
  inner join tracks t on l.tracks_id = t.id
  left join albums album on t.albums_id = album.id
  left join artists artist on album.artists_id = artist.id"
  
  add_lyrics_to_tracks "$db"

  sql-utils optimize "$db"
}

commitDB() {
  local dbBranch="db"
  local db="$1"
  local tempDB="$(mktemp)"
  git branch -D "$dbBranch" || true
  git checkout --orphan "$dbBranch"
  mv "$db" "$tempDB"
  rm -rf *
  mv "$tempDB" "$db"
  git add "$db"
  git commit "$db" -m "push db"
#  git push origin "$dbBranch" -f
}
commitData() {
  git config user.name "Automated"
  git config user.email "actions@users.noreply.github.com"
  git add -A
  timestamp=$(date -u)
  git commit -m "Latest data: ${timestamp}" || exit 0
#  git push
}

publishDB() {
  local dockerDatasette="datasette"
  docker build --tag "$dockerDatasette" --pull --file datasette.Dockerfile .
  docker run \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerDatasette"\
    publish vercel "$db" \
      --project=lastfmlog \
      --generate-vercel-json > vercel.json
  sed -i 's/@vercel\/python@3\.0\.7/@vercel\/python@4.7.0/g' vercel.json
  docker run \
    -v"$(pwd):/wd" \
    -w /wd \
    "$dockerDatasette" \
    publish vercel "$db" --vercel-json=vercel.json --token $VERCEL_TOKEN --project=lastfmlog --install=datasette-vega
}
set -x

run() {
  local db="music.db"
  local today=$(date "+%Y-%m-%d")
  local yesterday=$(date -d@"$(($(date +%s) - (24 * 60 * 60)))" +"%Y-%m-%d")

  local startDate="2012-11-01"

  ensureHaveAllSinceDate "$startDate"
#   force re download
  downloadDateToFile "$yesterday"
  downloadDateToFile "$today"

  set -x # debug: print commands before they are executed

  commitData

  makeDB "$db"
  publishDB "$db"
  commitDB "$db"

}

buildDocker
run "$@"
