{
  description = "Scrape a Last.fm listening history and build the Datasette database from it";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forEachSystem = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Only the Go sources: including data/ would copy the whole archive into
      # the store and rebuild the binary every time a day is scraped.
      src = lib.fileset.toSource {
        root = ./.;
        fileset = lib.fileset.unions [
          ./go.mod
          ./go.sum
          ./main.go
          ./internal
        ];
      };

      # The settled half of the archive, as its own store path so that
      # re-scraping the pending tail does not invalidate the database below.
      # Rooted at the directory itself, not the flake, for the same reason.
      settled = lib.fileset.toSource {
        root = ./data/lastfm;
        fileset = ./data/lastfm;
      };
    in
    {
      packages = forEachSystem (pkgs: rec {
        default = lastfm;

        lastfm = pkgs.buildGoModule {
          pname = "lastfm";
          version = "0.1.0";
          inherit src;

          vendorHash = "sha256-q9BCDpLndmvFq67BZEyA8i4q/gyDwiTa7+Sz3JlrnQM=";

          # go-sqlite3 compiles FTS5 out unless it is asked for, and the
          # database exposes full-text search over the track, album and artist
          # names.
          tags = [ "sqlite_fts5" ];
          ldflags = [
            "-s"
            "-w"
          ];

          meta = {
            description = "Last.fm scraper and music.db builder";
            mainProgram = "lastfm";
          };
        };

        # The database over the settled days alone: reproducible, and rebuilt
        # when a day is added rather than every time the tail is re-scraped.
        # It is deliberately a day or two behind — what the site serves is built
        # by the scraper, which also has the pending days.
        musicdb = pkgs.runCommand "music.db" { } ''
          mkdir -p pending
          ${lib.getExe lastfm} build -data ${settled} -pending pending -db $out
        '';

        # What the systemd unit runs. A package rather than a script inlined in
        # the module, so the unit's PATH is the one the VM test exercises.
        scrape = pkgs.writeShellApplication {
          name = "lastfm-scrape";
          runtimeInputs = [
            lastfm
            pkgs.coreutils
            pkgs.git
            pkgs.openssh
          ];
          text = builtins.readFile ./nix/scrape.sh;
          meta.description = "fetch the latest plays, commit them, rebuild and publish music.db";
        };
      });

      nixosModules.scraper = import ./nix/scraper-module.nix { inherit self; };

      checks = forEachSystem (
        pkgs:
        {
          # buildGoModule runs `go test` as part of this.
          build = self.packages.${pkgs.system}.lastfm;
          inherit (self.packages.${pkgs.system}) musicdb scrape;

          gofmt = pkgs.runCommandLocal "lastfm-gofmt" { nativeBuildInputs = [ pkgs.go ]; } ''
            unformatted=$(cd ${src} && gofmt -l .)
            if [ -n "$unformatted" ]; then
              echo "not gofmt'd:" >&2
              echo "$unformatted" >&2
              exit 1
            fi
            touch $out
          '';
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # Drives the real unit, so the scraper's PATH and sandbox are exercised
          # rather than the builder's. No API key and no network: the window is
          # one day that is already on disk, so there is nothing to scrape and the
          # run is all git, build and publish.
          scraper = pkgs.testers.runNixOSTest {
            name = "lastfm-scraper";

            nodes.machine = {
              imports = [ self.nixosModules.scraper ];
              environment.systemPackages = [
                pkgs.git
                pkgs.sqlite
              ];
              services.lastfm-scraper = {
                enable = true;
                user = "root";
                remote = "/srv/archive.git";
                stateDir = "/var/lib/lastfm-scraper";
                publish = "/srv/site/music.db";
                from = "2020-01-01";
                to = "2020-01-01";
                refresh = 0;
              };
              # Started by the test, not by the clock.
              systemd.timers.lastfm-scraper.wantedBy = lib.mkForce [ ];
            };

            testScript = ''
              day = """[{"artist":{"url":"u","name":"X","image":[],"mbid":""},"date":{"uts":"1577836800","#text":"01 Jan 2020, 00:00"},"mbid":"t","name":"One","image":[],"url":"tu","streamable":"0","album":{"mbid":"","#text":"A"},"loved":"0"}]"""

              machine.wait_for_unit("multi-user.target")

              # Seeded as a pending day, so the run has something to settle and
              # therefore something to commit and push.
              machine.succeed("mkdir -p /srv/seed/data/pending")
              machine.succeed(f"cat > /srv/seed/data/pending/2020-01-01.json <<'JSON'\n{day}\nJSON")
              machine.succeed(
                  "git -C /srv/seed init -q -b main",
                  "git -C /srv/seed add -A",
                  "git -C /srv/seed -c user.name=t -c user.email=t@t commit -qm seed",
                  "git clone -q --bare /srv/seed /srv/archive.git",
              )

              machine.succeed("systemctl start lastfm-scraper.service")

              with subtest("the day it settled was pushed"):
                  tracked = machine.succeed("git -C /srv/archive.git ls-tree -r --name-only main")
                  assert "data/lastfm/2020-01-01.json" in tracked, tracked
                  assert "data/pending/2020-01-01.json" not in tracked, tracked
                  machine.succeed("git -C /srv/archive.git log -1 --format=%s | grep -q '^Latest data: '")

              with subtest("the database was published"):
                  machine.succeed("test -s /srv/site/music.db")
                  listens = machine.succeed("sqlite3 /srv/site/music.db 'select count(*) from listens'")
                  assert listens.strip() == "1", listens

              with subtest("a second run has nothing to do and still succeeds"):
                  machine.succeed("systemctl start lastfm-scraper.service")
            '';
          };
        }
      );

      devShells = forEachSystem (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gcc
            pkgs.sqlite
          ];
        };
      });

      formatter = forEachSystem (pkgs: pkgs.nixfmt-tree);
    };
}
