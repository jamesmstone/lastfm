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

          vendorHash = "sha256-sgDI6KHLv+HDo861fQ2eIqWOsIMA+WBfcomaj7SJFvQ=";

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
          ];
          text = builtins.readFile ./nix/scrape.sh;
          meta.description = "fetch the latest plays into the state directory, rebuild and publish music.db";
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
          # The unit as a deployment would get it, checked without booting
          # anything: the state directory is the only copy of the archive, so
          # the unit must not reach for git, ssh or a home directory, and must be
          # started by its timer alone.
          module =
            let
              eval = lib.nixosSystem {
                inherit (pkgs.stdenv.hostPlatform) system;
                modules = [
                  self.nixosModules.scraper
                  {
                    services.lastfm-scraper = {
                      enable = true;
                      apiKeyFile = "/run/secrets/lastfm";
                      publish = "/var/lib/dbs/lastfm/music.db";
                    };
                    boot.loader.grub.enable = false;
                    fileSystems."/" = {
                      device = "none";
                      fsType = "tmpfs";
                    };
                    system.stateVersion = "25.05";
                  }
                ];
              };
              inherit (eval.config.systemd) services;
              unit = eval.config.systemd.units."lastfm-scraper.service".unit;
              closure = pkgs.closureInfo { rootPaths = [ unit ]; };
            in
            assert services.lastfm-scraper.wantedBy == [ ];
            assert !services.lastfm-scraper.restartIfChanged;
            assert eval.config.users.users.lastfm-scraper.isSystemUser;
            pkgs.runCommandLocal "lastfm-scraper-module" { } ''
              u=${unit}/lastfm-scraper.service
              cat "$u"
              for want in \
                '^User=lastfm-scraper$' \
                '^Group=lastfm-scraper$' \
                '^StateDirectory=lastfm-scraper$' \
                '^ProtectSystem=strict$' \
                '^ProtectHome=true$' \
                '^PrivateTmp=true$' \
                '^NoNewPrivileges=true$' \
                '^ReadWritePaths=/var/lib/lastfm-scraper$' \
                '^ReadWritePaths=/var/lib/dbs/lastfm$' \
                '^LoadCredential=api-key:/run/secrets/lastfm$'; do
                grep -q "$want" "$u" || { echo "missing $want" >&2; exit 1; }
              done
              if grep -iE 'REMOTE|GIT_|SSH' "$u"; then
                echo "the unit still names a remote, git or ssh" >&2
                exit 1
              fi
              # Nothing the unit can run is git or ssh.
              if grep -E -- '-(git|openssh)-[0-9]' ${closure}/store-paths; then
                echo "git or ssh is in the unit's closure" >&2
                exit 1
              fi
              touch $out
            '';

          # Drives the real unit, so the scraper's PATH and sandbox are exercised
          # rather than the builder's. No API key and no network (the unit is
          # denied it outright): the window is one day that is already on disk,
          # so there is nothing to scrape and the run is migration, settling,
          # build and publish.
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
                publish = "/srv/site/music.db";
                from = "2020-01-01";
                to = "2020-01-01";
                refresh = 0;
              };
              systemd.services.lastfm-scraper.serviceConfig.IPAddressDeny = "any";
              systemd.tmpfiles.rules = [ "d /srv/site 0755 lastfm-scraper lastfm-scraper -" ];
              # Started by the test, not by the clock.
              systemd.timers.lastfm-scraper.wantedBy = lib.mkForce [ ];
            };

            testScript = ''
              day = """[{"artist":{"url":"u","name":"X","image":[],"mbid":""},"date":{"uts":"1577836800","#text":"01 Jan 2020, 00:00"},"mbid":"t","name":"One","image":[],"url":"tu","streamable":"0","album":{"mbid":"","#text":"A"},"loved":"0"}]"""
              state = "/var/lib/lastfm-scraper"
              repo = f"{state}/repo"

              machine.wait_for_unit("multi-user.target")

              with subtest("nothing starts the scraper but its timer"):
                  machine.fail("systemctl is-active lastfm-scraper.service")
                  machine.fail(f"test -e {state}")

              # The layout the git-scrape left behind: a root-owned clone (on
              # hermanii it was the old user's) holding a pending day, so the
              # run has something to migrate and then to settle.
              machine.succeed(f"mkdir -p {repo}/data/pending")
              machine.succeed(f"cat > {repo}/data/pending/2020-01-01.json <<'JSON'\n{day}\nJSON")
              machine.succeed(
                  f"git -C {repo} init -q -b main",
                  f"git -C {repo} add -A",
                  f"git -C {repo} -c user.name=t -c user.email=t@t commit -qm seed",
              )

              machine.succeed("systemctl start lastfm-scraper.service")

              with subtest("the archive moved out of the clone and the day settled"):
                  machine.succeed(f"test -s {state}/data/lastfm/2020-01-01.json")
                  machine.fail(f"test -e {state}/data/pending/2020-01-01.json")
                  machine.fail(f"test -e {state}/.data.migrating")
                  owner = machine.succeed(f"stat -c %U {state}/data/lastfm/2020-01-01.json").strip()
                  assert owner == "lastfm-scraper", owner

              with subtest("the old clone was left as it was"):
                  machine.succeed(f"test -s {repo}/data/pending/2020-01-01.json")
                  machine.fail(f"test -e {repo}/data/lastfm/2020-01-01.json")
                  status = machine.succeed(f"git -c safe.directory='*' -C {repo} status --porcelain")
                  assert status == "", status
                  count = machine.succeed(f"git -c safe.directory='*' -C {repo} rev-list --count HEAD").strip()
                  assert count == "1", count

              with subtest("the database was published"):
                  machine.succeed("test -s /srv/site/music.db")
                  listens = machine.succeed("sqlite3 /srv/site/music.db 'select count(*) from listens'")
                  assert listens.strip() == "1", listens

              with subtest("a second run does not migrate again and still succeeds"):
                  # Written to the clone after the migration: the next run
                  # must not copy it, since the clone is no longer the archive.
                  machine.succeed(f"mkdir -p {repo}/data/lastfm && echo '[]' > {repo}/data/lastfm/2019-12-31.json")
                  machine.succeed("systemctl start lastfm-scraper.service")
                  machine.succeed(f"test -s {state}/data/lastfm/2020-01-01.json")
                  machine.fail(f"test -e {state}/data/lastfm/2019-12-31.json")
                  listens = machine.succeed("sqlite3 /srv/site/music.db 'select count(*) from listens'")
                  assert listens.strip() == "1", listens
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
