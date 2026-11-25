{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.lastfm-scraper;

  # StateDirectory= creates the directory and, when its owner is wrong (a
  # persisted bind mount, or a state dir an earlier user owned), chowns it
  # recursively. It only takes paths under /var/lib; anything else gets a
  # tmpfiles rule instead.
  stateDirectory = lib.removePrefix "/var/lib/" cfg.stateDir;
  underVarLib = lib.hasPrefix "/var/lib/" cfg.stateDir;
in
{
  imports = [
    (lib.mkRemovedOptionModule [ "services" "lastfm-scraper" "remote" ] ''
      The scraper no longer clones, commits or pushes: the archive lives only in
      `stateDir`, which should be backed up.
    '')
  ];

  options.services.lastfm-scraper = {
    enable = lib.mkEnableOption "the Last.fm scraper";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.scrape;
      defaultText = lib.literalExpression "lastfm.packages.\${system}.scrape";
      description = "The scraper package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "lastfm-scraper";
      description = ''
        User the scraper runs as; created if it is the default. It needs write
        access to the directory `publish` is in, if that is set.
      '';
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "lastfm-scraper";
      description = "Group the scraper runs as; created if it is the default.";
    };

    interval = lib.mkOption {
      type = lib.types.str;
      default = "*-*-* 00/2:06:00";
      description = "systemd OnCalendar expression for how often to scrape.";
    };

    stateDir = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/lastfm-scraper";
      description = ''
        The only copy of the archive (data/lastfm and data/pending) and the
        music.db built from it. Back it up; music.db need not be.
      '';
    };

    apiKeyFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        File holding the Last.fm API key, passed to the unit as a credential.
        Only a run with days to scrape needs one.
      '';
    };

    publish = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "/var/lib/datasette-sites/lastfm/music.db";
      description = ''
        Where to install the rebuilt database, replaced by rename so a reader
        never sees a partial file. Its directory must already exist and be
        writable by the unit; it is the only path outside `stateDir` the unit
        may write. Null builds it in `stateDir` and stops there.
      '';
    };

    refresh = lib.mkOption {
      type = lib.types.int;
      default = 2;
      description = ''
        How many of the most recent days to treat as still in progress. Those
        days are re-scraped every run and held out of the settled archive, so a
        build over that archive alone is reproducible between scrapes.
      '';
    };

    from = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Earliest day to scrape, YYYY-MM-DD. Null uses the binary's default.";
    };

    to = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "Latest day to scrape, YYYY-MM-DD. Null scrapes up to today.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users = lib.mkIf (cfg.user == "lastfm-scraper") {
      lastfm-scraper = {
        isSystemUser = true;
        group = cfg.group;
        home = cfg.stateDir;
      };
    };
    users.groups = lib.mkIf (cfg.group == "lastfm-scraper") { lastfm-scraper = { }; };

    systemd.tmpfiles.rules = lib.optional (
      !underVarLib
    ) "d ${cfg.stateDir} 0750 ${cfg.user} ${cfg.group} - -";

    systemd.services.lastfm-scraper = {
      description = "Scrape the Last.fm listening history and rebuild music.db";
      # No wantedBy: a wanted oneshot is started by every switch-to-configuration.
      # The timer is the only thing that starts it.
      restartIfChanged = false;
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      environment = {
        LASTFM_STATE = cfg.stateDir;
        LASTFM_REFRESH = toString cfg.refresh;
      }
      // lib.optionalAttrs (cfg.publish != null) { LASTFM_PUBLISH = cfg.publish; }
      // lib.optionalAttrs (cfg.from != null) { LASTFM_FROM = cfg.from; }
      // lib.optionalAttrs (cfg.to != null) { LASTFM_TO = cfg.to; };

      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        WorkingDirectory = cfg.stateDir;
        ExecStart = lib.getExe cfg.package;
        # A scrape is never urgent; keep it out of the way of the host's other work.
        Nice = 19;
        IOSchedulingClass = "idle";
        ProtectSystem = "strict";
        ProtectHome = true;
        ReadWritePaths = [ cfg.stateDir ] ++ lib.optional (cfg.publish != null) (dirOf cfg.publish);
        PrivateTmp = true;
        NoNewPrivileges = true;
        LoadCredential = lib.optional (cfg.apiKeyFile != null) "api-key:${cfg.apiKeyFile}";
      }
      // lib.optionalAttrs underVarLib {
        StateDirectory = stateDirectory;
        StateDirectoryMode = "0750";
      };
    };

    systemd.timers.lastfm-scraper = {
      description = "Scrape the Last.fm listening history on a schedule";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.interval;
        # A box that was off at the scheduled minute should still catch up.
        Persistent = true;
        RandomizedDelaySec = "2m";
        Unit = "lastfm-scraper.service";
      };
    };
  };
}
