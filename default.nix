{ nixpkgs ? (import <nixpkgs> { }).fetchgit {
  url = "https://github.com/NixOS/nixpkgs.git";
  rev = "ce9f1aaa39ee2a5b76a9c9580c859a74de65ead5";
  sha256 = "1s2b9rvpyamiagvpl5cggdb2nmx4f7lpylipd397wz8f0wngygpi";
}, overlayUrl ? "git@gitlab.intr:_ci/nixpkgs.git", overlayRef ? "master" }:

with import nixpkgs {
  overlays = [
    (import (builtins.fetchGit { url = overlayUrl; ref = overlayRef; }))
  ];
};

let
  inherit (dockerTools) buildLayeredImage;
  inherit (lib) firstNChars mkPythonPath dockerRunCmd;
  inherit (builtins) getEnv;
  te = with (import <nixpkgs> { }).fetchgit {
    url = "https://github.com/NixOS/nixpkgs.git";
    rev = "ce9f1aaa39ee2a5b76a9c9580c859a74de65ead5";
    sha256 = "1s2b9rvpyamiagvpl5cggdb2nmx4f7lpylipd397wz8f0wngygpi";
  }; callPackage ./te.nix { inherit overlayRef; };
  gitAbbrev = firstNChars 8 (getEnv "GIT_COMMIT");
  dockerArgHints = {
    name = "taskexecutor";
    init = true;
    read_only = true;
    network = "host";
    pid = "host";
    hostname = "$(hostname -s)";
    devices = [ "$(grep '/home ' /proc/mounts | cut -d' ' -f1)" ];
    volumes = [
      ({
        type = "bind";
        source = "/run/docker.sock";
        target = "/var/run/docker.sock";
      })
      ({
        type = "bind";
        source = "/home";
        target = "/home";
      })
      ({
        type = "bind";
        source = "/opt";
        target = "/opt";
      })
      ({
        type = "bind";
        source = "/var/cache";
        target = "/var/cache";
      })
      ({
        type = "bind";
        source = "/opcache";
        target = "/opcache";
      })
      ({
        type = "bind";
        source = "/sys/fs/cgroup";
        target = "/sys/fs/cgroup";
      })
      # TODO: remove /etc bind mount as soon as possible
      ({
        type = "bind";
        source = "/etc";
        target = "/etc";
      })
      ({
        type = "tmpfs";
        target = "/var/tmp";
      })
      ({
        type = "tmpfs";
        target = "/tmp";
      })
    ];
  };
in buildLayeredImage rec {
  name = "docker-registry.intr/hms/taskexecutor";
  tag = if gitAbbrev != "" then gitAbbrev else "latest";
  maxLayers = 50;
  contents = [ te nss-certs bash coreutils findutils quota killall mariadb.client gitMinimal restic rsync gzip gnutar ];
  topLayer = te;
  config = {
    Entrypoint = [ "${python37mj}/bin/python" "-m" ];
    Cmd = "taskexecutor";
    Env = [
      "PYTHONPATH=${mkPythonPath ([ te ] ++ te.requiredPythonModules)}"
      "SSL_CERT_FILE=${nss-certs}/etc/ssl/certs/ca-bundle.crt"
      "CONFIG_PROFILE=dev"
      "APIGW_HOST=api-dev.intr"
      "APIGW_PASSWORD=ab0LyS4zY2XI"
      "TZ=Europe/Moscow"
      "TZDIR=${tzdata}/share/zoneinfo"
      "LOCALE_ARCHIVE_2_27=${locale}/lib/locale/locale-archive"
      "LOCALE_ARCHIVE=${locale}/lib/locale/locale-archive"
    ];
    Labels = { "ru.majordomo.docker.cmd" = dockerRunCmd dockerArgHints "${name}:${tag}"; };
  };
}
