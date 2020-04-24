{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit {
      url = "git@gitlab.intr:_ci/nixpkgs.git";
      inherit ref;
    }))
  ];
};

let
  inherit (dockerTools) buildLayeredImage;
  inherit (lib) firstNChars mkPythonPath dockerRunCmd;
  inherit (builtins) getEnv;
  te = callPackage ./te.nix { inherit ref; };
  gitAbbrev = firstNChars 8 (getEnv "GIT_COMMIT");
  dockerArgHints = {
    name = "taskexecutor";
    init = true;
    read_only = true;
    network = "host";
    pid = "host";
    hostname = "$(hostname -s)";
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
    ];
  };
in buildLayeredImage rec {
  name = "docker-registry.intr/hms/taskexecutor";
  tag = if gitAbbrev != "" then gitAbbrev else "latest";
  maxLayers = 50;
  contents = [ te nss-certs bash coreutils findutils quota killall mariadb.client gitMinimal restic rsync gzip ];
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
    ];
    Labels = { "ru.majordomo.docker.cmd" = dockerRunCmd dockerArgHints "${name}:${tag}"; };
  };
}
