{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit { url = "git@gitlab.intr:_ci/nixpkgs.git"; inherit ref; }))
  ];
};

let
    inherit (dockerTools) buildLayeredImage;
    inherit (lib) firstNChars mkPythonPath;
    inherit (builtins) getEnv;
    te = callPackage ./te.nix { inherit ref; };
    gitAbbrev = firstNChars 8 (getEnv "GIT_COMMIT");
in
    buildLayeredImage {
        name = "docker-registry.intr/hms/taskexecutor";
        tag = if gitAbbrev != "" then gitAbbrev else "latest";
        maxLayers = 20;
        contents = [ te nss-certs bash quota ];
        config = {
            Entrypoint = [ "${python37mj}/bin/python" ];
            Env = [
                "PYTHONPATH=${mkPythonPath ([ te ] ++ te.requiredPythonModules)}"
                "SSL_CERT_FILE=${nss-certs}/etc/ssl/certs/ca-bundle.crt"
                "APIGW_PASSWORD=Efu0ahs6"
            ];
        };
    }