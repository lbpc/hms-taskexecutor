{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit { url = "git@gitlab.intr:_ci/nixpkgs.git"; inherit ref; }))
  ];
};
let
  inherit (dockerTools) buildLayeredImage;
  inherit (lib) mkPythonPath;
  inherit (import ./pypkgs.nix) pyfakefs;
  te = callPackage ./default.nix {};
in
  buildLayeredImage {
    name = "docker-registry.intr/hms/taskexecutor-dev";
    tag = "latest";
    contents = [ python37 te ];
    config = {
      Env = [
        "PYTHONPATH=${mkPythonPath (te.requiredPythonModules ++ [ pyfakefs ])}"
      ];
    };
  }
