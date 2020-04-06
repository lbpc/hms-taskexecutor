{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit { url = "git@gitlab.intr:_ci/nixpkgs.git"; inherit ref; }))
  ];
};

with callPackage ./pypkgs.nix { inherit ref; };

let
  te = callPackage ./default.nix {};
in
  python37mj.withPackages (ps: te.requiredPythonModules ++ [ pyfakefs ])
