{ pkgs ? import (builtins.fetchGit {
  name = "nixpkgs-19.09-2020-02-27";
  url = "https://github.com/nixos/nixpkgs.git";
  rev = "ce9f1aaa39ee2a5b76a9c9580c859a74de65ead5";
  ref = "refs/heads/nixos-19.09";
}) {
  overlays = [
    (import (builtins.fetchGit {
      url = "git@gitlab.intr:_ci/nixpkgs.git";
      ref = "master";
    }))
  ];
}
}:

with pkgs;

with callPackage ./pypkgs.nix { inherit pkgs; };

python37mj.pkgs.buildPythonPackage rec {
  name = "taskexecutor";
  src = ./src/python;
  propagatedBuildInputs = with python37mj.pkgs; [
    kombu
    clamd
    PyMySQL
    jinja2
    schedule
    psutil
    pyaml
    docker
    pg8000
    requests
    alerta
    attrs
    giturlparse
  ];
  checkInputs = [ pyfakefs ];
}
