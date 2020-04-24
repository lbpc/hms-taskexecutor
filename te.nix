{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit {
      url = "git@gitlab.intr:_ci/nixpkgs.git";
      inherit ref;
    }))
  ];
};
with callPackage ./pypkgs.nix { inherit ref; };

python37mj.pkgs.buildPythonPackage rec {
  name = "taskexecutor";
  src = ./.;
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
  ];
  checkInputs = [ pyfakefs ];
}
