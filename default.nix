{ ref ? "master" }:

with import <nixpkgs> {
  overlays = [
    (import (builtins.fetchGit { url = "git@gitlab.intr:_ci/nixpkgs.git"; inherit ref; }))
  ];
};
with import ./pypkgs.nix;

python37Packages.buildPythonPackage rec {
  name = "taskexecutor";
  src = ./.;
  propagatedBuildInputs = with python37Packages; [
    pika
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
  ];
  checkInputs = [ pyfakefs ];
}
