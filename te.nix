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

with callPackage ./pypkgs.nix { inherit overlayRef; };

python37mj.pkgs.buildPythonPackage rec {
  name = "taskexecutor";
  src = ./src;
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
