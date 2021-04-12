{
  description = "Taskexecutor";

  inputs = {
    nixpkgs-unstable.url = "nixpkgs/nixpkgs-unstable";
    nixpkgs-19-09 = {
      url = "github:NixOS/nixpkgs/ce9f1aaa39ee2a5b76a9c9580c859a74de65ead5";
      flake = false;
    };
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    majordomo.url = "git+https://gitlab.intr/_ci/nixpkgs";
    ssl-certificates.url = "git+ssh://git@gitlab.intr/office/ssl-certificates";
  };

  outputs = { self, majordomo, nixpkgs-19-09, nixpkgs-unstable, ssl-certificates, ... }:
    let
      pkgs-unstable = import nixpkgs-unstable { inherit system; };
      system = "x86_64-linux";
      pkgs = majordomo.outputs.nixpkgs;
      inherit (pkgs) callPackage;
      inherit (pkgs) lib;
    in {
      devShell.${system} = pkgs-unstable.mkShell {
        buildInputs = [ pkgs-unstable.nixUnstable ] ++ (with pkgs; with pkgs.python37mj.pkgs; with callPackage ./pypkgs.nix { inherit pkgs; }; [
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
        ]);
      };
      packages.${system} =
        let
          pkgs = import nixpkgs-19-09 {
            inherit system;
            overlays = [ majordomo.overlay ];
          };
        in rec {
          te = pkgs.callPackage ./te.nix { inherit pkgs; };
          pythonWithTaskexecutor = pkgs.python37mj.withPackages (ps: (with ps; [ te ]));
        } // {
          container = pkgs.callPackage ./default.nix {
            inherit pkgs;
            inherit (ssl-certificates.packages.${system}) certificates;
          };
          deploy = majordomo.outputs.deploy { tag = "hms/taskexecutor"; };
        };
      defaultPackage.${system} = self.packages.${system}.container;
    };
}
