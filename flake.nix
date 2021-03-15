{
  description = "Taskexecutor";

  inputs = {
    nixpkgs-unstable.url = "nixpkgs/nixpkgs-unstable";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    majordomo.url = "git+https://gitlab.intr/_ci/nixpkgs";
  };

  outputs = { self, majordomo, nixpkgs-unstable, ... }:
    let
      pkgs-unstable = import nixpkgs-unstable { inherit system; };
      system = "x86_64-linux";
      pkgs = majordomo.outputs.nixpkgs;
      inherit (pkgs) callPackage;
      inherit (pkgs) lib;
    in {
      devShell.${system} = pkgs-unstable.mkShell {
        buildInputs = [ pkgs-unstable.nixUnstable ];
      };
      packages.${system} = {
        te = pkgs.callPackage ./te.nix {};
        container = pkgs.callPackage ./default.nix {};
        deploy = majordomo.outputs.deploy { tag = "hms/taskexecutor"; };
      };
      defaultPackage.${system} = self.packages.${system}.container;
    };
}
