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

let inherit (python37mj.pkgs) buildPythonPackage fetchPypi;
in {
  PyMySQL = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "PyMySQL";
    version = "0.9.3";
    src = fetchPypi {
      inherit pname version;
      sha256 = "1ry8lxgdc1p3k7gbw20r405jqi5lvhi5wk83kxdbiv8xv3f5kh6q";
    };
    doCheck = false;
  };

  clamd = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "clamd";
    version = "1.0.2";
    src = fetchPypi {
      inherit pname version;
      sha256 = "0q4myb07gn55v9mkyq83jkgfpj395vxxmshznfhkajk82kc2yanq";
    };
    doCheck = false;
  };

  pyfakefs = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "pyfakefs";
    version = "4.0.2";
    src = fetchPypi {
      inherit pname version;
      sha256 = "1kpar87y0507fl8a4wisipmjbhx2w2kk467l5awp5ap36z3y25f4";
    };
    postPatch = ''
      substituteInPlace pyfakefs/tests/fake_filesystem_test.py \
        --replace "test_expand_root" "notest_expand_root"
      substituteInPlace pyfakefs/tests/test_issue.py --replace "import flask_restx" ""
    '';
  };
}
