{ ref ? "master" }:

with import (builtins.fetchGit {
  name = "nixpkgs-19.09-2020-02-27";
  url = "https://github.com/nixos/nixpkgs-channels";
  rev = "ce9f1aaa39ee2a5b76a9c9580c859a74de65ead5";
  ref = "refs/heads/nixos-19.09";
}) {
  overlays = [
    (import (builtins.fetchGit {
      url = "git@gitlab.intr:_ci/nixpkgs.git";
      inherit ref;
    }))
  ];
};

let inherit (python37mj.pkgs) buildPythonPackage fetchPypi pbr pytest setuptools;
in {

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

  giturlparse = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "git-url-parse";
    version = "1.2.2";
    src = fetchPypi {
      inherit pname version;
      sha256 = "05zi8n2aj3fsy1cyailf5rn21xv3q15bv8v7xvz3ls8xxcx4wpvv";
    };
    propagatedBuildInputs = [ pbr setuptools ];
    checkInputs = [ pytest ];
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

}
