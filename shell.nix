with import <nixpkgs> {};
let
  inherit (python37Packages) buildPythonPackage buildPythonApplication fetchPypi;

  pyfakefs = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "pyfakefs";
    version="4.0.2";
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


in
  mkShell {
    buildInputs = [
      python37
      pyfakefs
    ];
  }
