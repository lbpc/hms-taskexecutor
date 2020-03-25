with import <nixpkgs> {};
let
  inherit (python37Packages) buildPythonPackage buildPythonApplication fetchPypi;

  PyMySQL = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "PyMySQL";
    version="0.9.3";
    src = fetchPypi {
      inherit pname version;
      sha256 = "1ry8lxgdc1p3k7gbw20r405jqi5lvhi5wk83kxdbiv8xv3f5kh6q";
    };
    doCheck = false;
  };

  clamd = buildPythonPackage rec {
    name = "${pname}-${version}";
    pname = "clamd";
    version="1.0.2";
    src = fetchPypi {
      inherit pname version;
      sha256 = "0q4myb07gn55v9mkyq83jkgfpj395vxxmshznfhkajk82kc2yanq";
    };
    doCheck = false;
  };


in
  buildPythonApplication rec {
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
  }
