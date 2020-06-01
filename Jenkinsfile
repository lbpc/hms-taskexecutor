buildWebService(testHook: { args ->
      sh "nix-shell -p python37Packages.pylint --run 'pylint -E --disable=C0111,E1101 src/taskexecutor/__main__.py'"
    } 
)
