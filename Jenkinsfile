buildWebService(saveResult: false,
                testHook: { args ->
        sh "nix-shell -p python37Packages.pylint --run 'pylint -E --disable=C0111,E1101 src/python/taskexecutor/__main__.py'"
    }
)
