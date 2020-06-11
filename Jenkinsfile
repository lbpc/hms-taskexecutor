buildWebService(
    scanPasswords: false,
    saveResult: false,

    testHook: { args ->
        sh "nix-shell -p python37Packages.pylint --run 'pylint -E --disable=C0111,E1101 src/python/taskexecutor/__main__.py'"
    },

    postPush: {
        // XXX: It's not used by Jenkins but could be useful for manual deploy.
        String SCRIPT_DEPLOY_WEB_RPOD = readFile "deploy/web/prod.sh"

        parallelCall (
            nodeLabels: ["web"],
            procedure: { nodeLabels ->
                if (TAG == "master") {
                    sh (["sudo -n /usr/bin/docker pull docker-registry.intr/hms/taskexecutor:$TAG",
                         "sudo -n /sbin/stop taskexecutor", // Always returns true.
                         "sleep 5", // Wait until service stops.
                         "sudo -n /sbin/start taskexecutor"].join("; "))
                    String "docker-registry.intr/hms/taskexecutor:$TAG deployed to web."
                }
            }
        )
    }
)
