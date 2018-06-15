@Library('mj-shared-library') _
    pipeline {
        agent {
            dockerfile {
            filename 'docker/Dockerfile'
            args  '-v /var/lib/jenkins-docker/.cache:/home/jenkins/.cache'
            }
        }
        environment {
            PROJECT_NAME = gitRemoteOrigin.getProject()
            GROUP_NAME = gitRemoteOrigin.getGroup()
        }
        options { gitLabConnection(Constants.gitLabConnection) }
        stages {
            stage('Build pants Docker image') {
                steps {
                    sh 'echo ${WORKSPACE} '
                    sh 'which pants'
                    sh 'which pylint'
                    sh 'pwd'
                    sh 'tree'
                    sh 'ls -la'
                    sh 'whoami'
                    sh 'cp -pr /bin/pants . '
                    sh 'pylint -E --disable=C0111,E1101 src/python/te/main.py' // Please recheck E1101 | Instance of '__Config' has no 'process_watchdog' member (no-member) |
                    sh './pants binary src/python/te'
                }
            }
            stage('Deploy te on webs') {
                when { branch 'pants-docker' } // change to master
                steps {
                    sh 'echo ${WORKSPACE} '
                    sh 'ls -la '
                    sh 'pwd '
                    gitlabCommitStatus(STAGE_NAME) {
                        filesDeploy srcPath: "dist/", dstPath: "/home/jenkins/", nodeLabel: "web"
                    }
                }
                post {
                    success {
                        notifySlack "Taskexecutor deployed to webs"
                    }
                }
            }
        }
    }
