@Library('mj-shared-library') _
    pipeline {
        agent { label 'master' }
        environment {
            PROJECT_NAME = gitRemoteOrigin.getProject()
            GROUP_NAME = gitRemoteOrigin.getGroup()
        }
        options { gitLabConnection(Constants.gitLabConnection) }
        stages {
            stage('Build pants Docker image') {
                agent {
                    dockerfile {
                    filename 'docker/Dockerfile'
                    args  '-v /var/lib/jenkins-docker/.cache:/home/jenkins/.cache'
                    }
                }  
                steps {
                    sh 'which pants'
                    sh 'which pylint'
                    sh 'pwd'
                    sh 'tree'
                    sh 'ls -la'
                    sh 'whoami'
                    sh 'pants binary src/python/te'
                }
            }
        }
    }
