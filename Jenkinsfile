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
                    }
                }  
                steps {
                    sh 'which pants'
                    sh 'which pylint'
                    sh 'pwd'
                    sh 'tree'
                    sh 'whoami'
                    sh 'ls -la'
                    sh 'pants binary src/python/te'
                }
            }
        }
    }
