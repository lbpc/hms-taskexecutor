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
                    pants binary src/python/te
                }
            }
        }
