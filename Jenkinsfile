@Library('mj-shared-library') _
pipeline {
    agent {
        dockerfile {
        filename 'Dockerfile.build'
        args  "-v ${new JenkinsContainer().getHostPath(HOME)}/.cache:/home/jenkins/.cache"
        }
    }
    options {
        gitLabConnection(Constants.gitLabConnection)
        gitlabBuilds(builds: ['Code analysis', 'Build Python binary', 'Deploy'])
    }
    stages {
        stage('Code analysis')
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    sh 'pylint -E --disable=C0111,E1101 src/python/te/main.py'
                }
            }
        stage('Build Python binary') {
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    sh 'cp -pr /bin/pants . '
                    sh './pants binary src/python/te'
                }
            }
        }
        stage('Deploy') {
            when { branch 'pants-docker' }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    filesDeploy srcPath: 'dist', dstPath: '/opt/bin', nodeLabels: ['web']
                }
            }
            post {
                success {
                    notifySlack "Taskexecutor deployed to webs"
                }
            }
        }
        post {
            success { cleanWs() }
            failure { notifySlack "Build failled: ${JOB_NAME} [<${RUN_DISPLAY_URL}|${BUILD_NUMBER}>]", "red" }
        }
    }
}
