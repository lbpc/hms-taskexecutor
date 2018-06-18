@Library('mj-shared-library') _

def jenkinsHomeOnHost = new JenkinsContainer().getHostPath(env.JENKINS_HOME)

pipeline {
    agent {
        dockerfile {
        filename 'Dockerfile.build'
        args  "-v ${jenkinsHomeOnHost}/.cache:/home/jenkins/.cache"
        }
    }
    options {
        gitLabConnection(Constants.gitLabConnection)
        gitlabBuilds(builds: ['Code analysis', 'Build Python binary'])
    }
    stages {
        stage('Code analysis') {
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    sh 'pylint -E --disable=C0111,E1101 src/python/te/main.py'
                }
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
            when { branch 'master' }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    filesDeploy srcPath: 'dist', dstPath: '/opt/bin', nodeLabels: ['web', 'pop'], postDeployCmd: 'sudo restart taskexecutor'
                }
            }
            post {
                success {
                    notifySlack "Taskexecutor deployed"
                }
            }
        }
    }
    post {
        success { cleanWs() }
        failure { notifySlack "Build failled: ${JOB_NAME} [<${RUN_DISPLAY_URL}|${BUILD_NUMBER}>]", "red" }
    }
}
