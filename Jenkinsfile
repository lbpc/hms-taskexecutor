@Library('mj-shared-library') _

def jenkinsHomeOnHost = new JenkinsContainer().getHostPath(env.JENKINS_HOME)

pipeline {
    agent { label 'master' }
    options {
        gitLabConnection(Constants.gitLabConnection)
        gitlabBuilds(builds: ['Code analysis', 'Build Python binary'])
    }
    stages {
        stage('Code analysis') {
            agent {
                dockerfile {
                   filename 'Dockerfile.build'
                    args  "-v ${jenkinsHomeOnHost}/.cache:/home/jenkins/.cache"
                }
            }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    sh 'pylint -E --disable=C0111,E1101 src/python/te/main.py'
                }
            }
        }
        stage('Build Python binary') {
            agent {
                dockerfile {
                    filename 'Dockerfile.build'
                    args  "-v ${jenkinsHomeOnHost}/.cache:/home/jenkins/.cache -u root:root"
                }
            }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    sh 'cp -pr /bin/pants . '
                    sh './pants -v'
                    sh './pants binary src/python/te'
                }
            }
        }
        stage('Deploy') {
            when { branch 'master' }
            agent {
                dockerfile {
                    filename 'Dockerfile.build'
                    args  "-v ${jenkinsHomeOnHost}/.cache:/home/jenkins/.cache"
                }
            }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    filesDeploy srcPath: 'dist', dstPath: '/opt/bin', nodeLabels: ['web', 'pop']
                }
            }
            post {
                success {
                    notifySlack 'Taskexecutor deployed'
                }
            }
        }
        stage('Post-deploy') {
            when { branch 'master' }
            steps {
                gitlabCommitStatus(STAGE_NAME) {
                    parallelSh cmd: 'sudo restart taskexecutor', nodeLabels: ['web', 'pop']
                }
            }
            post {
                success {
                    notifySlack 'Taskexecutor restarted'
                }
            }
        }
    }
    post {
        success { cleanWs() }
        failure { notifySlack "Build failled: ${JOB_NAME} [<${RUN_DISPLAY_URL}|${BUILD_NUMBER}>]", "red" }
    }
}
