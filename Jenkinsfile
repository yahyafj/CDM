pipeline {
    agent any
    
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Build') {
            steps {
                script {
                    def modifiedFiles = sh(script: 'git diff --name-only HEAD^', returnStdout: true).trim().split('\n')
                    def pythonFiles = modifiedFiles.findAll { it.endsWith('.py') }
                    if (!pythonFiles.isEmpty()) {
                        sh 'python3 print.py' 
                    } else {
                        echo 'No Python files modified or added'
                    }
                }
            }
        }
    }
}

