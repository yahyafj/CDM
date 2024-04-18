pipeline {
  agent any
  stages {
    stage('version') {
      steps {
        sh '/usr/bin/python3'
      }
    }
    stage('hello') {
      steps {
        sh 'python3 hello.py'
      }
    }
  }
}
