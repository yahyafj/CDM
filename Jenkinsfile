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
                    // Récupérer la liste des fichiers modifiés ou ajoutés
                    def modifiedFiles = sh(script: 'git diff --name-only HEAD^', returnStdout: true).trim().split('\n')
                    
                    // Vérifier si des fichiers .py ont été modifiés ou ajoutés
                    def pythonFiles = modifiedFiles.findAll { it.endsWith('.py') }
                    
                    // Exécuter le script Python pour chaque fichier .py trouvé
                    pythonFiles.each { pythonFile ->
                        sh "python3 $pythonFile"
                    }
                    
                    if (pythonFiles.isEmpty()) {
                        echo 'No Python files modified or added'
                    }
                }
            }
        }
    }
}
