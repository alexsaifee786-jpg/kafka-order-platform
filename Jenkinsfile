pipeline {
    agent any

    stages  {
        stage('Checkout') {
            steps {
                git branch: 'main',
                    url: 'https://github.com/alexsaifee786-jpg/kafka-order-platform.git'
            }
        }

        stage('Test Order Service') {
            steps {
                dir('order-service') {
                    sh 'chmod +x mvnw'
                    sh './mvnw test -Dspring.datasource.url=jdbc:mysql://host.docker.internal:3306/order_db'
                }
            }
        }

        stage('Package Order Service') {
            steps {
                dir('order-service') {
                    sh './mvnw package -DskipTests'
                }
            }
        }

        stage('Archive Order Service') {
            steps {
                archiveArtifacts artifacts: 'order-service/target/*.jar'
            }
        }

        stage('Test Inventory Service') {
            steps {
                dir('inventory-service') {
                    sh 'chmod +x mvnw'
                    sh '''
                        TEST_DB_URL=jdbc:mysql://host.docker.internal:3306/inventory_test_db \
                        TEST_SCHEMA_REGISTRY_URL=http://host.docker.internal:8081 \
                        ./mvnw test
                    '''
                }
            }
        }

        stage('Package Inventory Service') {
            steps {
                dir('inventory-service') {
                    sh './mvnw package -DskipTests'
                }
            }
        }

        stage('Archive Inventory Service') {
            steps {
                archiveArtifacts artifacts: 'inventory-service/target/*.jar'
            }
        }
        stage('Build Order Docker Image') {
            steps {
                sh 'docker build -t order-service:${BUILD_NUMBER} ./order-service'
            }
        }

        stage('Build Inventory Docker Image') {
            steps {
                sh 'docker build -t inventory-service:${BUILD_NUMBER} ./inventory-service'
            }
        }
        stage('Approval for Deploy') {
            steps {
                input message: "Deploy Build #${BUILD_NUMBER} to local environment?",
              ok: 'Deploy'
            }
        }
    }
}
