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
                sh "docker build -t order-service:${BUILD_NUMBER} ./order-service"
            }
        }

        stage('Build Inventory Docker Image') {
            steps {
                sh "docker build -t inventory-service:${BUILD_NUMBER} ./inventory-service"
            }
        }
        stage('Approval for Deploy') {
            steps {
                input message: "Deploy Build #${BUILD_NUMBER} to local environment?",
              ok: 'Deploy'
            }
        }
        stage('Deploy Order Service') {
    steps {
        sh '''
            docker rm -f order-service-container || true

            docker run -d \
              --name order-service-container \
              --network kafka-order-platform_default \
              -p 8080:8080 \
              -e SPRING_DATASOURCE_URL=jdbc:mysql://host.docker.internal:3306/order_db \
              -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kop-kafka-1:19092,kop-kafka-2:19092,kop-kafka-3:19092 \
              -e SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL=http://kop-schema-registry:8081 \
              order-service:${BUILD_NUMBER}
        '''
    }
}

stage('Deploy Inventory Service') {
    steps {
        sh '''
            docker rm -f inventory-service-container || true

            docker run -d \
              --name inventory-service-container \
              --network kafka-order-platform_default \
              -p 8082:8082 \
              -e SPRING_DATASOURCE_URL=jdbc:mysql://host.docker.internal:3306/inventory_db \
              -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kop-kafka-1:19092,kop-kafka-2:19092,kop-kafka-3:19092 \
              -e SPRING_KAFKA_PRODUCER_PROPERTIES_SCHEMA_REGISTRY_URL=http://kop-schema-registry:8081 \
              -e SPRING_KAFKA_CONSUMER_PROPERTIES_SCHEMA_REGISTRY_URL=http://kop-schema-registry:8081 \
              inventory-service:${BUILD_NUMBER}
        '''
    }
}
stage('Health Check') {
    steps {
        sh '''
            echo "Checking Order Service..."
            curl -s --fail --retry 15 --retry-delay 2 --retry-connrefused \
              http://host.docker.internal:8080/actuator/health | grep -q '"status":"UP"'

            echo "Order Service is healthy"

            echo "Checking Inventory Service..."
            curl -s --fail --retry 15 --retry-delay 2 --retry-connrefused \
              http://host.docker.internal:8082/actuator/health | grep -q '"status":"UP"'

            echo "Inventory Service is healthy"
        '''
    }
}
    }
}
