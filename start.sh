#!/bin/sh

# Verifica se as dependências estão disponíveis
wait_for_dependencies() {
  until nc -z mariadb 3306; do
    echo "Waiting for MariaDB..."
    sleep 2
  done
  
  until nc -z rabbitmq 5672; do
    echo "Waiting for RabbitMQ..."
    sleep 2
  done
}

# Configura tratamento de sinais para shutdown gracioso
trap 'kill -TERM $PID; wait $PID' TERM INT

# Cria diretórios e ajusta permissões
mkdir -p /app/storage/logs /app/storage/rate-limit
chmod -R 755 /app/storage

echo "Starting application with profiles: $SPRING_PROFILES_ACTIVE"
echo "Using database: $SPRING_DATASOURCE_URL"
echo "Using RabbitMQ: $SPRING_RABBITMQ_HOST:$SPRING_RABBITMQ_PORT"

# Aguarda dependências e inicia a aplicação
wait_for_dependencies
java $JAVA_OPTS -jar /app/broadcast-worker.jar &

PID=$!
wait $PID