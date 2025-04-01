#!/bin/sh

# Create directories if they don't exist and ensure proper permissions
mkdir -p /app/storage/logs /app/storage/rate-limit
chmod -R 777 /app/storage

# Give information about the environment
echo "Starting application with profiles: $SPRING_PROFILES_ACTIVE"
echo "Using database: $SPRING_DATASOURCE_URL"
echo "Using RabbitMQ: $SPRING_RABBITMQ_HOST:$SPRING_RABBITMQ_PORT"
echo "Rate limits: ${APPLICATION_RATE_LIMIT_SCRIPT_SECONDS}s / ${APPLICATION_RATE_LIMIT_SCRIPT_MINUTES}m"

# Start the application
exec java $JAVA_OPTS -jar /app/broadcast-worker.jar