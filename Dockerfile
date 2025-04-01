FROM eclipse-temurin:17-jre
WORKDIR /app

# Copy application JAR
COPY target/broadcast-worker-1.0-SNAPSHOT.jar /app/broadcast-worker.jar
COPY start.sh /app/start.sh

# Create directories with proper permissions
RUN mkdir -p /app/storage/logs /app/storage/rate-limit && \
    chmod -R 777 /app/storage && \
    chmod +x /app/broadcast-worker.jar /app/start.sh

# Set environment variables
ENV JAVA_OPTS="-Xms256m -Xmx512m"
ENV SPRING_PROFILES_ACTIVE="prod"

# Run the startup script
CMD ["/app/start.sh"]