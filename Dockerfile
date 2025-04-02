FROM eclipse-temurin:17-jre

RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY target/broadcast-worker-1.0-SNAPSHOT.jar /app/broadcast-worker.jar
COPY start.sh /app/start.sh

RUN mkdir -p /app/storage/logs /app/storage/rate-limit && \
    chmod -R 755 /app/storage && \
    chmod +x /app/start.sh

ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Xms512m -Xmx1536m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/app/storage/logs"
ENV SPRING_PROFILES_ACTIVE="prod"

CMD ["/app/start.sh"]