# ----------------------------------------------------
# STAGE 1: Layer Extraction using Spring Boot layertools
# ----------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS extractor
WORKDIR /extracted

# Copy the fat JAR (ignoring the .original file)
COPY target/payment-kafka-service-0.0.1-SNAPSHOT.jar app.jar

# Unpack layers for Docker caching
RUN java -Djarmode=layertools -jar app.jar extract

# ----------------------------------------------------
# STAGE 2: Minimal, Hardened Runtime Image
# ----------------------------------------------------
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Run as non-root user for security
RUN useradd -u 1001 appuser && chown -R appuser:appuser /app
USER appuser

# Copy extracted layers from least frequent to most frequent change
COPY --from=extractor /extracted/dependencies/ ./
COPY --from=extractor /extracted/spring-boot-loader/ ./
COPY --from=extractor /extracted/snapshot-dependencies/ ./
COPY --from=extractor /extracted/application/ ./

# Container JVM ergonomics
ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

EXPOSE 8080

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]