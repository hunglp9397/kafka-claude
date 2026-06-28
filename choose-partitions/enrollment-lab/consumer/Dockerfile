# Stage 1: Build
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /app
COPY pom.xml .
# Cache dependencies riêng để rebuild nhanh hơn
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q

# Stage 2: Run — image nhỏ hơn
FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar

# JVM tuning cho container — tránh OOMKilled
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75.0 \
               -XX:+UseG1GC \
               -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8081

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
