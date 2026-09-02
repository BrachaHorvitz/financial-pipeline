# ── Stage 1: BUILD ────────────────────────────────────────────────────────────
# We use the official Maven image that already has JDK 17 bundled.
# This stage compiles the code and produces the fat JAR inside the container,
# so the developer's machine does NOT need Maven or Java installed.
FROM maven:3.9-eclipse-temurin-17-alpine AS builder

# Set the working directory inside this stage's filesystem.
# All subsequent COPY/RUN commands in this stage operate relative to here.
WORKDIR /build

# Copy the Maven project descriptor first — before the source code.
# Docker builds in layers: if pom.xml hasn't changed, Docker reuses the cached
# layer where all dependencies were downloaded, making rebuilds much faster.
COPY pom.xml .

# Download every dependency declared in pom.xml into the local Maven cache
# that lives inside this layer.  The -B flag means "batch mode" (no ANSI
# colours or interactive prompts) and -q suppresses most log noise.
# We pass -e so that actual errors are still visible.
RUN mvn dependency:go-offline -B -q -e

# Now copy the full source tree.  This layer is rebuilt only when source files
# change, NOT when pom.xml is unchanged — that's the point of copying pom.xml
# first.
COPY src ./src

# Compile, test-skip, and package into a single executable "fat JAR" that
# bundles all runtime dependencies.  -DskipTests keeps the build fast; tests
# should be run separately in CI before the image is built.
RUN mvn package -DskipTests -B -q -e


# ── Stage 2: RUNTIME ──────────────────────────────────────────────────────────
# Start fresh from a minimal JRE image — we do NOT copy the Maven cache or
# source files.  The final image is much smaller this way (≈ 180 MB vs 600 MB+).
FROM eclipse-temurin:17-jre-alpine AS runtime

# Good practice: don't run the process as root inside the container.
# We create a dedicated user and group called "spring".
RUN addgroup -S spring && adduser -S spring -G spring

# Switch to that non-root user for all subsequent commands.
USER spring

# Our app will listen on 8080 (matches server.port in application.yml).
# EXPOSE is documentation — it tells Docker Compose and humans which port
# the container uses; it does not actually publish the port to the host.
EXPOSE 8080

# Copy only the fat JAR produced by Stage 1.
# The wildcard *.jar picks it up regardless of the exact version suffix.
COPY --from=builder /build/target/*.jar app.jar

# The command that runs when the container starts.
# We add a JVM flag that makes JVM memory ergonomics work correctly inside
# containers (it reads cgroup limits rather than host RAM).
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-jar", "app.jar"]
