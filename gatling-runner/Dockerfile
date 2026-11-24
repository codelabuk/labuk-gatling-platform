# The perf runner image from the distribution (alternative to `mvn jib:dockerBuild`).
#   mvn -pl gatling-runner -am package
#   docker build -t labuk-perf-runner:1.0-SNAPSHOT gatling-runner
FROM eclipse-temurin:17-jre

ARG VERSION=1.0-SNAPSHOT
COPY target/perf-${VERSION}/perf-${VERSION}/ /opt/perf/

ENV RESULTS_DIR=/tmp/results
WORKDIR /tmp
USER 1000

# heap from the container memory limit; extra JDBC drivers can be mounted in /opt/perf/drivers
ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-XX:MaxRAMPercentage=75", \
            "-Dlogback.configurationFile=/opt/perf/conf/logback.xml", \
            "-cp", "/opt/perf/lib/*:/opt/perf/drivers/*", "org.codelabuk.perf.runner.Perf"]
CMD ["help"]
