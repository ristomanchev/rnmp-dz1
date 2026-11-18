#!/usr/bin/env bash
# Compiles and starts the Flink application from the terminal (without an IDE).
# Example: ./run-job.sh --offsets latest
# (mvn exec:java does not work with Flink 2.3 because of its class loader, hence a plain java command)
set -euo pipefail
cd "$(dirname "$0")"
# Java 17: found automatically on macOS; elsewhere JAVA_HOME must point to a JDK 17 (or java 17 on the PATH)
if [ -x /usr/libexec/java_home ]; then
    export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=test
exec "$JAVA" -cp "target/classes:$(cat target/classpath.txt)" mk.ukim.finki.rnmp.dz1.AirQualityJob "$@"
