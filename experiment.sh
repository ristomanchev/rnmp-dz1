#!/usr/bin/env bash
# Experiment: earliest vs latest on the SAME data, plus different watermark bounds.
#
#   ./experiment.sh                  # 6 hours of data at 60x (~8 minutes in total)
#   SPEEDUP=600 ./experiment.sh      # faster, for a quick try (~3 minutes)
#
# Kafka must be running. WARNING: deletes and re-creates the topic air-quality and the output
# topics of a previous experiment, so that every experiment starts from a clean state.
# Logs and results go to results/.
set -euo pipefail
cd "$(dirname "$0")"

SPEEDUP="${SPEEDUP:-60}"
HOURS="${HOURS:-6}"
TOPICS="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"
PIDS=""
trap '[ -n "$PIDS" ] && kill $PIDS 2>/dev/null; true' EXIT   # Ctrl+C -> also stops the Flink applications

echo "== 1. Clean start: deleting air-quality and the outputs of the previous experiment"
OLD=$($TOPICS --list | grep -E '^(air-quality|aq-(stats|alerts|late)-(latest|earliest)(-0|-60)?)$' || true)
for t in $OLD; do $TOPICS --delete --topic "$t"; done
while $TOPICS --list | grep -x air-quality > /dev/null; do sleep 1; done   # deletion takes a few seconds
$TOPICS --create --topic air-quality --partitions 4 --replication-factor 1

echo "== 2. Compiling"
# Java 17: found automatically on macOS; elsewhere JAVA_HOME must point to a JDK 17 (or java 17 on the PATH)
if [ -x /usr/libexec/java_home ]; then
    export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
(cd flink-app && mvn -q compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=test)
CP="flink-app/target/classes:$(cat flink-app/target/classpath.txt)"

# Starts one Flink application in the background: start_job <name> <arguments...>
# Its outputs go to aq-stats-<name>, aq-alerts-<name>, aq-late-<name>, its log to results/<name>.log
start_job() {
    local name="$1"; shift
    "$JAVA" -cp "$CP" mk.ukim.finki.rnmp.dz1.AirQualityJob \
        --input-topic air-quality --print false --run "$name" "$@" > "results/$name.log" 2>&1 &
    PIDS="$PIDS $!"
}

# Checks that all applications are still running (if one crashed, the error is in its log)
check_jobs() {
    for pid in $PIDS; do
        kill -0 "$pid" 2>/dev/null || { echo "A Flink application crashed, see results/*.log"; exit 1; }
    done
}

stop_jobs() {
    kill $PIDS 2>/dev/null || true
    wait $PIDS 2>/dev/null || true
    PIDS=""
}

echo "== 3. LATEST: 3 applications (bound 180, 60 and 0 s) start BEFORE the producer"
start_job latest    --offsets latest --ooo-sec 180
start_job latest-60 --offsets latest --ooo-sec 60
start_job latest-0  --offsets latest --ooo-sec 0
sleep 25   # Flink must start and fetch the latest offsets before the messages arrive
check_jobs

echo "== 4. Producer: $HOURS hours of data at ${SPEEDUP}x"
date +"   start: %H:%M:%S"
.venv/bin/python -u producer/producer.py --topic air-quality --speedup "$SPEEDUP" --hours "$HOURS" | tee results/producer.log
date +"   end:   %H:%M:%S"
sleep 15   # let the last results reach Kafka
check_jobs
stop_jobs

echo "== 5. EARLIEST: the data is already in the topic, the applications start AFTER the producer"
start_job earliest   --offsets earliest --ooo-sec 180
start_job earliest-0 --offsets earliest --ooo-sec 0
sleep 40   # startup + reading all messages
check_jobs
stop_jobs

echo "== 6. Results (saved to results/summary.txt)"
.venv/bin/python results/compare.py latest earliest latest-60 latest-0 earliest-0 | tee results/summary.txt
