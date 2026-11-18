#!/usr/bin/env bash
# Creates the input topic with 4 partitions (Kafka must be running).
# Run it BEFORE the Flink application, and again after Kafka was reset (./stop.sh in the course repo).
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic air-quality --partitions 4 --replication-factor 1
