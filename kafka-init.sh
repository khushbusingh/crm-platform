#!/bin/bash
set -e

TOPICS=("tickets" "conversations" "agent-availability" "sla-events")

for topic in "${TOPICS[@]}"; do
  kafka-topics --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1
  echo "kafka-init.sh: topic '$topic' ready"
done