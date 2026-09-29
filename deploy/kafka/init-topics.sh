#!/bin/sh
set -eu

broker="kafka:29092"
create_topic() {
  topic="$1"
  partitions="$2"
  shift 2
  set -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
  "$@"
}

create_topic updatis_connect_configs 1 --config cleanup.policy=compact
create_topic updatis_connect_offsets 1 --config cleanup.policy=compact
create_topic updatis_connect_statuses 1 --config cleanup.policy=compact
create_topic updatis.00000000000000000000000000000001.00000000000000000000000000000101.example.orders \
  1 --config cleanup.policy=delete --config retention.ms=604800000 --config max.message.bytes=2097152
create_topic updatis.00000000000000000000000000000001.00000000000000000000000000000101.example.customers \
  1 --config cleanup.policy=delete --config retention.ms=604800000 --config max.message.bytes=2097152
