#!/bin/bash
kafka-topics --create --topic events_raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic events_valid --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic events_invalid --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
echo "Topics created!"
kafka-topics --list --bootstrap-server localhost:9092