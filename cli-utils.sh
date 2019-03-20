#!/usr/bin/env bash

# Create test topic
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic test

# Create event topic
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 2 --topic event
