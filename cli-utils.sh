# Create test-topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic test-topic


