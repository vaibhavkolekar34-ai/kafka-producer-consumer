# Testing Guide

This guide reflects the 3-broker KRaft setup used earlier on the local machine for the Kafka producer/consumer benchmark application.

## 1. Local 3-broker KRaft cluster

The Kafka cluster used for testing was configured with three brokers, each using KRaft with a dedicated controller listener. The broker configuration matched the following pattern.

### Broker 1

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/opt/kafka/data/broker-1
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false
```

### Broker 2

```properties
process.roles=broker,controller
node.id=2
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9094,CONTROLLER://:9095
advertised.listeners=PLAINTEXT://localhost:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/opt/kafka/data/broker-2
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false
```

### Broker 3

```properties
process.roles=broker,controller
node.id=3
controller.quorum.voters=1@localhost:9093,2@localhost:9095,3@localhost:9097
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://:9096,CONTROLLER://:9097
advertised.listeners=PLAINTEXT://localhost:9096
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/opt/kafka/data/broker-3
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
auto.create.topics.enable=false
```

## 2. Application connection settings

The application should connect to the broker list as a cluster, not to a single node:

```properties
kafka.bootstrap.servers=localhost:9092,localhost:9094,localhost:9096
kafka.topic=performance-test
```

This allows the producer and consumer to connect to any broker in the cluster and automatically discover the rest of the brokers.

## 3. Topic creation for the benchmark

Create the topic with replication enabled across all three brokers:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092,localhost:9094,localhost:9096 \
  --create \
  --topic performance-test \
  --partitions 3 \
  --replication-factor 3
```

If the topic already exists, you can validate it with:

```bash
bin/kafka-topics.sh \
  --bootstrap-server localhost:9092,localhost:9094,localhost:9096 \
  --describe \
  --topic performance-test
```

## 4. Running the benchmark application

Run the consumer first, then run the producer in a separate terminal:

```bash
java -jar target/kafka-producer-consumer-1.0.0.jar consumer
java -jar target/kafka-producer-consumer-1.0.0.jar producer
```

The default configuration file is in `config/application.properties` and should match this pattern:

```properties
kafka.bootstrap.servers=localhost:9092,localhost:9094,localhost:9096
kafka.topic=performance-test

test.run.id=baseline-001
test.message.count=50000
test.result.directory=results

producer.batch.size=16384
producer.linger.ms=0
producer.compression.type=none
producer.acks=all

consumer.group.id=performance-test-group
consumer.auto.offset.reset=earliest
consumer.enable.auto.commit=false
consumer.poll.timeout.ms=1000
consumer.max.wait.seconds=300
```

## 5. Failover verification

This exact verification was performed on the 3-broker KRaft cluster to confirm that data remains available when one broker is unavailable.

### Initial topic state

Before producing data:

```bash
/opt/kafka/bin/kafka-get-offsets.sh -bootstrap-server localhost:9092 --topic data-availability
```

Output:

```text
data-availability:0:0
data-availability:1:0
data-availability:2:0
```

Topic metadata showed a 3-partition, 3-replica topic with `min.insync.replicas=2`:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic data-availability
```

Key output:

```text
Topic: data-availability        PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Partition: 0    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3,1
        Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,1,2
```

### Producer run

The application produced 50,000 messages to the topic:

```bash
java -jar kafka-performance-test-1.0.0.jar producer
```

Observed output:

```text
Producer started: runId=data-availability-0001, topic=data-availability, messages=50000
Producer completed: 50,000 messages in 1.519 s (32921.01 messages/s)
```

Offsets after the producer run:

```bash
/opt/kafka/bin/kafka-get-offsets.sh -bootstrap-server localhost:9092 --topic data-availability
```

```text
data-availability:0:0
data-availability:1:50000
data-availability:2:0
```

### Broker failure test

After stopping one broker, the topic continued to be read from the remaining brokers:

```bash
systemctl stop kafka-broker-1.service
/opt/kafka/bin/kafka-get-offsets.sh -bootstrap-server localhost:9094 --topic data-availability
/opt/kafka/bin/kafka-get-offsets.sh -bootstrap-server localhost:9096 --topic data-availability
```

Both commands reported the same offsets:

```text
data-availability:0:0
data-availability:1:50000
data-availability:2:0
```

This confirms the remaining brokers still served the replicated data while one broker was down.

### Metadata after broker failure

A describe call against one of the remaining brokers showed the cluster updating to a new leader layout:

```bash
/opt/kafka/bin/kafktopics.sh --bootstrap-server localhost:9094 --describe --topic data-availability
```

Relevant output:

```text
Topic: data-availability        PartitionCount: 3       ReplicationFactor: 3    Configs: min.insync.replicas=2
        Partition: 0    Leader: 2       Replicas: 1,2,3 Isr: 2,3
        Partition: 1    Leader: 2       Replicas: 2,3,1 Isr: 2,3
        Partition: 2    Leader: 3       Replicas: 3,1,2 Isr: 3,2
```

This is the expected failover behavior: the cluster maintained replication with `min.insync.replicas=2` and rebalanced leadership to the alive brokers.

### Consumer validation after failover

The consumer was then run while one broker was down and successfully processed all 50,000 messages:

```bash
java -jar kafka-performance-test-1.0.0.jar consumer
```

Observed output:

```text
Consumer connecting: runId=data-availability-0001, topic=data-availability, expecting 50,000 messages
Consumer ready: assigned partitions=[data-availability-2, data-availability-0, data-availability-1]
Consumer completed successfully.
```

The output file was also created successfully:

```text
Results written to /opt/kafka-performance/results/data-availability-0001.csv
```

CSV content:

```csv
test,batchSize,lingerMs,compression,messages,throughput,avgLatencyMs,p50Ms,p95Ms,p99Ms,maxLatencyMs
data-availability-0001,16384,0,none,50000,214068.29,166042.161,166073.000,166240.000,166246.000,166644.000
```

This demonstrates the data remained available and the application was able to consume all records without data loss while one broker was offline.

## 6. Practical validation checklist

- Ensure all three brokers are running and reachable on the configured ports.
- Keep the same topic name in the producer and consumer configuration.
- Use a fresh `test.run.id` for each benchmark run.
- Confirm the consumer group is intentionally reused or intentionally reset for a new run.
- Verify that replication is configured with `default.replication.factor=3` and `min.insync.replicas=2`.
- Validate failover by stopping one broker and confirming the topic remains readable and consumers still complete successfully.

## 7. Summary

The verified failover pattern for this project is:

```properties
kafka.bootstrap.servers=localhost:9092,localhost:9094,localhost:9096
```

With a 3-broker KRaft cluster configured as shown above, the application continued to serve and consume the full message set even when one broker was stopped. This confirms the benchmark setup is suitable for resilient producer/consumer testing and broker outage validation.
