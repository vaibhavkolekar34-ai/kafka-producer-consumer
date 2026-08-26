# Kafka Producer and Consumer

A Java/Maven application for benchmarking Apache Kafka producer and consumer behavior.

## Overview

This project demonstrates a simple Kafka workflow for:

- sending a configurable number of messages to a Kafka topic
- consuming the same run's records from the topic
- measuring end-to-end latency and throughput
- writing benchmark results to a CSV file in the `results/` folder

Each message is tagged with a unique `test.run.id`, so the consumer can ignore stale records from earlier runs in the same topic.

## Build locally

From the project root, run:

```bash
mvn clean package
```

The executable JAR is generated at:

```text
target/kafka-producer-consumer-1.0.0.jar
```

## Run the application

Start the consumer first, then run the producer in a separate terminal:

```bash
java -jar target/kafka-producer-consumer-1.0.0.jar consumer
java -jar target/kafka-producer-consumer-1.0.0.jar producer
```

The application loads `config/application.properties` by default. You can also point to a custom config file:

```bash
java -jar target/kafka-producer-consumer-1.0.0.jar consumer /path/to/config/application.properties
```

## Configuration

The default settings are in `config/application.properties`.

Key values include:

- `kafka.bootstrap.servers` - broker connection string
- `kafka.topic` - target Kafka topic
- `test.run.id` - unique benchmark identifier
- `test.message.count` - number of test messages
- `producer.batch.size`, `producer.linger.ms`, `producer.compression.type` - tuning values used for benchmarking

## EC2 and failover testing

For deployment instructions and broker configuration examples, see [TESTING_GUIDE.md](TESTING_GUIDE.md).

## Example benchmark matrix

Use a fresh `test.run.id` for each execution and vary only the producer tuning parameters listed below:

| Run | batch.size | linger.ms | compression.type |
| --- | ---: | ---: | --- |
| baseline | 16384 | 0 | none |
| batch-64kb | 65536 | 0 | none |
| lz4 | 65536 | 5 | lz4 |
| zstd | 65536 | 5 | zstd |

## License

This project is provided for learning and experimentation purposes.
