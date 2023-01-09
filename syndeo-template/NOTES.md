# Technical notes on Syndeo template

## Reading from Kafka

A template's default configuration must allow it to start from the last-committed offset, and the start offset for the
very first run should be user-configurable. For this purpose, it should:

- Kafka manages offset consumption via consumer group ids. The template should derive the same consumer group ID
     for repeated runs.
- Users should be able to override `ConsumerConfig` parameters, but should be warned to possibly unexpected outcomes.

Important resources:
- https://docs.confluent.io/platform/current/clients/consumer.html
- https://stackoverflow.com/questions/41137281/offsets-stored-in-zookeeper-or-kafka
