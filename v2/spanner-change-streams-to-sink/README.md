# Spanner Change Streams To Sink Dataflow Template

The [SpannerChangeStreamsToSink](src/main/java/com/google/cloud/teleport/v2/templates/SpannerChangeStreamsToSink.java) pipeline
ingests data supplied by Spanner change streams, orders the records per shard and writes the data to Sink.

## Getting Started

### Requirements
* Java 11
* Maven
* Change stream is created on Spanner
