package org.apache.beam.it.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class test {

  private static final Logger LOG = LoggerFactory.getLogger(test.class);

  public static void main(String[] args) {
    KafkaResourceManager rm = KafkaResourceManager.builder("test").setHost("192.0.0.2").build();

    String topicName = rm.createTopic("test", 5);

    KafkaProducer<String, String> kafkaProducer =
        rm.buildProducer(new StringSerializer(), new StringSerializer());

    for (int i = 1; i <= 10; i++) {
      publish(kafkaProducer, topicName, i + "1", "{\"id\": " + i + "1, \"name\": \"Dataflow\"}");
      publish(kafkaProducer, topicName, i + "2", "{\"id\": " + i + "2, \"name\": \"Pub/Sub\"}");
      // Invalid schema
      publish(
          kafkaProducer, topicName, i + "3", "{\"id\": " + i + "3, \"description\": \"Pub/Sub\"}");

      try {
        TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    rm.cleanupAll();
  }

  private static void publish(
      KafkaProducer<String, String> producer, String topicName, String key, String value) {
    try {
      RecordMetadata recordMetadata =
          producer.send(new ProducerRecord<>(topicName, key, value)).get();
      LOG.info(
          "Published record {}, partition {} - offset: {}",
          recordMetadata.topic(),
          recordMetadata.partition(),
          recordMetadata.offset());
    } catch (Exception e) {
      throw new RuntimeException("Error publishing record to Kafka", e);
    }
  }
}
