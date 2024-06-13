/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.common.io.Resources;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.kafka.KafkaResourceManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(KafkaToGcsFlex.class)
@RunWith(JUnit4.class)
public class KafkaToGcsAvroBinaryIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToGcsAvroBinaryIT.class);
  private KafkaResourceManager kafkaResourceManager;
  private static final Pattern RESULT_REGEX = Pattern.compile(".*.json.");
  private Schema avroSchema;
  private String prefix;
  private String recordClassName = "AvroProductKafkaRecord";

  @Before
  public void setup() throws IOException {
    kafkaResourceManager =
        KafkaResourceManager.builder(testName).setHost(TestProperties.hostIp()).build();
    URL avroschemaResource = Resources.getResource("KafkaToGcsAvroBinaryIT/avro_schema.avsc");
    gcsClient.uploadArtifact("avro_schema.avsc", avroschemaResource.getPath());
    avroSchema = new Schema.Parser().parse(avroschemaResource.openStream());
    prefix = testName + '-';
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(kafkaResourceManager);
  }

  @Test
  public void testKafkaToGcsBinaryEncoding() throws IOException {
    baseKafkaToGcs(
        b ->
            b.addParameter(
                    "messageFormat",
                    KafkaTemplateParameters.MessageFormatConstants.AVRO_BINARY_ENCODING)
                .addParameter("binaryAvroSchemaPath", getGcsPath("avro_schema.avsc")));
  }

  private void baseKafkaToGcs(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {

    // Arrange
    String topicName = kafkaResourceManager.createTopic(testName, 5);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter(
                    "readBootstrapServerAndTopic",
                    kafkaResourceManager.getBootstrapServers().replace("PLAINTEXT://", "")
                        + ";"
                        + topicName)
                .addParameter("windowDuration", "10s")
                .addParameter("kafkaReadOffset", "earliest")
                .addParameter("outputDirectory", getGcsPath(testName))
                .addParameter("outputFilenamePrefix", prefix)
                .addParameter("numShards", "2")
                .addParameter("kafkaReadAuthenticationMode", "NONE"));

    KafkaProducer<String, byte[]> producer =
        kafkaResourceManager.buildProducer(new StringSerializer(), new ByteArraySerializer());

    // Create GenericRecord
    HashSet<GenericRecord> genericRecords = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = createRecord(i, "Kafka templates", i);
      genericRecords.add(record);
      publish(producer, topicName, String.valueOf(i), convertGenericRecordToBytes(record));
    }

    // Assert
    List<GenericRecord> avroRecordsInGcsBucket =
        readAvroTestResult(
            TestProperties.project(), getGcsBasePath(), prefix + "/" + recordClassName);
    for (GenericRecord genericRecord : avroRecordsInGcsBucket) {
      assert genericRecords.contains(genericRecord);
    }
  }

  private GenericRecord createRecord(int id, String productName, double value) {
    return new GenericRecordBuilder(avroSchema)
        .set("productId", id)
        .set("productName", productName)
        .build();
  }

  // Reads the avro files from Gcs and returns List of GenericRecords
  public List<GenericRecord> readAvroTestResult(
      String projectID, String bucketName, String prefix) {
    List<GenericRecord> records = new ArrayList<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectID).build().getService();
    Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(prefix));
    for (Blob blob : blobs.iterateAll()) {
      byte[] content = blob.getContent();
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      try (DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<>(new SeekableByteArrayInput(content), datumReader)) {
        for (GenericRecord record : dataFileReader) {
          records.add(record);
          System.out.println(record);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return records;
  }

  public byte[] convertGenericRecordToBytes(GenericRecord record) {
    try {
      GenericDatumWriter<GenericRecord> userDatumWriter =
          new GenericDatumWriter<>(record.getSchema());
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      userDatumWriter.write(record, encoder);
      encoder.flush();
      byte[] serializedBytes = outputStream.toByteArray();
      outputStream.close();
      return serializedBytes;
    } catch (Exception e) {
      throw new RuntimeException("Error serializing Avro message to bytes");
    }
  }

  private void publish(
      KafkaProducer<String, byte[]> producer, String topicName, String key, byte[] value) {
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
