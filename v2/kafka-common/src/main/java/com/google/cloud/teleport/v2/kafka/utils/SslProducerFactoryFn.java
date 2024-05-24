/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.kafka.utils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create Kafka Producer with configured SSL. */
public class SslProducerFactoryFn
    implements SerializableFunction<Map<String, Object>, Producer<Void, String>> {
  private final Map<String, String> sslConfig;
  private static final String TRUSTSTORE_LOCAL_PATH = "/tmp/kafka.truststore.jks";
  private static final String KEYSTORE_LOCAL_PATH = "/tmp/kafka.keystore.jks";

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(SslProducerFactoryFn.class);

  public SslProducerFactoryFn(Map<String, String> sslConfig) {
    this.sslConfig = sslConfig;
  }

  @Override
  public Producer<Void, String> apply(Map<String, Object> config) {
    String bucket = sslConfig.get("bucket");
    String trustStorePath = sslConfig.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
    String keyStorePath = sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    String trustStorePassword = sslConfig.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    String keyStorePassword = sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    String keyPassword = sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
    String outputTrustStoreFilePath;
    String outputKeyStoreFilePath;
    try {
      outputTrustStoreFilePath = TRUSTSTORE_LOCAL_PATH;
      outputKeyStoreFilePath = KEYSTORE_LOCAL_PATH;
      getGcsFileAsLocal(bucket, trustStorePath, outputTrustStoreFilePath);
      getGcsFileAsLocal(bucket, keyStorePath, outputKeyStoreFilePath);
    } catch (IOException e) {
      LOG.error("Failed to retrieve data for SSL", e);
      return new KafkaProducer<>(config);
    }

    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
    config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, outputTrustStoreFilePath);
    config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, outputKeyStoreFilePath);
    config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
    config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
    config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);

    return new KafkaProducer<>(config);
  }

  /**
   * Reads a file from GCS and writes it locally.
   *
   * @param bucket GCS bucket name
   * @param filePath path to file in GCS
   * @param outputFilePath path where to save file locally
   * @throws IOException thrown if not able to read or write file
   */
  public static void getGcsFileAsLocal(String bucket, String filePath, String outputFilePath)
      throws IOException {
    String gcsFilePath = String.format("gs://%s/%s", bucket, filePath);
    LOG.info("Reading contents from GCS file: {}", gcsFilePath);
    Set<StandardOpenOption> options = new HashSet<>(2);
    options.add(StandardOpenOption.CREATE);
    options.add(StandardOpenOption.APPEND);
    // Copy the GCS file into a local file and will throw
    // an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
      try (FileChannel writeChannel = FileChannel.open(Paths.get(outputFilePath), options)) {
        writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
      }
    }
  }
}
