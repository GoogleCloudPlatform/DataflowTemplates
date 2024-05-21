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

import static org.apache.hadoop.hdfs.DFSInotifyEventInputStream.LOG;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ProducerProperties} is a utility class for constructing properties for Kafka
 * producers. In this case, it is the Kafka destination where we write the data to.
 *
 * <p>The {@link ProducerProperties} class provides a static method to generate producer properties
 * required for configuring a Kafka producer. These properties are needed to establish connections
 * to Kafka brokers. They ensure security through SASL authentication. The properties should specify
 * the necessary authentication credentials in order to establish a successful connection to the
 * Kafka destination.
 */
final class ProducerProperties {
  private static final Logger LOGG = LoggerFactory.getLogger(ProducerProperties.class);
  private static void downloadFileFromGCS(String gcsUri, String localPath) throws IOException {
    Storage storage = StorageOptions.newBuilder().setProjectId("dataflow-testing-311516").setCredentials(
        GoogleCredentials.getApplicationDefault()).build().getService();//getDefaultInstance().getService();
    Blob blob = storage.get("testbucketktm",gcsUri);
    ReadChannel readChannel = blob.reader();
    FileOutputStream fileOutputStream;
    fileOutputStream = new FileOutputStream(localPath);
    fileOutputStream.getChannel().transferFrom(readChannel, 0, Long.MAX_VALUE);
    fileOutputStream.close();
    File f = new File(localPath);
    if (f.exists())
    {
      LOG.debug("key exists");

    }
    else
    {
      LOG.error("key does not exist");

    }
    // try (InputStream input = Channels.newInputStream(blob.reader());
    //     FileOutputStream output = new FileOutputStream(localPath)) {
    //   byte[] buffer = new byte[2048];
    //   int bytesRead;
    //   while ((bytesRead = input.read(buffer)) != -1) {
    //     output.write(buffer, 0, bytesRead);
    //   }
    // } catch (Exception e) {
    //   throw new RuntimeException("Failed to download file from GCS", e);
    // }
  }

  public static ImmutableMap<String, Object> get(KafkaToKafkaOptions options) throws IOException {
    downloadFileFromGCS("keystore.jks", "file:/tmp/keystore.jks");
    downloadFileFromGCS("truststore.jks","file:/tmp/truststore.jks");

    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    properties.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, options.getSourceBootstrapServers());

    //         Note: in other languages, set sasl.username and sasl.password instead.
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "file:/tmp/keystore.jks");
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "file:/tmp/truststore.jks");
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "123456");
    properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "123456");
    properties.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "123456");
    properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,"");
    // properties.put(
    //     SaslConfigs.SASL_JAAS_CONFIG,
    //     "org.apache.kafka.common.security.plain.PlainLoginModule required"
    //         + " username=\'"
    //         + SecretManagerUtils.getSecret(options.getSourceUsernameSecretId())
    //         + "\'"
    //         + " password=\'"
    //         + SecretManagerUtils.getSecret(options.getSourcePasswordSecretId())
    //         + "\';");

    return properties.buildOrThrow();
  }
}
