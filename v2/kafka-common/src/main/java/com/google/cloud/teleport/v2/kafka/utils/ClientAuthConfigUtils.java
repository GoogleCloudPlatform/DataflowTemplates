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
package com.google.cloud.teleport.v2.kafka.utils;

import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

public class ClientAuthConfigUtils {

  private static final String TRUSTSTORE_LOCATION = "/tmp/kafka.truststore.jks";

  private static final String KEYSTORE_LOCATION = "/tmp/kafka.keystore.jks";

  /**
   * Method to create Kafka Client Authentication Config with the given username and password.
   *
   * @param usernameSecretId Kafka username stored in Secret Manager.
   * @param passwordSecretId Kafka password stored in Secret Manager.
   * @return ImmutableMap object.
   */
  public static ImmutableMap<String, Object> setSaslPlainConfig(
      String usernameSecretId, String passwordSecretId) {
    String username = SecretManagerUtils.getSecret(usernameSecretId);
    String password = SecretManagerUtils.getSecret(passwordSecretId);
    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\'"
            + username
            + "\'"
            + " password=\'"
            + password
            + "\';");
    return properties.buildOrThrow();
  }

  /**
   * Method to create Kafka Client Authentication Config with the given username and password.
   *
   * @param truststoreLocation JKS truststore location in GCS.
   * @param truststorePasswordSecretId Truststore password stored in Secret Manager.
   * @param keystoreLocation JKS keystore location in GCS.
   * @param keystorePasswordSecretId Keystore password stored in Secret Manager.
   * @param keyPasswordSecretId Key password stored in Secret Manager.
   * @return ImmutableMap object.
   */
  public static ImmutableMap<String, Object> setSslConfig(
      String truststoreLocation,
      String truststorePasswordSecretId,
      String keystoreLocation,
      String keystorePasswordSecretId,
      String keyPasswordSecretId,
      Boolean download)
      throws IOException {

    String truststorePassword = SecretManagerUtils.getSecret(truststorePasswordSecretId);
    String keystorePassword = SecretManagerUtils.getSecret(keystorePasswordSecretId);
    String keyPassword = SecretManagerUtils.getSecret(keyPasswordSecretId);

    ImmutableMap.Builder<String, Object> config = ImmutableMap.<String, Object>builder();
    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
    config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
    config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);

    if (download) {
      getGcsFileAsLocal(truststoreLocation, TRUSTSTORE_LOCATION);
      getGcsFileAsLocal(keystoreLocation, KEYSTORE_LOCATION);
      config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, TRUSTSTORE_LOCATION);
      config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, KEYSTORE_LOCATION);
    } else {
      config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
      config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
    }
    return config.buildOrThrow();
  }

  /**
   * Reads a file from GCS and writes it locally.
   *
   * @param bucket GCS bucket name
   * @param filePath path to file in GCS
   * @param outputFilePath path where to save file locally
   * @throws IOException thrown if not able to read or write file
   */
  public static void getGcsFileAsLocal(String gcsFilePath, String outputFilePath)
      throws IOException {

    File f = new File(outputFilePath);
    if (f.exists() && !f.isDirectory()) {
      return;
    }
    // LOG.info("Reading contents from GCS file: {}", gcsFilePath);
    Set<StandardOpenOption> options = new HashSet<>(2);
    options.add(StandardOpenOption.CREATE);
    options.add(StandardOpenOption.APPEND);
    // Copy the GCS file into a local file and will throw an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
      try (FileChannel writeChannel = FileChannel.open(Paths.get(outputFilePath), options)) {
        writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
      }
    }
  }
}
