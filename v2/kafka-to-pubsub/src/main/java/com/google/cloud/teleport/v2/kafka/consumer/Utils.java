/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.kafka.consumer;

import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.BUCKET;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.KAFKA_CREDENTIALS;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.PASSWORD;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.SSL_CREDENTIALS;
import static com.google.cloud.teleport.v2.templates.KafkaPubsubConstants.USERNAME;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.io.CharStreams;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for construction of Kafka Consumer. */
public class Utils {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  /**
   * Retrieves all credentials from HashiCorp Vault secret storage.
   *
   * @param secretStoreUrl url to the secret storage that contains a credentials for Kafka
   * @param token Vault token to access the secret storage
   * @return credentials for Kafka consumer config
   */
  public static Map<String, Map<String, String>> getKafkaCredentialsFromVault(
      String secretStoreUrl, String token) {

    JsonObject credentials = null;
    try {
      HttpClient client = HttpClientBuilder.create().build();
      HttpGet request = new HttpGet(secretStoreUrl);
      request.addHeader("X-Vault-Token", token);
      LOG.info("Fetching kafka credentials from vault");
      HttpResponse response = client.execute(request);
      String json = EntityUtils.toString(response.getEntity(), "UTF-8");

      /*
       Vault's response JSON has a specific schema, where the actual data is placed under
       {data: {data: <actual data>}}.
       Example:
         {
           "request_id": "6a0bb14b-ef24-256c-3edf-cfd52ad1d60d",
           "lease_id": "",
           "renewable": false,
           "lease_duration": 0,
           "data": {
             "data": {
               "bucket": "kafka_to_pubsub_test",
               "ssl.key.password": "secret",
               "ssl.keystore.password": "secret",
               "ssl.keystore.location": "ssl_cert/kafka.keystore.jks",
               "ssl.keystore.type": "JKS",
               "password": "admin-secret",
               "ssl.truststore.password": "secret",
               "ssl.truststore.location": "ssl_cert/kafka.truststore.jks",
               "ssl.truststore.type": "JKS",
               "username": "admin",
               "sasl.mechanism": "SCRAM-SHA-256",
               "group.id": "group_id"
             },
             "metadata": {
               "created_time": "2020-10-20T11:43:11.109186969Z",
               "deletion_time": "",
               "destroyed": false,
               "version": 8
             }
           },
           "wrap_info": null,
           "warnings": null,
           "auth": null
         }
      */
      // Parse security properties from the response JSON
      credentials =
          JsonParser.parseString(json)
              .getAsJsonObject()
              .get("data")
              .getAsJsonObject()
              .getAsJsonObject("data");
    } catch (IOException e) {
      LOG.error("Failed to retrieve credentials from Vault.", e);
    }

    return parseCredentialsJson(credentials);
  }

  /**
   * Configures Kafka consumer for authorized connection and group ID if provided
   * <p>
   * If no SASL mechanism is provided, defaults to SCRAM-SHA-512.
   *
   * @param props username, password, SASL mechanism, and group ID for Kafka
   * @return configuration set of parameters for Kafka
   */
  public static Map<String, Object> configureKafka(Map<String, String> props) {
    // Create the configuration for Kafka
    Map<String, Object> config = new HashMap<>();
    if (props == null) {
      return config;
    }

    if (props.containsKey(USERNAME) && props.containsKey(PASSWORD)) {
      String saslMechanism = props.get(SaslConfigs.SASL_MECHANISM);
      if (Objects.equals(saslMechanism, ScramMechanism.SCRAM_SHA_256.mechanismName()) ||
          Objects.equals(saslMechanism, ScramMechanism.SCRAM_SHA_512.mechanismName())
      ) {
        config.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
      } else {
        config.put(SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_512.mechanismName());
        LOG.warn("No valid SASL hash mechanism was provided. SCRAM-SHA-512 was set.");
      }

      config.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          String.format(
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
                  + "username=\"%s\" password=\"%s\";",
              props.get(USERNAME), props.get(PASSWORD)));
    }
    ConsumerConfig.configNames().stream()
        .filter(props::containsKey)
        .map(configName -> validateConfigValue(configName, props.get(configName)))
        .forEach(configName -> config.put(configName, props.get(configName)));
    return config;
  }

  /**
   * Retrieves all credentials from Google Cloud Storage.
   *
   * @param kafkaOptionsGcsPath GCS path that contains a credentials for Kafka in JSON format.
   *                            Should be in the following format: `gs://bucket-name/path/to/file`
   * @return credentials for Kafka consumer config
   */
  public static Map<String, Map<String, String>> getKafkaCredentialsFromGCS(
      String kafkaOptionsGcsPath) {
    JsonObject credentials = null;
    try {
      String json = getGcsFileAsString(kafkaOptionsGcsPath);
      /*
       Parse security properties from the JSON that may have the following keys:
       {
         "bucket": "kafka_to_pubsub_test",
         "ssl.key.password": "secret",
         "ssl.keystore.password": "secret",
         "ssl.keystore.location": "ssl_cert/kafka.keystore.jks",
         "ssl.keystore.type": "JKS",
         "password": "admin-secret",
         "ssl.truststore.password": "secret",
         "ssl.truststore.location": "ssl_cert/kafka.truststore.jks",
         "ssl.truststore.type": "JKS",
         "username": "admin",
         "sasl.mechanism": "SCRAM-SHA-256",
         "group.id": "group_id"
       }
      */
      credentials =
          JsonParser.parseString(json)
              .getAsJsonObject();
    } catch (IOException e) {
      LOG.error("Failed to retrieve credentials from GCS.", e);
    }

    return parseCredentialsJson(credentials);
  }

  /**
   * Reads a file from GCS and returns it as a string.
   *
   * @param filePath path to file in GCS
   * @return contents of the file as a string
   * @throws IOException thrown if not able to read file
   */
  public static String getGcsFileAsString(String filePath) throws IOException {
    LOG.info("Reading contents from GCS file: {}", filePath);
    Set<StandardOpenOption> options = new HashSet<>(2);
    options.add(StandardOpenOption.CREATE);
    options.add(StandardOpenOption.APPEND);
    // Read the GCS file into a string and will throw an I/O exception in case file not found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(filePath).resourceId())) {
      Reader reader = Channels.newReader(readerChannel, StandardCharsets.UTF_8.name());
      return CharStreams.toString(reader);
    }
  }

  /**
   * Parses credentials for Kafka from JSON.
   *
   * @param credentials JSON with credentials for Kafka
   * @return Map with both authorization and ssl options if applicable
   */
  private static Map<String, Map<String, String>> parseCredentialsJson(JsonObject credentials) {
    Map<String, Map<String, String>> credentialMap = new HashMap<>();
    if (credentials != null) {
      // Username, password, and SASL mechanism for Kafka authorization
      credentialMap.put(KAFKA_CREDENTIALS, new HashMap<>());

      if (credentials.has(USERNAME) && credentials.has(PASSWORD)) {
        credentialMap.get(KAFKA_CREDENTIALS).put(USERNAME, credentials.get(USERNAME).getAsString());
        credentialMap.get(KAFKA_CREDENTIALS).put(PASSWORD, credentials.get(PASSWORD).getAsString());
        if (credentials.has(SaslConfigs.SASL_MECHANISM)) {
          credentialMap.get(KAFKA_CREDENTIALS).put(SaslConfigs.SASL_MECHANISM,
              credentials.get(SaslConfigs.SASL_MECHANISM).getAsString());
        }
      } else {
        LOG.warn(
            "There are no username and/or password for Kafka."
                + "Trying to initiate an unauthorized connection.");
      }

      // Kafka consumer configs
      ConsumerConfig.configNames().stream()
          .filter(credentials::has)
          .forEach(configName -> credentialMap
              .get(KAFKA_CREDENTIALS)
              .put(configName, credentials.get(configName).getAsString()));

      // SSL truststore, keystore, and password
      try {
        Map<String, String> sslCredentials = new HashMap<>();
        String[] configNames = {
            BUCKET,
            SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
            SslConfigs.SSL_KEY_PASSWORD_CONFIG,
            SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
            SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
        };
        for (String configName : configNames) {
          sslCredentials.put(configName, credentials.get(configName).getAsString());
        }
        credentialMap.put(SSL_CREDENTIALS, sslCredentials);
      } catch (NullPointerException e) {
        LOG.warn(
            "There is no enough information to configure SSL."
                + "Trying to initiate an unsecure connection.",
            e);
      }
    }
    return credentialMap;
  }

  private static String validateConfigValue(String key, String value) {
    Map<String, String> property = Collections.singletonMap(key, value);
    ConfigValue configValue = ConsumerConfig.configDef().validate(property).get(0);
    if (!configValue.errorMessages().isEmpty()) {
      throw new ConfigException(key, configValue, configValue.errorMessages().toString());
    }
    return key;
  }
}
