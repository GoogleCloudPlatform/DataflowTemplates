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

import com.google.cloud.teleport.v2.kafka.options.KafkaReadOptions;
import com.google.cloud.teleport.v2.kafka.options.KafkaWriteOptions;
import com.google.cloud.teleport.v2.kafka.options.SchemaRegistryOptions;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 * The {@link KafkaConfig} is a utility class for constructing properties for Kafka consumers and
 * producers.
 */
public class KafkaConfig {
  private static final String SCHEMA_REGISTRY_PREFIX = "schema.registry.";

  public static Map<String, Object> fromReadOptions(KafkaReadOptions options) {
    Map<String, Object> properties =
        from(
            options.getKafkaReadAuthenticationMode(),
            options.getKafkaReadKeystoreLocation(),
            options.getKafkaReadTruststoreLocation(),
            options.getKafkaReadTruststorePasswordSecretId(),
            options.getKafkaReadKeystorePasswordSecretId(),
            options.getKafkaReadKeyPasswordSecretId(),
            options.getKafkaReadUsernameSecretId(),
            options.getKafkaReadPasswordSecretId());

    properties.putAll(KafkaCommonUtils.configureKafkaOffsetCommit(options));

    return properties;
  }

  public static Map<String, Object> fromWriteOptions(KafkaWriteOptions options) {
    return from(
        options.getKafkaWriteAuthenticationMethod(),
        options.getKafkaWriteKeystoreLocation(),
        options.getKafkaWriteTruststoreLocation(),
        options.getKafkaWriteTruststorePasswordSecretId(),
        options.getKafkaWriteKeystorePasswordSecretId(),
        options.getKafkaWriteKeyPasswordSecretId(),
        options.getKafkaWriteUsernameSecretId(),
        options.getKafkaWritePasswordSecretId());
  }

  public static Map<String, Object> fromSchemaRegistryOptions(SchemaRegistryOptions options) {
    Map<String, Object> properties = new HashMap<>();
    if (options.getSchemaRegistryAuthenticationMode() == null
        || options.getSchemaRegistryAuthenticationMode().equals(KafkaAuthenticationMethod.NONE)) {
      return properties;
    }

    if (options.getSchemaRegistryAuthenticationMode().equals(KafkaAuthenticationMethod.TLS)) {
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          options.getSchemaRegistryKeystoreLocation());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
          options.getSchemaRegistryTruststoreLocation());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSchemaRegistryTruststorePasswordSecretId());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSchemaRegistryKeystorePasswordSecretId());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSchemaRegistryKeyPasswordSecretId());
      properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    } else if (options
        .getSchemaRegistryAuthenticationMode()
        .equals(KafkaAuthenticationMethod.OAUTH)) {
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE,
          "OAUTHBEARER");
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SchemaRegistryClientConfig.BEARER_AUTH_ISSUER_ENDPOINT_URL,
          options.getSchemaRegistryOauthTokenEndpointUrl());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_ID,
          options.getSchemaRegistryOauthClientId());
      properties.put(
          SCHEMA_REGISTRY_PREFIX + SchemaRegistryClientConfig.BEARER_AUTH_CLIENT_SECRET,
          SecretManagerUtils.getSecret(options.getSchemaRegistryOauthClientSecretId()));
      if (options.getSchemaRegistryOauthScope() != null) {
        properties.put(
            SCHEMA_REGISTRY_PREFIX + SchemaRegistryClientConfig.BEARER_AUTH_SCOPE,
            options.getSchemaRegistryOauthScope());
      }
    }
    return properties;
  }

  private static Map<String, Object> from(
      String authMode,
      String keystoreLocation,
      String truststoreLocation,
      String truststorePasswordSecretId,
      String keystorePasswordSecretId,
      String keyPasswordSecretId,
      String usernameSecretId,
      String passwordSecretId) {
    Map<String, Object> properties = new HashMap<>();
    if (authMode == null || authMode.equals(KafkaAuthenticationMethod.NONE)) {
      return properties;
    }

    if (authMode.equals(KafkaAuthenticationMethod.TLS)) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
      properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
      properties.put(
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX + truststorePasswordSecretId);
      properties.put(
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX + keystorePasswordSecretId);
      properties.put(
          SslConfigs.SSL_KEY_PASSWORD_CONFIG,
          FileAwareFactoryFn.SECRET_MANAGER_VALUE_PREFIX + keyPasswordSecretId);
      properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    } else if (authMode.equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required"
              + " username=\'"
              + SecretManagerUtils.getSecret(usernameSecretId)
              + "\'"
              + " password=\'"
              + SecretManagerUtils.getSecret(passwordSecretId)
              + "\';");
    } else if (authMode.equals(KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS)) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
      properties.put(
          SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
          "com.google.cloud.teleport.v2.kafka.auth.GcpLoginCallbackHandler");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
    } else {
      throw new RuntimeException("Authentication method not supported: " + authMode);
    }
    return properties;
  }
}
