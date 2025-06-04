/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.options;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;

public class BigtableChangeStreamsToKafkaOptionsUtils {
  public static void validate(BigtableChangeStreamsToKafkaOptions options) {
    // DLQ
    checkArgument(options.getDlqRetryMinutes() > 0, "dlqRetryMinutes must be positive");
    checkArgument(options.getDlqMaxRetries() >= 0, "dlqMaxRetries cannot be negative");

    // Kafka non-auth
    checkArgument(
        !options.getWriteBootstrapServerAndTopic().trim().isEmpty(),
        "WriteBootstrapServerAndTopic must be set");
    // Kafka auth
    if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      checkArgument(
          !options.getKafkaWriteUsernameSecretId().trim().isEmpty(),
          "KafkaWriteUsernameSecretId required to access username for destination Kafka");
      checkArgument(
          !options.getKafkaWritePasswordSecretId().trim().isEmpty(),
          "KafkaWritePasswordSecretId required to access password for destination Kafka");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.TLS)) {
      checkArgument(
          !options.getKafkaWriteTruststoreLocation().trim().isEmpty(),
          "KafkaWriteTruststoreLocation for trust store certificate required for ssl authentication");
      checkArgument(
          !options.getKafkaWriteTruststorePasswordSecretId().trim().isEmpty(),
          "KafkaWriteTruststorePasswordSecretId for trust store password required for accessing truststore");
      checkArgument(
          !options.getKafkaWriteKeystoreLocation().trim().isEmpty(),
          "KafkaWriteKeystoreLocation for key store location required for ssl authentication");
      checkArgument(
          !options.getKafkaWriteKeystorePasswordSecretId().trim().isEmpty(),
          "KafkaWriteKeystorePasswordSecretId for key store password required to access key store");
      checkArgument(
          !options.getKafkaWriteKeyPasswordSecretId().trim().isEmpty(),
          "KafkaWriteKeyPasswordSecretId for key password secret id version required for SSL authentication");
    } else if (options.getKafkaWriteAuthenticationMethod().equals(KafkaAuthenticationMethod.NONE)
        || options
            .getKafkaWriteAuthenticationMethod()
            .equals(KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS)) {
      // No additional validation is required for these auth mechanisms since they don't depend on
      // any specific pipeline options.
    } else {
      throw new IllegalArgumentException(
          "Kafka authentication method not supported: "
              + options.getKafkaWriteAuthenticationMethod());
    }

    if (options
        .getMessageFormat()
        .equals(KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {
      // Schema Registry non-auth
      checkArgument(
          !options.getSchemaRegistryConnectionUrl().trim().isEmpty(),
          "SchemaRegistryConnectionUrl must be set when MessageFormat is set to '"
              + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
              + "'");
      checkArgument(
          options.getSchemaFormat().equals(KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY),
          "SchemaFormat must be set to '"
              + KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY
              + "' when MessageFormat is set to '"
              + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
              + "'");
      // Schema Registry auth
      if (options.getSchemaRegistryAuthenticationMode().equals(KafkaAuthenticationMethod.TLS)) {
        checkArgument(
            !options.getSchemaRegistryTruststoreLocation().trim().isEmpty(),
            "SchemaRegistryTruststoreLocation for trust store certificate required for ssl authentication");
        checkArgument(
            !options.getSchemaRegistryTruststorePasswordSecretId().trim().isEmpty(),
            "SchemaRegistryTruststorePasswordSecretId for trust store password required for accessing truststore");
        checkArgument(
            !options.getSchemaRegistryKeystoreLocation().trim().isEmpty(),
            "SchemaRegistryKeystoreLocation for key store location required for ssl authentication");
        checkArgument(
            !options.getSchemaRegistryKeystorePasswordSecretId().trim().isEmpty(),
            "SchemaRegistryKeystorePasswordSecretId for key store password required to access key store");
        checkArgument(
            !options.getSchemaRegistryKeyPasswordSecretId().trim().isEmpty(),
            "SchemaRegistryKeyPasswordSecretId for source key password secret id version required for SSL authentication");
      } else if (options
          .getSchemaRegistryAuthenticationMode()
          .equals(KafkaAuthenticationMethod.OAUTH)) {
        checkArgument(
            !options.getSchemaRegistryOauthTokenEndpointUrl().trim().isEmpty(),
            "SchemaRegistryOauthTokenEndpointUrl for OAuth token endpoint URL required for oauth authentication");
        checkArgument(
            !options.getSchemaRegistryOauthClientId().trim().isEmpty(),
            "SchemaRegistryOauthClientId for OAuth client ID required for oauth authentication");
        checkArgument(
            !options.getSchemaRegistryOauthClientSecretId().trim().isEmpty(),
            "SchemaRegistryOauthClientSecretId for OAuth client secret ID required for oauth authentication");
      } else if (options
          .getSchemaRegistryAuthenticationMode()
          .equals(KafkaAuthenticationMethod.NONE)) {
        // No additional validation is required since it doesn't depend on any specific pipeline
        // options.
      } else {
        throw new IllegalArgumentException(
            "Schema Registry authentication method not supported: "
                + options.getSchemaRegistryAuthenticationMode());
      }
    }
  }
}
