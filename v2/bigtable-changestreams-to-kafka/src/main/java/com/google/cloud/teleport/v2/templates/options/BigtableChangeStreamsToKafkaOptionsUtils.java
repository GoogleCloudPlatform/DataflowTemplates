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

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.cloud.teleport.v2.templates.model.BigtableSource;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;

public class BigtableChangeStreamsToKafkaOptionsUtils {
  public static Instant getStartTimestamp(BigtableChangeStreamsToKafkaOptions options) {
    if (StringUtils.isEmpty(options.getBigtableChangeStreamStartTimestamp())) {
      return Instant.now();
    } else {
      Timestamp ts = Timestamp.parseTimestamp(options.getBigtableChangeStreamStartTimestamp());
      return new Instant(ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L);
    }
  }

  public static BigtableSource buildBigtableSource(BigtableChangeStreamsToKafkaOptions options) {
    return new BigtableSource(
        options.getBigtableReadInstanceId(),
        options.getBigtableReadTableId(),
        StringUtils.isEmpty(options.getBigtableChangeStreamCharset())
            ? "UTF-8"
            : options.getBigtableChangeStreamCharset(),
        options.getBigtableChangeStreamIgnoreColumnFamilies(),
        options.getBigtableChangeStreamIgnoreColumns());
  }

  public static void validate(BigtableChangeStreamsToKafkaOptions options) {
    // Kafka non-auth
    checkArgument(
        StringUtils.isNotBlank(options.getWriteBootstrapServerAndTopic()),
        "WriteBootstrapServerAndTopic must be set");
    // Kafka auth
    if (KafkaAuthenticationMethod.SASL_PLAIN.equals(options.getKafkaWriteAuthenticationMethod())) {
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteUsernameSecretId()),
          "KafkaWriteUsernameSecretId required to access username for destination Kafka");
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWritePasswordSecretId()),
          "KafkaWritePasswordSecretId required to access password for destination Kafka");
    } else if (KafkaAuthenticationMethod.TLS.equals(options.getKafkaWriteAuthenticationMethod())) {
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteTruststoreLocation()),
          "KafkaWriteTruststoreLocation for trust store certificate required for ssl authentication");
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteTruststorePasswordSecretId()),
          "KafkaWriteTruststorePasswordSecretId for trust store password required for accessing truststore");
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteKeystoreLocation()),
          "KafkaWriteKeystoreLocation for key store location required for ssl authentication");
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteKeystorePasswordSecretId()),
          "KafkaWriteKeystorePasswordSecretId for key store password required to access key store");
      checkArgument(
          StringUtils.isNotBlank(options.getKafkaWriteKeyPasswordSecretId()),
          "KafkaWriteKeyPasswordSecretId for key password secret id version required for SSL authentication");
    } else if (KafkaAuthenticationMethod.NONE.equals(options.getKafkaWriteAuthenticationMethod())
        || KafkaAuthenticationMethod.APPLICATION_DEFAULT_CREDENTIALS.equals(
            options.getKafkaWriteAuthenticationMethod())) {
      // No additional validation is required for these auth mechanisms since they don't depend on
      // any specific pipeline options.
    } else {
      throw new IllegalArgumentException(
          "Kafka authentication method not supported: "
              + options.getKafkaWriteAuthenticationMethod());
    }

    if (KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT.equals(
        options.getMessageFormat())) {
      // Schema Registry non-auth
      checkArgument(
          StringUtils.isNotBlank(options.getSchemaRegistryConnectionUrl()),
          "SchemaRegistryConnectionUrl must be set when MessageFormat is set to '"
              + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
              + "'");
      checkArgument(
          KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY.equals(options.getSchemaFormat()),
          "SchemaFormat must be set to '"
              + KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY
              + "' when MessageFormat is set to '"
              + KafkaTemplateParameters.MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT
              + "'");
      // Schema Registry auth
      if (KafkaAuthenticationMethod.TLS.equals(options.getSchemaRegistryAuthenticationMode())) {
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryTruststoreLocation()),
            "SchemaRegistryTruststoreLocation for trust store certificate required for ssl authentication");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryTruststorePasswordSecretId()),
            "SchemaRegistryTruststorePasswordSecretId for trust store password required for accessing truststore");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryKeystoreLocation()),
            "SchemaRegistryKeystoreLocation for key store location required for ssl authentication");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryKeystorePasswordSecretId()),
            "SchemaRegistryKeystorePasswordSecretId for key store password required to access key store");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryKeyPasswordSecretId()),
            "SchemaRegistryKeyPasswordSecretId for source key password secret id version required for SSL authentication");
      } else if (KafkaAuthenticationMethod.OAUTH.equals(
          options.getSchemaRegistryAuthenticationMode())) {
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryOauthTokenEndpointUrl()),
            "SchemaRegistryOauthTokenEndpointUrl for OAuth token endpoint URL required for oauth authentication");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryOauthClientId()),
            "SchemaRegistryOauthClientId for OAuth client ID required for oauth authentication");
        checkArgument(
            StringUtils.isNotBlank(options.getSchemaRegistryOauthClientSecretId()),
            "SchemaRegistryOauthClientSecretId for OAuth client secret ID required for oauth authentication");
      } else if (KafkaAuthenticationMethod.NONE.equals(
          options.getSchemaRegistryAuthenticationMode())) {
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
