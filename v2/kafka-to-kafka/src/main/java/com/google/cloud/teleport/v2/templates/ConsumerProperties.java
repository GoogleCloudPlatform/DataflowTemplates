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








import com.google.cloud.teleport.v2.kafka.utils.FileAwareConsumerFactoryFn;
import com.google.cloud.teleport.v2.kafka.values.KafkaAuthenticationMethod;


import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;



import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;


import org.apache.kafka.common.config.SslConfigs;


/**
 * The {@link ConsumerProperties} is a utility class for constructing properties for Kafka
 * consumers. In this case, it is the Kafka source where we read the data from.
 *
 * <p>The {@link ConsumerProperties} class provides a static method to generate consumer properties
 * required for configuring a Kafka consumer. These properties are needed to establish connections
 * to Kafka brokers. They ensure security through SASL authentication. The properties should specify
 * the necessary authentication credentials in order to establish successful connection to the
 * source Kafka.
 */
final class ConsumerProperties {



  public static Map<String, Object> from(KafkaToKafkaOptions options) throws IOException {
    Map<String, Object> properties = new HashMap<>();
    String authMethod = options.getSourceAuthenticationMethod();
    if (authMethod == null) {
      return properties;

    }


    if (authMethod.equals(KafkaAuthenticationMethod.SSL)) {

      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KafkaAuthenticationMethod.SSL);
      properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, options.getSourceKeystoreLocation());
      properties.put(
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, options.getSourceTruststoreLocation());
      properties.put(
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          FileAwareConsumerFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSourceTruststorePasswordSecretId());
      properties.put(
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          FileAwareConsumerFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSourceKeystorePasswordSecretId());
      properties.put(
          SslConfigs.SSL_KEY_PASSWORD_CONFIG,
          FileAwareConsumerFactoryFn.SECRET_MANAGER_VALUE_PREFIX
              + options.getSourceKeyPasswordSecretId());
      properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaOffset());

    } else if (authMethod.equals(KafkaAuthenticationMethod.SASL_PLAIN)) {
      properties.put(SaslConfigs.SASL_MECHANISM, KafkaAuthenticationMethod.SASL_MECHANISM);

      //         Note: in other languages, set sasl.username and sasl.password instead.
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KafkaAuthenticationMethod.SASL_PLAIN);
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required"
              + " username=\'"
              + SecretManagerUtils.getSecret(options.getSourceUsernameSecretId())
              + "\'"
              + " password=\'"
              + SecretManagerUtils.getSecret(options.getSourcePasswordSecretId())
              + "\';");


      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.getKafkaOffset());
    } else {
      throw new UnsupportedEncodingException("Authentication method not supported: " + authMethod);


    }
    return properties;
  }
}
