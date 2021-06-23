/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.kafka.utils;

import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.PASSWORD;
import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.USERNAME;
import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.configureKafka;
import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.getKafkaCredentialsFromVault;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.Assert;
import org.junit.Test;

/** Test class for {@link Utils}. */
public class UtilsTest {
  /** Tests configureKafka() with a null input properties. */
  @Test
  public void testConfigureKafkaNullProps() {
    Map<String, Object> config = configureKafka(null);
    Assert.assertEquals(config, new HashMap<>());
  }

  /** Tests configureKafka() without a Password in input properties. */
  @Test
  public void testConfigureKafkaNoPassword() {
    Map<String, String> props = new HashMap<>();
    props.put(USERNAME, "username");
    Map<String, Object> config = configureKafka(props);
    Assert.assertEquals(config, new HashMap<>());
  }

  /** Tests configureKafka() without a Username in input properties. */
  @Test
  public void testConfigureKafkaNoUsername() {
    Map<String, String> props = new HashMap<>();
    props.put(PASSWORD, "password");
    Map<String, Object> config = configureKafka(props);
    Assert.assertEquals(config, new HashMap<>());
  }

  /** Tests configureKafka() with an appropriate input properties. */
  @Test
  public void testConfigureKafka() {
    Map<String, String> props = new HashMap<>();
    props.put(USERNAME, "username");
    props.put(PASSWORD, "password");

    Map<String, Object> expectedConfig = new HashMap<>();
    expectedConfig.put(SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_512.mechanismName());
    expectedConfig.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"%s\" password=\"%s\";",
            props.get(USERNAME), props.get(PASSWORD)));

    Map<String, Object> config = configureKafka(props);
    Assert.assertEquals(config, expectedConfig);
  }

  /** Tests getKafkaCredentialsFromVault() with an invalid url. */
  @Test
  public void testGetKafkaCredentialsFromVaultInvalidUrl() {
    Map<String, Map<String, String>> credentials =
        getKafkaCredentialsFromVault("some-url", "some-token");
    Assert.assertEquals(credentials, new HashMap<>());
  }
}
