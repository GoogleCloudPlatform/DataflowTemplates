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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils.getKafkaCredentialsFromVault;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Test class for {@link PubsubToKafka}. */
public class PubsubToKafkaTest {

  /** Tests getKafkaCredentialsFromVault() with an invalid url. */
  @Test
  public void testGetKafkaCredentialsFromVaultInvalidUrl() {
    Map<String, Map<String, String>> credentials =
        getKafkaCredentialsFromVault("some-url", "some-token");
    Assert.assertEquals(credentials, new HashMap<>());
  }
}
