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

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class KafkaTopicUtilsTest {

  private static final String PROJECT = "test-project";

  @Test
  public void testParsingBootstrapServerAndTopicFromGMK() {
    String input = "projects/project1/locations/us-central1/clusters/cluster1/topics/topic1";
    List<String> result = KafkaTopicUtils.getBootstrapServerAndTopic(input, PROJECT);
    Assert.assertEquals(
        result,
        List.of(
            "bootstrap.cluster1.us-central1.managedkafka.test-project.cloud.goog:9092", "topic1"));
  }

  @Test
  public void testParsingBootstrapServerAndTopic() {
    String input = "1.1.1.1:9094;topic1";
    List<String> result = KafkaTopicUtils.getBootstrapServerAndTopic(input, PROJECT);
    Assert.assertEquals(result, List.of("1.1.1.1:9094", "topic1"));
  }

  @Test
  public void testParsingBootstrapServerAndMultipleTopics() {
    String input = "1.1.1.1:9094;topic1,topic2,topic3";
    List<String> result = KafkaTopicUtils.getBootstrapServerAndTopic(input, PROJECT);
    Assert.assertEquals(result, List.of("1.1.1.1:9094", "topic1", "topic2", "topic3"));
  }
}
