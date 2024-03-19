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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ProcessingContextGeneratorTest {

  @Test
  public void processingContextForKafka() {

    Map<String, ProcessingContext> response =
        ProcessingContextGenerator.getProcessingContextForKafka(
            "src/test/resources/shard.json",
            "mysql",
            "src/test/resources/kafkaConnectionProfile.json",
            "src/test/resources/allMatchSession.json",
            "+00:00");

    assertEquals(response.size(), 2);
    assertEquals(response.get("shardA").getBufferType(), "kafka");
  }

  @Test
  public void processingContextForPubSub() {
    Map<String, ProcessingContext> response =
        ProcessingContextGenerator.getProcessingContextForPubSub(
            "src/test/resources/shard.json",
            "mysql",
            "tada",
            "src/test/resources/allMatchSession.json",
            42,
            "+00:00");

    assertEquals(response.size(), 2);
    assertEquals(response.get("shardA").getBufferType(), "pubsub");
  }
}
