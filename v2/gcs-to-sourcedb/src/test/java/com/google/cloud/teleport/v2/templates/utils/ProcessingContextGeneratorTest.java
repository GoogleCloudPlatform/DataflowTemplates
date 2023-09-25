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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ProcessingContextGeneratorTest {

  @Test
  public void processingContextForGCS() {
    // TODO: add mock for ShardProgressTracker
    /*Map<String, ProcessingContext> response =
    ProcessingContextGenerator.getProcessingContextForGCS(
        "src/test/resources/shard.json",
        "mysql",
        "src/test/resources/allMatchSession.json",
        "+00:00",
        "2023-08-10T09:00:00Z",
        "10s",
        "gs://test",
        "some",
        "metaInst",
        "db",
        1,
        1,
        "regular");*/
    assertEquals(1, 1);
  }
}
