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

import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link JmsToPubsub} class. */
@RunWith(JUnit4.class)
public class JmsToPubsubTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testValidation() {
    JmsToPubsub.JmsToPubsubOptions options =
        PipelineOptionsFactory.create().as(JmsToPubsub.JmsToPubsubOptions.class);
    options.setUsername("test");
    options.setPassword("");
    assertThrows(IllegalArgumentException.class, () -> JmsToPubsub.validate(options));
  }
}
