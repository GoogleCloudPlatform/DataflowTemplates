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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DataStreamToSpannerTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testGetSourceTypeWithDatastreamSourceType() {
    String[] args = new String[] {"--datastreamSourceType=mysql"};
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);

    assertEquals("mysql", result);
  }

  @Test
  public void testGetSourceTypeWithEmptyStreamName() {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Stream name cannot be empty.");
    String[] args = new String[] {""};
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);
  }

  @Test
  public void testGetSourceTypeWithGcpCredentialsMissing() {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Unable to initialize DatastreamClient:");
    String[] args =
        new String[] {
          "--streamName=projects/sample-project/locations/sample-location/streams/sample-stream"
        };
    DataStreamToSpanner.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamToSpanner.Options.class);
    String result = DataStreamToSpanner.getSourceType(options);
  }
}
