/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.transforms.SpannerToJsonTransform.StructToJson;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit test for {@link com.google.cloud.teleport.v2.transforms.SpannerToJsonTransform} methods.
 */
@RunWith(JUnit4.class)
public class SpannerToJsonTransformTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testStructToJsonValidInput() {
    // spotless:off
    Struct testStruct =
        Struct.newBuilder()
            .set("bool").to(Value.bool(true))
            .set("string").to(Value.string("test-string"))
            .set("int").to(Value.int64(2))
            .set("float").to(Value.float64(3.0))
            .build();
    // spotless:on

    PCollection<String> actualJson =
        pipeline.apply("Create", Create.of(testStruct)).apply(new StructToJson());

    String expectedJson = "{\"bool\":true,\"string\":\"test-string\",\"int\":2,\"float\":3.0}";
    PAssert.that(actualJson).containsInAnyOrder(expectedJson);
    pipeline.run();
  }
}
