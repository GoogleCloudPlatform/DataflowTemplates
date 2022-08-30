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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.transforms.SpannerToJsonTransform.StructToJson;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link SpannerToElasticsearch}. */
public class SpannerToElasticsearchTest {
  private static final String a = "testing";
  private static final long b = 1001;
  private static final Struct struct =
    Struct.newBuilder().set("a").to(a).set("b").to(b).build();
  private static final List<Struct> rows = ImmutableList.of(struct);
  private static final String jsonifiedTableRow =
      "{\"a\":\"testing\",\"b\":1001}";
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Test the {@link SpannerToElasticsearch} pipeline end-to-end. */
  @Test
  public void testSpannerToElasticsearchE2E() {

    // Build pipeline
    PCollection<String> testStrings =
        pipeline
            .apply("CreateInput", Create.of(rows))
            .apply("TestStructToJson", new StructToJson());

    PAssert.that(testStrings)
        .satisfies(
            collection -> {
              String result = collection.iterator().next();
              assertThat(result, is(equalTo(jsonifiedTableRow)));
              return null;
            });

    // Execute pipeline
    pipeline.run();
  }
}
