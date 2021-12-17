/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link KeyValueToGenericRecordFn}. */
@RunWith(JUnit4.class)
public class KeyValueToGenericRecordFnTest {

  /** Rule for pipeline testing. */
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test whether {@link KeyValueToGenericRecordFn} correctly maps the message. */
  @Test
  @Category(NeedsRunner.class)
  public void testKeyValueToGenericRecordFn() throws Exception {

    // Create the test input.
    final String key = "Name";
    final String value = "Generic";
    final KV<String, String> message = KV.of(key, value);

    // Apply the ParDo.
    PCollection<GenericRecord> results =
        pipeline
            .apply(
                "CreateInput",
                Create.of(message)
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply("GenericRecordCreation", ParDo.of(new KeyValueToGenericRecordFn()))
            .setCoder(AvroCoder.of(GenericRecord.class, KeyValueToGenericRecordFn.SCHEMA));

    // Assert on the results.
    PAssert.that(results)
        .satisfies(
            collection -> {
              GenericRecord result = collection.iterator().next();
              assertThat(result.get("message").toString(), is(equalTo(value)));
              assertThat(result.get("attributes").toString(), is(equalTo("{Name=Generic}")));
              return null;
            });
    // Run the pipeline.
    pipeline.run();
  }
}
