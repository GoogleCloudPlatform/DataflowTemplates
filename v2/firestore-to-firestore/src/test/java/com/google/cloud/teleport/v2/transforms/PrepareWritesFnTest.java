/*
 * Copyright (C) 2025 Google LLC
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

import com.google.common.collect.ImmutableList;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class PrepareWritesFnTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPrepareWritesFn_correctlyTransformsName() {
    String sourceName = "projects/source-proj/databases/(default)/documents/myCol/docId1";
    Document inputDoc =
        Document.newBuilder()
            .setName(sourceName)
            .putFields("message", Value.newBuilder().setStringValue("Hello").build())
            .build();

    String destProject = "dest-proj";
    String destDb = "dest-db";

    PCollection<Document> input = p.apply(Create.of(inputDoc));
    PCollection<Write> output = input.apply(ParDo.of(new PrepareWritesFn(destProject, destDb)));

    String expectedName = "projects/dest-proj/databases/dest-db/documents/myCol/docId1";
    Document expectedDoc = inputDoc.toBuilder().setName(expectedName).build();
    Write expectedWrite = Write.newBuilder().setUpdate(expectedDoc).build();

    PAssert.that(output).containsInAnyOrder(expectedWrite);
    p.run();
  }

  @Test
  public void testPrepareWritesFn_preservesFields() {
    String sourceName = "projects/source/databases/(default)/documents/data/item1";
    MapValue nestedMap =
        MapValue.newBuilder()
            .putFields("nested", Value.newBuilder().setBooleanValue(true).build())
            .build();
    Document inputDoc =
        Document.newBuilder()
            .setName(sourceName)
            .putFields("fieldA", Value.newBuilder().setStringValue("valueA").build())
            .putFields("fieldB", Value.newBuilder().setIntegerValue(123).build())
            .putFields("fieldC", Value.newBuilder().setMapValue(nestedMap).build())
            .build();

    PCollection<Document> input = p.apply(Create.of(inputDoc));
    PCollection<Write> output = input.apply(ParDo.of(new PrepareWritesFn("dest", "(default)")));

    // Extract the Document from the Write
    PCollection<Document> outputDocs =
        output.apply("ExtractUpdate", ParDo.of(new ExtractDocumentFromWriteFn()));

    // Assert that the fields map is as expected.
    PCollection<Map<String, Value>> outputFields =
        outputDocs.apply("ExtractFields", ParDo.of(new ExtractFieldsFn()));

    // Pass the expected map as a List to PAssert
    PAssert.that(outputFields).containsInAnyOrder(ImmutableList.of(inputDoc.getFieldsMap()));

    p.run();
  }

  private static class ExtractDocumentFromWriteFn extends DoFn<Write, Document> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null && c.element().hasUpdate()) {
        c.output(c.element().getUpdate());
      }
    }
  }

  private static class ExtractFieldsFn extends DoFn<Document, Map<String, Value>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null) {
        c.output(c.element().getFieldsMap());
      }
    }
  }
}
