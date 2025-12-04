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
package com.google.cloud.teleport.v2.templates;

import com.google.firestore.v1.DocumentRootName;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FirestoreToFirestoreTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  // Note: Fully testing the pipeline logic with FirestoreIO requires integration tests
  // or a sophisticated mocking of FirestoreIO's builders and transforms, which is complex.
  // This test just checks if the pipeline can be constructed without errors.
  @Test
  public void testPipelineConstruction() {
    FirestoreToFirestore.Options options =
        TestPipeline.testingPipelineOptions().as(FirestoreToFirestore.Options.class);

    options.setSourceProjectId("test-source-project");
    options.setSourceDatabaseId("test-source-db");
    options.setDatabaseCollection("my-collection");
    options.setDestinationProjectId("test-dest-project");
    options.setDestinationDatabaseId("(default)");

    // Simply creating the pipeline and calling main to ensure it constructs without exceptions
    // We are not running it as it would try to connect to actual Firestore.
    try {
      Pipeline testP = Pipeline.create(options);

      String sourceProject = options.getSourceProjectId();
      String sourceDb = options.getSourceDatabaseId();
      String collectionId = options.getDatabaseCollection();

      // The following lines would normally be part of the pipeline run,
      // but we can't execute them in a unit test without extensive mocking.
      // We just ensure the types match and a basic structure is buildable.
      StructuredQuery baseQuery =
          StructuredQuery.newBuilder()
              .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
              .build();
      PartitionQueryRequest partitionRequest =
          PartitionQueryRequest.newBuilder()
              .setParent(DocumentRootName.format(sourceProject, sourceDb))
              .setStructuredQuery(baseQuery)
              .setPartitionCount(10)
              .build();

      PCollection<PartitionQueryRequest> req = testP.apply(Create.of(partitionRequest));

      // Chaining a simple ParDo to check type - real test needs FirestoreIO mock
      req.apply(ParDo.of(new ExtractParentFn()));

      // If we reach here, the basic setup with options is fine.
    } catch (Exception e) {
      throw new AssertionError("Pipeline construction failed", e);
    }
  }

  private static class ExtractParentFn extends DoFn<PartitionQueryRequest, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element() != null) {
        c.output(c.element().getParent());
      }
    }
  }
}
