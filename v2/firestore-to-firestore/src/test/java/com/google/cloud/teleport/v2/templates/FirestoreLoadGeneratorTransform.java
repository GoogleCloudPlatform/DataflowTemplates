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

import com.google.firestore.v1.Document;
import com.google.firestore.v1.MapValue;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.util.Timestamps;
import java.util.UUID;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PTransform to generate write load on a Firestore database.
 *
 * <p>This transform can be applied within a larger Dataflow pipeline, typically for integration or
 * load testing purposes.
 */
public class FirestoreLoadGeneratorTransform extends PTransform<PBegin, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreLoadGeneratorTransform.class);

  private static final int ESTIMATED_DOC_SIZE_BYTES = 235;

  private static final int TARGET_QPS = 100000;

  private final String projectId;
  private final String databaseId;
  private final String collectionId;
  private final int targetSizeGib;
  private final RpcQosOptions rpcQosOptions;

  /**
   * Configures the Firestore Load Generator.
   *
   * @param projectId The target Google Cloud project ID.
   * @param databaseId The target Firestore database ID (e.g., "(default)").
   * @param collectionId The Firestore collection to write documents to.
   * @param targetSizeGib Target total size in GiB.
   * @param rpcQosOptions QoS options for FirestoreIO.
   */
  public FirestoreLoadGeneratorTransform(
      String projectId,
      String databaseId,
      String collectionId,
      int targetSizeGib,
      RpcQosOptions rpcQosOptions) {
    this.projectId = projectId;
    this.databaseId = databaseId;
    this.collectionId = collectionId;
    this.targetSizeGib = targetSizeGib;
    this.rpcQosOptions = rpcQosOptions;
  }

  @Override
  public PDone expand(PBegin input) {
    LOG.info(
        "Initializing Firestore load generation: project={}, db={}, collection={}, targetGib={}",
        projectId,
        databaseId,
        collectionId,
        targetSizeGib);

    GenerateSequence sequence = GenerateSequence.from(0L).withRate(TARGET_QPS,
        Duration.standardSeconds(1));

    if (targetSizeGib <= 0) {
      throw new IllegalArgumentException(
          "Invalid target size for load generation: " + targetSizeGib);
    }
    long numElements = ((long) targetSizeGib * 1024 * 1024 * 1024) / ESTIMATED_DOC_SIZE_BYTES;
    sequence = sequence.to(numElements);
    LOG.info("Configured to generate approximately {} elements.", numElements);

    PCollection<Long> loadTrigger = input.apply("GenerateSequence", sequence);

    // Reshuffle to prevent fusion of the generation and write stages.
    // This allows the stages to scale independently and can help prevent
    // the GenerateSequence from becoming a bottleneck or causing memory issues
    // if the write sink is slow.
    PCollection<Long> reshuffledTrigger =
        loadTrigger.apply("ReshuffleBeforeWrites", Reshuffle.viaRandomKey());

    PCollection<Write> writes =
        reshuffledTrigger.apply(
            "CreateFirestoreWrites",
            ParDo.of(new GenerateWriteRequestsFn(projectId, databaseId, collectionId)));

    writes.apply("WriteToFirestore", FirestoreIO.v1()
        .write()
        .withProjectId(projectId)
        .withDatabaseId(databaseId)
        .batchWrite()
        .withRpcQosOptions(rpcQosOptions).build());

    return PDone.in(input.getPipeline());
  }

  // DoFn to convert generated Longs into Firestore Write requests
  private static class GenerateWriteRequestsFn extends DoFn<Long, Write> {

    private final String projectId;
    private final String databaseId;
    private final String collectionId;

    public GenerateWriteRequestsFn(String projectId, String databaseId, String collectionId) {
      this.projectId = projectId;
      this.databaseId = databaseId;
      this.collectionId = collectionId;
    }

    private String getDocumentPath(String docId) {
      return String.format(
          "projects/%s/databases/%s/documents/%s/%s", projectId, databaseId, collectionId, docId);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String docId = UUID.randomUUID().toString();
      Document doc =
          Document.newBuilder()
              .setName(getDocumentPath(docId))
              .putFields("uuid", Value.newBuilder().setStringValue(docId).build())
              .putFields("sequence", Value.newBuilder().setIntegerValue(c.element()).build())
              .putFields(
                  "timestamp",
                  Value.newBuilder().setTimestampValue(Timestamps.now()).build())
              .putFields(
                  "payload",
                  Value.newBuilder()
                      .setStringValue("Random payload " + Math.random() * 1000)
                      .build())
              .putFields("nested", Value.newBuilder().setMapValue(MapValue.newBuilder()
                      .putFields("field_a", Value.newBuilder().setStringValue("A").build())
                      .putFields("field_b", Value.newBuilder().setBooleanValue(true).build()))
                  .build())
              .build();
      c.output(Write.newBuilder().setUpdate(doc).build());
    }
  }
}
