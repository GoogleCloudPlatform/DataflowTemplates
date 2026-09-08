/*
 * Copyright (C) 2026 Google LLC
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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.ErrorType;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.FailureStage;
import com.google.cloud.teleport.v2.transforms.DocumentWithMetadata.OperationType;
import com.google.cloud.teleport.v2.transforms.TimestampSortKey;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link MongoDbToMongoDb}. */
@RunWith(JUnit4.class)
public class MongoDbToMongoDbTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void options_defaultValues() {
    MongoDbToMongoDb.Options options =
        PipelineOptionsFactory.as(MongoDbToMongoDb.Options.class);

    assertEquals("BACKFILL_AND_STREAMING", options.getMigrationMode());
    assertEquals(Integer.valueOf(1), options.getNumChangeStreamSplits());
    assertEquals("updateLookup", options.getChangeStreamFullDocument());
    assertEquals(Integer.valueOf(5000), options.getBatchSize());
    assertEquals(Integer.valueOf(10), options.getMaxConcurrentAsyncWrites());
    assertEquals(Integer.valueOf(3), options.getMaxWriteRetries());
    assertEquals(Integer.valueOf(3), options.getDlqMaxRetries());
    assertEquals(Integer.valueOf(200000), options.getTargetBackfillChunkSize());
    assertEquals(Integer.valueOf(256), options.getMaxBackfillSplits());
    assertEquals(Integer.valueOf(128), options.getMaxConcurrentBackfillReads());
    assertEquals(Integer.valueOf(64), options.getNumWriteShards());
    assertEquals(Integer.valueOf(200), options.getMaxBufferingDurationMs());
    assertEquals(Boolean.FALSE, options.getReadFromDlq());
  }

  @Test
  public void options_customValues() {
    String[] args = {
      "--sourceUri=mongodb://localhost:27017",
      "--targetUri=mongodb://localhost:27018",
      "--sourceDatabase=srcDb",
      "--targetDatabase=tgtDb",
      "--sourceCollection=srcCol",
      "--targetCollection=tgtCol",
      "--migrationMode=STREAMING_CDC",
      "--numChangeStreamSplits=4",
      "--changeStreamFullDocument=whenAvailable",
      "--startAtOperationTime=1700000000",
      "--targetBackfillChunkSize=100000",
      "--maxBackfillSplits=128",
      "--maxConcurrentBackfillReads=64",
      "--numWriteShards=128",
      "--maxBufferingDurationMs=100",
      "--batchSize=1000",
      "--maxConcurrentAsyncWrites=20"
    };

    MongoDbToMongoDb.Options options =
        PipelineOptionsFactory.fromArgs(args).as(MongoDbToMongoDb.Options.class);

    assertEquals("mongodb://localhost:27017", options.getSourceUri());
    assertEquals("mongodb://localhost:27018", options.getTargetUri());
    assertEquals("srcDb", options.getSourceDatabase());
    assertEquals("tgtDb", options.getTargetDatabase());
    assertEquals("srcCol", options.getSourceCollection());
    assertEquals("tgtCol", options.getTargetCollection());
    assertEquals("STREAMING_CDC", options.getMigrationMode());
    assertEquals(Integer.valueOf(4), options.getNumChangeStreamSplits());
    assertEquals("whenAvailable", options.getChangeStreamFullDocument());
    assertEquals("1700000000", options.getStartAtOperationTime());
    assertEquals(Integer.valueOf(100000), options.getTargetBackfillChunkSize());
    assertEquals(Integer.valueOf(128), options.getMaxBackfillSplits());
    assertEquals(Integer.valueOf(64), options.getMaxConcurrentBackfillReads());
    assertEquals(Integer.valueOf(128), options.getNumWriteShards());
    assertEquals(Integer.valueOf(100), options.getMaxBufferingDurationMs());
    assertEquals(Integer.valueOf(1000), options.getBatchSize());
    assertEquals(Integer.valueOf(20), options.getMaxConcurrentAsyncWrites());
  }

  @Test
  public void options_batchMode() {
    String[] args = {
      "--sourceUri=mongodb://localhost:27017",
      "--targetUri=mongodb://localhost:27018",
      "--sourceDatabase=srcDb",
      "--targetDatabase=tgtDb",
      "--migrationMode=BATCH"
    };

    MongoDbToMongoDb.Options options =
        PipelineOptionsFactory.fromArgs(args).as(MongoDbToMongoDb.Options.class);

    assertEquals("BATCH", options.getMigrationMode());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void validateFn_validInsertDocument_outputsToMainTag() {
    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};
    MongoDbToMongoDb.ValidateFn fn = new MongoDbToMongoDb.ValidateFn(failureTag);

    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    DocumentWithMetadata doc =
        DocumentWithMetadata.of(new Document("_id", 1), "src", "tgt");
    when(context.element()).thenReturn(doc);

    fn.processElement(context);

    verify(context).output(doc);
    verify(context, never()).output(eq(failureTag), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void validateFn_validDeleteEvent_outputsToMainTag() {
    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};
    MongoDbToMongoDb.ValidateFn fn = new MongoDbToMongoDb.ValidateFn(failureTag);

    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    String docKey = "{\"_id\": 100}";
    DocumentWithMetadata deleteDoc =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "src",
            "tgt",
            OperationType.DELETE,
            TimestampSortKey.cdc(1700000000L, 1),
            docKey);
    when(context.element()).thenReturn(deleteDoc);

    fn.processElement(context);

    verify(context).output(deleteDoc);
    verify(context, never()).output(eq(failureTag), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void validateFn_validDropEvent_outputsToMainTag() {
    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};
    MongoDbToMongoDb.ValidateFn fn = new MongoDbToMongoDb.ValidateFn(failureTag);

    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    DocumentWithMetadata dropDoc =
        DocumentWithMetadata.cdcEvent(
            null,
            null,
            "src",
            "tgt",
            OperationType.DROP,
            TimestampSortKey.cdc(1700000000L, 1),
            null);
    when(context.element()).thenReturn(dropDoc);

    fn.processElement(context);

    verify(context).output(dropDoc);
    verify(context, never()).output(eq(failureTag), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void validateFn_nullPayloadNonDelete_outputsToFailureTag() {
    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};
    MongoDbToMongoDb.ValidateFn fn = new MongoDbToMongoDb.ValidateFn(failureTag);

    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    DocumentWithMetadata invalidDoc =
        DocumentWithMetadata.of(null, "src", "tgt");
    when(context.element()).thenReturn(invalidDoc);

    fn.processElement(context);

    ArgumentCaptor<DocumentWithMetadata> captor =
        ArgumentCaptor.forClass(DocumentWithMetadata.class);
    verify(context).output(eq(failureTag), captor.capture());

    DocumentWithMetadata failure = captor.getValue();
    assertEquals(ErrorType.PERMANENT, failure.getErrorType());
    assertEquals(FailureStage.VALIDATE, failure.getFailureStage());
    assertEquals("Null document payload", failure.getErrorMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void validateFn_nullItem_outputsToFailureTag() {
    TupleTag<DocumentWithMetadata> failureTag = new TupleTag<DocumentWithMetadata>() {};
    MongoDbToMongoDb.ValidateFn fn = new MongoDbToMongoDb.ValidateFn(failureTag);

    DoFn<DocumentWithMetadata, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    when(context.element()).thenReturn(null);

    fn.processElement(context);

    ArgumentCaptor<DocumentWithMetadata> captor =
        ArgumentCaptor.forClass(DocumentWithMetadata.class);
    verify(context).output(eq(failureTag), captor.capture());

    DocumentWithMetadata failure = captor.getValue();
    assertEquals(ErrorType.PERMANENT, failure.getErrorType());
    assertEquals(FailureStage.VALIDATE, failure.getFailureStage());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void parseDlqFn_validJson_outputsDocumentWithMetadata() {
    MongoDbToMongoDb.ParseDlqFn fn = new MongoDbToMongoDb.ParseDlqFn();

    DoFn<String, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    Document doc = new Document("_id", 42).append("key", "val");
    DocumentWithMetadata original =
        DocumentWithMetadata.of(doc, "src", "tgt");
    String dlqJson = original.toDlqJson("test error", ErrorType.RETRYABLE, 1);

    when(context.element()).thenReturn(dlqJson);

    fn.processElement(context);

    ArgumentCaptor<DocumentWithMetadata> captor =
        ArgumentCaptor.forClass(DocumentWithMetadata.class);
    verify(context).output(captor.capture());

    DocumentWithMetadata result = captor.getValue();
    assertNotNull(result);
    assertEquals(Integer.valueOf(42), result.getDocument().get("_id"));
    assertEquals("src", result.getSourceCollection());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void parseDlqFn_invalidJson_handlesGracefully() {
    MongoDbToMongoDb.ParseDlqFn fn = new MongoDbToMongoDb.ParseDlqFn();

    DoFn<String, DocumentWithMetadata>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    when(context.element()).thenReturn("invalid json {{{");

    fn.processElement(context);

    verify(context, never()).output(any());
  }
}
