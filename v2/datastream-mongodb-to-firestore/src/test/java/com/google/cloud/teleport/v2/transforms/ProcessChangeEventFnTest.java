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

import static com.mongodb.client.model.Filters.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link ProcessChangeEventFn}. */
@RunWith(JUnit4.class)
public class ProcessChangeEventFnTest {
  private static final String DATABASE_NAME = "test_db";
  private static final String DATA_COLLECTION = "test_data";
  private static final String SHADOW_COLLECTION = "shadow_test_data";
  private static final String DOC_ID = "test_doc_id";
  private static final Bson LOOKUP_BY_DOC_ID = eq("_id", DOC_ID);

  private ProcessChangeEventFn processFn;
  private DoFn.ProcessContext mockContext;
  private MultiOutputReceiver mockReceiver;
  private OutputReceiver mockSuccessReceiver;
  private OutputReceiver mockFailureReceiver;
  private MongoClient mockClient;
  private MongoDatabase mockDatabase;
  private ClientSession mockSession;
  private MongoCollection<Document> mockDataCollection;
  private MongoCollection<Document> mockShadowCollection;
  private MongoDbChangeEventContext mockElement;
  private Document mockTimestampDocNewer;
  private Document mockTimestampDocOlder;
  private Document mockShadowDocOlder;
  private Document mockShadowDocNewer;
  private Document mockDataDoc;
  private Document mockShadowDocElement;
  private FindIterable<Document> mockFindIterable;

  @Before
  public void setUp() throws Exception {
    mockContext = mock(DoFn.ProcessContext.class);
    mockReceiver = mock(MultiOutputReceiver.class);
    mockSuccessReceiver = mock(OutputReceiver.class);
    mockFailureReceiver = mock(OutputReceiver.class);
    mockClient = mock(MongoClient.class);
    mockDatabase = mock(MongoDatabase.class);
    mockSession = mock(ClientSession.class);
    mockDataCollection = mock(MongoCollection.class);
    mockShadowCollection = mock(MongoCollection.class);
    mockElement = mock(MongoDbChangeEventContext.class);
    mockTimestampDocNewer = new Document("seconds", 2L).append("nanos", 0);
    mockTimestampDocOlder = new Document("seconds", 1L).append("nanos", 0);
    mockShadowDocOlder =
        new Document(MongoDbChangeEventContext.TIMESTAMP_COL, mockTimestampDocOlder);
    mockShadowDocNewer =
        new Document(MongoDbChangeEventContext.TIMESTAMP_COL, mockTimestampDocNewer);
    mockDataDoc =
        new Document(
            "data",
            new Document(MongoDbChangeEventContext.DOC_ID_COL, DOC_ID).append("field", "value"));
    mockShadowDocElement =
        new Document(MongoDbChangeEventContext.SHADOW_DOC_ID_COL, DOC_ID)
            .append(MongoDbChangeEventContext.TIMESTAMP_COL, mockTimestampDocNewer);
    mockFindIterable = mock(FindIterable.class);
    processFn = new ProcessChangeEventFn(mockClient, DATABASE_NAME);

    when(mockClient.getDatabase(DATABASE_NAME)).thenReturn(mockDatabase);
    when(mockClient.startSession()).thenReturn(mockSession);
    when(mockDatabase.getCollection(DATA_COLLECTION)).thenReturn(mockDataCollection);
    when(mockDatabase.getCollection(SHADOW_COLLECTION)).thenReturn(mockShadowCollection);
    doNothing().when(mockSession).startTransaction();
    doNothing().when(mockSession).commitTransaction();
    doNothing().when(mockSession).abortTransaction();
    doNothing().when(mockSession).close();
    doNothing().when(mockClient).close();

    when(mockContext.element()).thenReturn(mockElement);
    when(mockElement.getDataCollection()).thenReturn(DATA_COLLECTION);
    when(mockElement.getShadowCollection()).thenReturn(SHADOW_COLLECTION);
    when(mockElement.getDocumentId()).thenReturn(DOC_ID);
    when(mockElement.getTimestampDoc()).thenReturn(mockTimestampDocNewer);
    when(mockElement.getDataAsJsonString()).thenReturn(mockDataDoc.toJson());
    when(mockElement.getShadowDocument()).thenReturn(mockShadowDocElement);
    when(mockShadowCollection.find(mockSession, LOOKUP_BY_DOC_ID)).thenReturn(mockFindIterable);

    // Mock the MultiOutputReceiver's get() method
    when(mockReceiver.get(ProcessChangeEventFn.successfulWriteTag)).thenReturn(mockSuccessReceiver);
    when(mockReceiver.get(ProcessChangeEventFn.failedWriteTag)).thenReturn(mockFailureReceiver);
  }

  @Test
  public void testProcessElementInsertOrUpdateNewer() {
    when(mockFindIterable.first()).thenReturn(null);
    UpdateResult mockUpdateResult = mock(UpdateResult.class);
    when(mockDataCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockDataDoc, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);
    when(mockShadowCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockShadowDocElement, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockSession).commitTransaction();

    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementInsertOrUpdateNewerExisting() {
    when(mockFindIterable.first()).thenReturn(mockShadowDocOlder);
    UpdateResult mockUpdateResult = mock(UpdateResult.class);
    when(mockDataCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockDataDoc, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);
    when(mockShadowCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockShadowDocElement, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockSession).commitTransaction();
    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementInsertOrUpdateOlderExisting() {
    when(mockElement.getTimestampDoc()).thenReturn(mockTimestampDocOlder);
    when(mockFindIterable.first()).thenReturn(mockShadowDocNewer);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockDataCollection, never()).replaceOne(any(), any(), any(), any());
    verify(mockShadowCollection, never()).replaceOne(any(), any(), any(), any());
    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementDeleteNewer() {
    when(mockElement.isDeleteEvent()).thenReturn(true);
    when(mockFindIterable.first()).thenReturn(null);
    DeleteResult mockDeleteResult = mock(DeleteResult.class);
    when(mockDataCollection.deleteOne(mockSession, LOOKUP_BY_DOC_ID)).thenReturn(mockDeleteResult);
    UpdateResult mockUpdateResult = mock(UpdateResult.class);
    when(mockShadowCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockShadowDocElement, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockDataCollection).deleteOne(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockSession).commitTransaction();
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementDeleteNewerExisting() {
    when(mockElement.isDeleteEvent()).thenReturn(true);
    when(mockFindIterable.first()).thenReturn(mockShadowDocOlder);
    DeleteResult mockDeleteResult = mock(DeleteResult.class);
    when(mockDataCollection.deleteOne(mockSession, LOOKUP_BY_DOC_ID)).thenReturn(mockDeleteResult);
    UpdateResult mockUpdateResult = mock(UpdateResult.class);
    when(mockShadowCollection.replaceOne(
            mockSession, LOOKUP_BY_DOC_ID, mockShadowDocElement, new ReplaceOptions().upsert(true)))
        .thenReturn(mockUpdateResult);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockDataCollection).deleteOne(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockSession).commitTransaction();
    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementDeleteOlderExisting() {
    when(mockElement.isDeleteEvent()).thenReturn(true);
    when(mockElement.getTimestampDoc()).thenReturn(mockTimestampDocOlder);
    when(mockFindIterable.first()).thenReturn(mockShadowDocNewer);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection).find(mockSession, LOOKUP_BY_DOC_ID);
    verify(mockDataCollection, never()).deleteOne(any(), any(), any());
    verify(mockShadowCollection, never()).replaceOne(any(), any(), any(), any());
    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.successfulWriteTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    verify(mockSession, never()).abortTransaction();
  }

  @Test
  public void testProcessElementTransientError_mixedErrors() {
    MongoException transientError = new MongoException("Fake transient error.");
    transientError.addLabel("TransientTransactionError");
    when(mockShadowCollection.find(mockSession, LOOKUP_BY_DOC_ID))
        .thenThrow(transientError)
        .thenThrow(new RuntimeException("Permanent error"));

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection, times(4)).find(mockSession, LOOKUP_BY_DOC_ID);
    ArgumentCaptor<MongoDbChangeEventContext> failureCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.failedWriteTag);
    verify(mockFailureReceiver, times(1)).output(failureCaptor.capture());
    verify(mockSession, never()).commitTransaction();
  }

  @Test
  public void testProcessElementTransientError_retryTillMaximum() {
    MongoException randomError = new MongoException("Fake transient error.");
    when(mockShadowCollection.find(mockSession, LOOKUP_BY_DOC_ID)).thenThrow(randomError);

    processFn.processElement(mockContext, mockReceiver);

    verify(mockShadowCollection, times(4)).find(mockSession, LOOKUP_BY_DOC_ID);
    ArgumentCaptor<MongoDbChangeEventContext> failureCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(ProcessChangeEventFn.failedWriteTag);
    verify(mockFailureReceiver, times(1)).output(failureCaptor.capture());
    verify(mockSession, never()).commitTransaction();
  }
}
