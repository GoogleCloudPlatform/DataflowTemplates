package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.Collections;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ProcessBackfillEventFnTest {
  private static final String DATABASE_NAME = "test_db";
  private static final String COLLECTION_NAME = "test_col";
  private static final int BATCH_SIZE = 2;

  private DataStreamMongoDBToFirestore.ProcessBackfillEventFn fn;
  private MongoClient mockClient;
  private MongoDatabase mockDatabase;
  private MongoCollection<Document> mockCollection;
  private MultiOutputReceiver mockReceiver;
  private OutputReceiver mockSuccessReceiver;
  private OutputReceiver mockFailureReceiver;
  private DoFn.ProcessContext mockContext;

  @Before
  public void setUp() {
    mockClient = mock(MongoClient.class);
    mockDatabase = mock(MongoDatabase.class);
    mockCollection = mock(MongoCollection.class);
    mockReceiver = mock(MultiOutputReceiver.class);
    mockSuccessReceiver = mock(OutputReceiver.class);
    mockFailureReceiver = mock(OutputReceiver.class);
    mockContext = mock(DoFn.ProcessContext.class);

    when(mockClient.getDatabase(DATABASE_NAME)).thenReturn(mockDatabase);
    when(mockDatabase.getCollection(COLLECTION_NAME)).thenReturn(mockCollection);
    when(mockReceiver.get(DataStreamMongoDBToFirestore.ProcessBackfillEventFn.successfulWriteTag)).thenReturn(mockSuccessReceiver);
    when(mockReceiver.get(DataStreamMongoDBToFirestore.ProcessBackfillEventFn.failedWriteTag)).thenReturn(mockFailureReceiver);

    fn = new DataStreamMongoDBToFirestore.ProcessBackfillEventFn(mockClient, DATABASE_NAME, BATCH_SIZE);
    fn.setup();
    fn.startBundle();
  }

  @Test
  public void testProcessBatch_partialFailure() {
    MongoDbChangeEventContext event1 = mock(MongoDbChangeEventContext.class);
    MongoDbChangeEventContext event2 = mock(MongoDbChangeEventContext.class);

    when(event1.getDataCollection()).thenReturn(COLLECTION_NAME);
    when(event2.getDataCollection()).thenReturn(COLLECTION_NAME);
    when(event1.getDocumentId()).thenReturn("id1");
    when(event2.getDocumentId()).thenReturn("id2");
    when(event1.getDataAsJsonString()).thenReturn("{\"data\": {\"_id\":\"id1\"}}");
    when(event2.getDataAsJsonString()).thenReturn("{\"data\": {\"_id\":\"id2\"}}");

    when(mockContext.element()).thenReturn(event1).thenReturn(event2);

    // Process first element
    fn.processElement(mockContext, mockReceiver);
    
    // Process second element, should trigger batch processing
    com.mongodb.bulk.BulkWriteError error = new com.mongodb.bulk.BulkWriteError(2, "At most 20 nested array/entity values are supported.", new org.bson.BsonDocument(), 1);
    com.mongodb.MongoBulkWriteException exception = new com.mongodb.MongoBulkWriteException(
        mock(com.mongodb.bulk.BulkWriteResult.class),
        Collections.singletonList(error),
        mock(com.mongodb.bulk.WriteConcernError.class),
        new com.mongodb.ServerAddress(),
        Collections.emptySet()
    );

    when(mockCollection.bulkWrite(anyList(), any())).thenThrow(exception);

    fn.processElement(mockContext, mockReceiver);

    // Verify output
    verify(mockSuccessReceiver, times(1)).output(event1);
    
    ArgumentCaptor<FailsafeElement> failureCaptor = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(mockFailureReceiver, times(1)).output(failureCaptor.capture());
    
    FailsafeElement failedElement = failureCaptor.getValue();
    assertEquals(event2, failedElement.getOriginalPayload());
    assertEquals("At most 20 nested array/entity values are supported.", failedElement.getErrorMessage());
  }
}
