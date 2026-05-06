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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link CreateMongoDbChangeEventContextFn}. */
@RunWith(JUnit4.class)
public class CreateMongoDbChangeEventContextFnTest {

  private static final String SHADOW_PREFIX = "shadow_";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private CreateMongoDbChangeEventContextFn createFn;
  private ProcessContext mockContext;
  private MultiOutputReceiver mockReceiver;
  private OutputReceiver mockSuccessReceiver;
  private OutputReceiver mockFailureReceiver;
  private FailsafeElement<String, String> successElement;
  private FailsafeElement<String, String> failureElement;
  private JsonNode validJsonNode;

  @Before
  public void setUp() throws Exception {
    createFn = new CreateMongoDbChangeEventContextFn(SHADOW_PREFIX);
    mockContext = mock(DoFn.ProcessContext.class);
    mockReceiver = mock(MultiOutputReceiver.class);
    mockSuccessReceiver = mock(OutputReceiver.class);
    mockFailureReceiver = mock(OutputReceiver.class);

    // Stub the get() method of MultiOutputReceiver
    when(mockReceiver.get(CreateMongoDbChangeEventContextFn.successfulCreationTag))
        .thenReturn(mockSuccessReceiver);
    when(mockReceiver.get(CreateMongoDbChangeEventContextFn.failedCreationTag))
        .thenReturn(mockFailureReceiver);

    String validPayload =
        """
            {
              "_metadata_source": {
                "collection": "test_collection"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": {
                "field1": "testString"
              },
              "_metadata_timestamp_seconds": 1683782270,
              "_metadata_timestamp_nanos": 123456789
            }""";
    successElement = FailsafeElement.of(validPayload, validPayload);
    when(mockContext.element()).thenReturn(successElement);
    validJsonNode = OBJECT_MAPPER.readTree(validPayload);

    failureElement = FailsafeElement.of("invalid json", "invalid json");
  }

  @Test
  public void testProcessElementSuccess() {
    when(mockContext.element()).thenReturn(successElement);

    createFn.processElement(mockContext, mockReceiver);

    ArgumentCaptor<MongoDbChangeEventContext> successCaptor =
        ArgumentCaptor.forClass(MongoDbChangeEventContext.class);
    verify(mockReceiver).get(CreateMongoDbChangeEventContextFn.successfulCreationTag);
    verify(mockSuccessReceiver, times(1)).output(successCaptor.capture());
    MongoDbChangeEventContext actualContext = successCaptor.getValue();

    assertEquals("test_collection", actualContext.getDataCollection());
    assertEquals(SHADOW_PREFIX + "test_collection", actualContext.getShadowCollection());
    assertEquals("645c9a7e7b8b1a0e9c0f8b3a", actualContext.getDocumentId().toString());
  }

  @Test
  public void testProcessElementIgnoreUpdateWithNullData() throws Exception {
    String updateWithNullDataPayload =
        """
            {
              "_metadata_source": {
                "collection": "test_collection"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": null,
              "_metadata_timestamp_seconds": 1683782270,
              "_metadata_timestamp_nanos": 123456789,
              "_metadata_change_type": "UPDATE"
            }""";
    FailsafeElement<String, String> updateElement = FailsafeElement.of(updateWithNullDataPayload, updateWithNullDataPayload);
    when(mockContext.element()).thenReturn(updateElement);

    createFn.processElement(mockContext, mockReceiver);

    // Verify that it does NOT call output on success receiver or failure receiver
    verify(mockSuccessReceiver, times(0)).output(org.mockito.Mockito.any());
    verify(mockFailureReceiver, times(0)).output(org.mockito.Mockito.any());
  }

  @Test
  public void testProcessElementFailureInvalidJson() throws Exception {
    when(mockContext.element()).thenReturn(failureElement);

    createFn.processElement(mockContext, mockReceiver);

    ArgumentCaptor<FailsafeElement<String, String>> failureCaptor =
        ArgumentCaptor.forClass(FailsafeElement.class);
    verify(mockReceiver).get(CreateMongoDbChangeEventContextFn.failedCreationTag);
    verify(mockFailureReceiver, times(1)).output(failureCaptor.capture());

    FailsafeElement<String, String> result = failureCaptor.getValue();
    assertEquals(failureElement, result);
    org.junit.Assert.assertNotNull(result.getErrorMessage());
    org.junit.Assert.assertNotNull(result.getStacktrace());
    org.junit.Assert.assertTrue(result.getErrorMessage().contains("Unrecognized token 'invalid'"));
  }

  @Test
  public void testProcessElementIgnoreUpdateWithNullData_incrementsCounter() {
    String updateWithNullDataPayload =
        """
            {
              "_metadata_source": {
                "collection": "test_collection"
              },
              "_id": "{\\\"$oid\\\": \\\"645c9a7e7b8b1a0e9c0f8b3a\\\"}",
              "data": null,
              "_metadata_timestamp_seconds": 1683782270,
              "_metadata_timestamp_nanos": 123456789,
              "_metadata_change_type": "UPDATE"
            }""";
    
    FailsafeElement<String, String> element = FailsafeElement.of(updateWithNullDataPayload, updateWithNullDataPayload);
    
    PCollectionTuple results = pipeline.apply(Create.of(element).withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(ParDo.of(createFn)
                    .withOutputTags(CreateMongoDbChangeEventContextFn.successfulCreationTag,
                            TupleTagList.of(CreateMongoDbChangeEventContextFn.failedCreationTag)));
                            
    results.get(CreateMongoDbChangeEventContextFn.successfulCreationTag).setCoder(SerializableCoder.of(MongoDbChangeEventContext.class));
    results.get(CreateMongoDbChangeEventContextFn.failedCreationTag).setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
                            
    PAssert.that(results.get(CreateMongoDbChangeEventContextFn.successfulCreationTag)).empty();
    PAssert.that(results.get(CreateMongoDbChangeEventContextFn.failedCreationTag)).empty();
    
    PipelineResult result = pipeline.run();
    
    MetricQueryResults metrics = result.metrics().queryMetrics(
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.named(CreateMongoDbChangeEventContextFn.class, "deletion-after-update"))
            .build());
            
    long count = metrics.getCounters().iterator().next().getCommitted();
    assertEquals(1L, count);
  }
}
