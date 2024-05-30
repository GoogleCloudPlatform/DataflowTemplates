/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.transform;

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_CHANGE_TYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.spanner.exceptions.TransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ChangeEventTransformerDoFnTest {
  @Test
  public void testProcessElementWithNonEmptySchema() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put("last_name", "Depp");
    changeEvent.put("age", 13);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    ObjectNode changeEvent2 = changeEvent.deepCopy();
    changeEvent2.put("synth_id", 123);

    when(schema.isEmpty()).thenReturn(false);
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(changeEventSessionConvertor.transformChangeEventViaSessionFile(eq(changeEvent)))
        .thenReturn(changeEvent2);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(schema, null, "mysql", customTransformation);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"synth_id\":123}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformation()
      throws InvalidChangeEventException, TransformationException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put("last_name", "Depp");
    changeEvent.put("age", 13);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    Map<String, Object> sourceRecord =
        ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);
    MigrationTransformationRequest expectedRequest =
        new MigrationTransformationRequest("Users", sourceRecord, "", "INSERT");
    Map<String, Object> spannerRecord = new HashMap<>(sourceRecord);
    spannerRecord.put("age", 10);
    MigrationTransformationResponse migrationTransformationResponse =
        new MigrationTransformationResponse(spannerRecord, false);

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(spannerMigrationTransformer.toSpannerRow(expectedRequest))
        .thenReturn(migrationTransformationResponse);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(schema, null, "mysql", customTransformation);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":10,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\"}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformationAndShardId()
      throws InvalidChangeEventException, TransformationException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    Map<String, String> schemaToShardId = new HashMap<>();
    schemaToShardId.put("db1", "shard1");
    TransformationContext transformationContext = new TransformationContext(schemaToShardId);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put(DatastreamConstants.EVENT_SCHEMA_KEY, "db1");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put("last_name", "Depp");
    changeEvent.put("age", 13);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    Map<String, Object> sourceRecord =
        ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);
    MigrationTransformationRequest expectedRequest =
        new MigrationTransformationRequest("Users", sourceRecord, "shard1", "INSERT");
    Map<String, Object> spannerRecord = new HashMap<>(sourceRecord);
    spannerRecord.put("shardId", "shard1");
    MigrationTransformationResponse migrationTransformationResponse =
        new MigrationTransformationResponse(spannerRecord, false);

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(spannerMigrationTransformer.toSpannerRow(expectedRequest))
        .thenReturn(migrationTransformationResponse);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, transformationContext, "mysql", customTransformation);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"_metadata_schema\":\"db1\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\",\"shardId\":\"shard1\"}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformationFilterEvent()
      throws InvalidChangeEventException, TransformationException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put("last_name", "Depp");
    changeEvent.put("age", 13);
    changeEvent.put(DatastreamConstants.MYSQL_TIMESTAMP_KEY, 12345);
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    Map<String, Object> sourceRecord =
        ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);
    MigrationTransformationRequest expectedRequest =
        new MigrationTransformationRequest("Users", sourceRecord, "", "INSERT");
    Map<String, Object> spannerRecord = new HashMap<>(sourceRecord);
    spannerRecord.put("age", 10);
    MigrationTransformationResponse migrationTransformationResponse =
        new MigrationTransformationResponse(spannerRecord, true);

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(spannerMigrationTransformer.toSpannerRow(expectedRequest))
        .thenReturn(migrationTransformationResponse);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(schema, null, "mysql", customTransformation);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.FILTERED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\"}",
        argument.getValue());
  }

  // Test case for T exception

  // Test case for invalid CE exception
}
