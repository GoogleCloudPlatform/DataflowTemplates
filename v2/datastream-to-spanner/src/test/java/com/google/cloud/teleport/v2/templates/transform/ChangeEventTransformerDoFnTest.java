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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.ShardingContext;
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
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ChangeEventTransformerDoFnTest {
  @Test
  public void testProcessElementWithNonEmptySchema() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);

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
    when(changeEventSessionConvertor.transformChangeEventViaSessionFile(changeEvent))
        .thenReturn(changeEvent2);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent2, databaseClientMock, null))
        .thenReturn(changeEvent2);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(processContextMock.sideInput(ddl)).thenReturn(null);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"synth_id\":123}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformation() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
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
    when(spannerMigrationTransformer.toSpannerRow(refEq(expectedRequest)))
        .thenReturn(migrationTransformationResponse);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);
    when(changeEventSessionConvertor.getShardId(changeEvent)).thenReturn("");

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":10,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\"}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformationAndTransformationContext()
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

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
    when(spannerMigrationTransformer.toSpannerRow(refEq(expectedRequest)))
        .thenReturn(migrationTransformationResponse);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);
    when(changeEventSessionConvertor.getShardId(changeEvent)).thenReturn("shard1");

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema,
            null,
            transformationContext,
            null,
            "mysql",
            customTransformation,
            false,
            ddl,
            spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"_metadata_schema\":\"db1\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\",\"shardId\":\"shard1\"}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformationAndShardingContext() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

    Map<String, Map<String, String>> streamToDbAndShardMap = new HashMap<>();
    Map<String, String> schemaToShardId = new HashMap<>();
    schemaToShardId.put("db1", "shard1");
    streamToDbAndShardMap.put("stream1", schemaToShardId);
    ShardingContext shardingContext = new ShardingContext(streamToDbAndShardMap);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_STREAM_NAME, "stream1");
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
    when(spannerMigrationTransformer.toSpannerRow(refEq(expectedRequest)))
        .thenReturn(migrationTransformationResponse);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);
    when(changeEventSessionConvertor.getShardId(changeEvent)).thenReturn("shard1");

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema,
            null,
            null,
            shardingContext,
            "mysql",
            customTransformation,
            false,
            ddl,
            spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_stream\":\"stream1\",\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"_metadata_schema\":\"db1\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\",\"shardId\":\"shard1\"}",
        argument.getValue().getPayload());
  }

  @Test
  public void testProcessElementWithCustomTransformationFilterEvent() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
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
    when(spannerMigrationTransformer.toSpannerRow(refEq(expectedRequest)))
        .thenReturn(migrationTransformationResponse);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);
    when(changeEventSessionConvertor.getShardId(changeEvent)).thenReturn("");

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.FILTERED_EVENT_TAG), argument.capture());
    assertEquals(
        "{\"_metadata_source_type\":\"mysql\",\"_metadata_table\":\"Users\",\"first_name\":\"Johnny\",\"last_name\":\"Depp\",\"age\":13,\"_metadata_timestamp\":12345,\"_metadata_change_type\":\"INSERT\"}",
        argument.getValue());
  }

  @Test
  public void testProcessElementWithInvalidTransformationException() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    Map<String, Object> sourceRecord =
        ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);
    MigrationTransformationRequest expectedRequest =
        new MigrationTransformationRequest("Users", sourceRecord, "", "INSERT");

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);
    doThrow(new InvalidTransformationException("invalid transformation"))
        .when(spannerMigrationTransformer)
        .toSpannerRow(refEq(expectedRequest));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);
    when(changeEventSessionConvertor.getShardId(changeEvent)).thenReturn("");

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement<String, String>> argument =
        ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertEquals(
        "com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException: invalid transformation",
        argument.getValue().getErrorMessage());
  }

  @Test
  public void testProcessElementWithDroppedTableException() throws DroppedTableException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    when(schema.isEmpty()).thenReturn(false);
    doThrow(new DroppedTableException("Cannot find entry for Users"))
        .when(schema)
        .verifyTableInSession("Users");
    when(processContextMock.element()).thenReturn(failsafeElement);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.processElement(processContextMock);
    verify(processContextMock, times(0)).output(any());
  }

  @Test
  public void testProcessElementWithIllegalArgumentException() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

    // Create failsafe element input for the DoFn
    ObjectNode changeEvent = mapper.createObjectNode();
    changeEvent.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    changeEvent.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    changeEvent.put("first_name", "Johnny");
    changeEvent.put(EVENT_CHANGE_TYPE_KEY, "INSERT");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(changeEvent.toString(), changeEvent.toString());

    when(schema.isEmpty()).thenReturn(false);
    doThrow(
            new IllegalArgumentException(
                "Missing entry for Users in srcToId map, provide a valid session file."))
        .when(schema)
        .verifyTableInSession("Users");
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            changeEvent, databaseClientMock, null))
        .thenReturn(changeEvent);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.processElement(processContextMock);
    ArgumentCaptor<FailsafeElement<String, String>> argument =
        ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertEquals(
        "Missing entry for Users in srcToId map, provide a valid session file.",
        argument.getValue().getErrorMessage());
  }

  @Test
  public void testProcessElementWithException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);

    String invalidJson = "{\"column\": {\"nestedColumn\": {\"invalidDatatype\":}}}";
    FailsafeElement<String, String> failsafeElement = FailsafeElement.of(invalidJson, invalidJson);

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setMapper(mapper);
    changeEventTransformerDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement<String, String>> argument =
        ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertNotNull(argument.getValue().getErrorMessage());
  }

  @Test
  public void testProcessElementWithInvalidChangeEventException() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    Schema schema = mock(Schema.class);
    CustomTransformation customTransformation = mock(CustomTransformation.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    ISpannerMigrationTransformer spannerMigrationTransformer =
        mock(ISpannerMigrationTransformer.class);
    PCollectionView<Ddl> ddl = mock(PCollectionView.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ChangeEventSessionConvertor changeEventSessionConvertor =
        mock(ChangeEventSessionConvertor.class);

    ObjectNode invalidArrayNode = mapper.createObjectNode();
    ArrayNode invalidArray = invalidArrayNode.putArray("invalidKey");
    invalidArray.add("not a number");
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(invalidArrayNode.toString(), invalidArrayNode.toString());

    when(schema.isEmpty()).thenReturn(true);
    when(processContextMock.element()).thenReturn(failsafeElement);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(changeEventSessionConvertor.transformChangeEventData(
            invalidArrayNode, databaseClientMock, null))
        .thenReturn(invalidArrayNode);

    ChangeEventTransformerDoFn changeEventTransformerDoFn =
        ChangeEventTransformerDoFn.create(
            schema, null, null, null, "mysql", customTransformation, false, ddl, spannerConfig);
    changeEventTransformerDoFn.setDatastreamToSpannerTransformer(spannerMigrationTransformer);
    changeEventTransformerDoFn.setSpannerAccessor(spannerAccessor);
    changeEventTransformerDoFn.setChangeEventSessionConvertor(changeEventSessionConvertor);
    changeEventTransformerDoFn.setMapper(mapper);

    changeEventTransformerDoFn.processElement(processContextMock);
    ArgumentCaptor<FailsafeElement<String, String>> argument =
        ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertEquals(
        "Invalid byte array value for column: invalidKey", argument.getValue().getErrorMessage());
  }
}
