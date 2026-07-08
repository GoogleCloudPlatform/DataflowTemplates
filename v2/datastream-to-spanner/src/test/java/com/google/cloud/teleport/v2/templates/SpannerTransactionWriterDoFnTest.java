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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.templates.source.mysql.MySqlDsToSpSourceConnector;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/** Unit tests for SpannerTransactionWriterDoFn class. */
public class SpannerTransactionWriterDoFnTest {

  @Test
  public void testGetTxnTag() {
    String[] args = new String[] {"--jobId=123"};
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, null, null, "", "mysql", true);
    String result = spannerTransactionWriterDoFn.getTxnTag(options);
    assertEquals(result, "txBy=123");
  }

  Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("age")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .createTable("shadow_Users")
            .column("first_name")
            .string()
            .max()
            .endColumn()
            .column("last_name")
            .string()
            .size(5)
            .endColumn()
            .column("version")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("first_name")
            .desc("last_name")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  @Test
  public void testProcessElement() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(1)).buffer(argument.capture());
    Iterable<Mutation> capturedMutations = argument.getValue();
    Iterator<Mutation> mutationIterator = capturedMutations.iterator();
    Mutation actualDataMutation = null;
    Mutation actualShadowTableMutation = null;

    if (mutationIterator.hasNext()) {
      // Get the first mutation
      actualDataMutation = mutationIterator.next();

      if (mutationIterator.hasNext()) {
        // Get the second mutation
        actualShadowTableMutation = mutationIterator.next();
      }
    }

    Mutation.WriteBuilder dataBuilder = Mutation.newInsertOrUpdateBuilder("Users");
    dataBuilder.set("first_name").to("Johnny");
    dataBuilder.set("last_name").to("Depp");
    dataBuilder.set("age").to(13);
    Mutation expectedDataMutation = dataBuilder.build();
    assertEquals(actualDataMutation, expectedDataMutation);

    Mutation.WriteBuilder shadowBuilder = Mutation.newInsertOrUpdateBuilder("shadow_Users");
    shadowBuilder.set("first_name").to("Johnny");
    shadowBuilder.set("last_name").to("Depp");
    shadowBuilder.set("timestamp").to(12345);
    shadowBuilder.set("log_file").to("");
    shadowBuilder.set("log_position").to(-1);
    Mutation expectedShadowMutation = shadowBuilder.build();
    assertEquals(actualShadowTableMutation, expectedShadowMutation);

    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementWithShardIdAndRetry() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    outputObject.put("_metadata_retry_count", 1);
    outputObject.put("_metadata_shard_id_column_name", "shard_id");
    outputObject.put("shard_id", "shard1");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(transactionContext.readRow(any(String.class), any(), anyList())).thenReturn(null);

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementWithDroppedTable() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "NonExistentTable");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);

    // Verify that it does NOT write to Spanner
    verify(transactionContext, never()).buffer(any(Iterable.class));
    // Verify that it does NOT output to success tag (since it's dropped)
    verify(processContextMock, never()).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementWithInvalidSourceType() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "invalid_source");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    outputObject.put("_metadata_shard_id_column_name", "shard_id");
    outputObject.put("shard_id", "shard1");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "invalid_source", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    // Verify that it outputs to dead letter queue (permanent error tag)
    verify(processContextMock, times(1))
        .output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementWithMissingPrimaryKey() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "mysql");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    // Missing first_name and last_name which are PKs
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    outputObject.put("_metadata_shard_id_column_name", "shard_id");
    outputObject.put("shard_id", "shard1");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    // Verify that it outputs to dead letter queue (permanent error tag)
    verify(processContextMock, times(1))
        .output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementCrossDatabase() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowTableSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    PCollectionView<Ddl> shadowTableDdlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    DatabaseClient shadowDatabaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);
    ValueProvider<String> shadowInstanceId = mock(ValueProvider.class);
    ValueProvider<String> shadowDatabaseId = mock(ValueProvider.class);
    com.google.cloud.spanner.ResultSet resultSet = mock(com.google.cloud.spanner.ResultSet.class);
    com.google.cloud.spanner.Struct struct = mock(com.google.cloud.spanner.Struct.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 200L);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(processContextMock.sideInput(eq(shadowTableDdlView))).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");

    when(shadowTableSpannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(shadowTableSpannerConfig.getInstanceId()).thenReturn(shadowInstanceId);
    when(shadowTableSpannerConfig.getDatabaseId()).thenReturn(shadowDatabaseId);
    when(shadowInstanceId.get()).thenReturn("shadow-instance");
    when(shadowDatabaseId.get()).thenReturn("shadow-database");

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);

    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);

    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(shadowTransactionContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false); // For two calls
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getLong(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("timestamp")) {
                return 10000L;
              }
              if (col.contains("log_position")) {
                return 100L;
              }
              return 0L;
            });
    when(struct.getString(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("log_file")) {
                return "mysql-bin.000001";
              }
              return "";
            });

    com.google.cloud.spanner.ResultSet mainResultSet =
        mock(com.google.cloud.spanner.ResultSet.class);
    when(transactionContext.executeQuery(any(Statement.class))).thenReturn(mainResultSet);
    when(mainResultSet.next()).thenReturn(false);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig,
            shadowTableSpannerConfig,
            ddlView,
            shadowTableDdlView,
            "shadow",
            "mysql",
            true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(shadowTransactionContext, times(1)).buffer(any(Mutation.class));
    verify(transactionContext, times(1)).buffer(any(Mutation.class));
    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementSingleDatabase_Skip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    com.google.cloud.spanner.Struct struct = mock(com.google.cloud.spanner.Struct.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 10000L); // older than shadow
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 100L);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(transactionContext.readRow(any(), any(), any())).thenReturn(struct);
    when(struct.getLong(0)).thenReturn(12345L); // newer timestamp
    when(struct.getString(1)).thenReturn("mysql-bin.000001");
    when(struct.getLong(2)).thenReturn(200L); // larger position

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig,
            spannerConfig,
            ddlView,
            ddlView,
            "shadow",
            "mysql",
            false); // false for single DB
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(transactionContext, times(0)).buffer(any(Mutation.class));
    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementCrossDatabase_Skip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    com.google.cloud.spanner.Struct struct = mock(com.google.cloud.spanner.Struct.class);

    SpannerConfig shadowTableSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient shadowDatabaseClientMock = mock(DatabaseClient.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    PCollectionView<Ddl> shadowTableDdlView = mock(PCollectionView.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 10000L); // older than shadow
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 100L);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();
    Ddl shadowDdl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(processContextMock.sideInput(eq(shadowTableDdlView))).thenReturn(shadowDdl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);
    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);

    com.google.cloud.spanner.ResultSet resultSet = mock(com.google.cloud.spanner.ResultSet.class);
    when(shadowTransactionContext.executeQuery(any(Statement.class))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getLong(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("timestamp")) {
                return 20000L;
              }
              if (col.contains("log_position")) {
                return 200L;
              }
              return 0L;
            });
    when(struct.getString(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("log_file")) {
                return "mysql-bin.000001";
              }
              return "";
            });

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig,
            shadowTableSpannerConfig,
            ddlView,
            shadowTableDdlView,
            "shadow",
            "mysql",
            true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(shadowTransactionContext, times(0)).buffer(any(Mutation.class));
    verify(databaseClientMock, times(0)).readWriteTransaction(any(), any(), any());
    verify(processContextMock, times(1)).output(any(com.google.cloud.Timestamp.class));
  }

  @Test
  public void testProcessElementCrossDatabase_ValidationFailure() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);

    SpannerConfig shadowTableSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);
    DatabaseClient shadowDatabaseClientMock = mock(DatabaseClient.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    PCollectionView<Ddl> shadowTableDdlView = mock(PCollectionView.class);

    com.google.cloud.spanner.Struct struct1 = mock(com.google.cloud.spanner.Struct.class);
    com.google.cloud.spanner.Struct struct2 = mock(com.google.cloud.spanner.Struct.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 15000L);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 150L);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();
    Ddl shadowDdl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);
    when(processContextMock.sideInput(eq(shadowTableDdlView))).thenReturn(shadowDdl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of("test-database"));

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);
    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);

    com.google.cloud.spanner.ResultSet resultSet1 = mock(com.google.cloud.spanner.ResultSet.class);
    com.google.cloud.spanner.ResultSet resultSet2 = mock(com.google.cloud.spanner.ResultSet.class);
    final int[] callCount = new int[] {0};
    org.mockito.stubbing.Answer<com.google.cloud.spanner.ResultSet> answer =
        invocation -> {
          callCount[0]++;
          if (callCount[0] == 1) {
            return resultSet1;
          }
          if (callCount[0] == 2) {
            return resultSet2;
          }
          return null;
        };
    org.mockito.Mockito.doAnswer(answer).when(shadowTransactionContext).executeQuery(any());
    when(resultSet1.next()).thenReturn(true).thenReturn(false);
    when(resultSet1.getCurrentRowAsStruct()).thenReturn(struct1);
    when(resultSet2.next()).thenReturn(true).thenReturn(false);
    when(resultSet2.getCurrentRowAsStruct()).thenReturn(struct2);

    com.google.cloud.spanner.ResultSet mockMainResultSet =
        mock(com.google.cloud.spanner.ResultSet.class);
    when(transactionContext.executeQuery(any(com.google.cloud.spanner.Statement.class)))
        .thenReturn(mockMainResultSet);
    when(mockMainResultSet.next()).thenReturn(false);

    when(struct1.getLong(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("timestamp")) {
                return 10000L;
              }
              if (col.contains("log_position")) {
                return 100L;
              }
              return 0L;
            });
    when(struct1.getString(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("log_file")) {
                return "mysql-bin.000001";
              }
              return "";
            });

    when(struct2.getLong(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("timestamp")) {
                return 20000L;
              }
              if (col.contains("log_position")) {
                return 200L;
              }
              return 0L;
            });
    when(struct2.getString(any(String.class)))
        .thenAnswer(
            invocation -> {
              String col = invocation.getArgument(0);
              if (col.contains("log_file")) {
                return "mysql-bin.000001";
              }
              return "";
            });

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig,
            shadowTableSpannerConfig,
            ddlView,
            shadowTableDdlView,
            "shadow",
            "mysql",
            true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(databaseClientMock, times(1)).readWriteTransaction(any(), any(), any());
    verify(processContextMock, times(0)).output(any(com.google.cloud.Timestamp.class));
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), any());
  }

  @Test
  public void testProcessElementWithInvalidChangeEvent() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);

    String instanceId = "test-instance";
    String databaseId = "test-database";
    when(spannerConfig.getInstanceId())
        .thenReturn(ValueProvider.StaticValueProvider.of(instanceId));
    when(spannerConfig.getDatabaseId())
        .thenReturn(ValueProvider.StaticValueProvider.of(databaseId));

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "random");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users1");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 123);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(schema.isEmpty()).thenReturn(true);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.processElement(processContextMock);

    ArgumentCaptor<FailsafeElement> argument = ArgumentCaptor.forClass(FailsafeElement.class);
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), argument.capture());
    assertEquals(
        "Change event with invalid source. Actual(random), Expected(mysql)",
        argument.getValue().getErrorMessage());
  }

  @Test
  public void testProcessElementWithRetryableSpannerException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.ABORTED, "Transaction Aborted");
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(0)).buffer(anyList());

    verify(processContextMock, times(1))
        .output(eq(RETRYABLE_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementWithNonRetryableSpannerException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    Schema schema = mock(Schema.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(schema.isEmpty()).thenReturn(true);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              throw SpannerExceptionFactory.newSpannerException(
                  ErrorCode.FAILED_PRECONDITION, "title must not be NULL in table Books");
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));
    spannerTransactionWriterDoFn.processElement(processContextMock);
    ArgumentCaptor<Iterable<Mutation>> argument = ArgumentCaptor.forClass(Iterable.class);
    verify(transactionContext, times(0)).buffer(anyList());

    verify(processContextMock, times(1))
        .output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementWithIllegalStateException() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> instanceId = mock(ValueProvider.class);
    ValueProvider<String> databaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    when(instanceId.get()).thenReturn("test-instance");
    when(databaseId.get()).thenReturn("test-database");
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);

    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenThrow(new IllegalStateException("Spanner pool closed"));

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, spannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(processContextMock, times(1))
        .output(eq(RETRYABLE_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessCrossDatabaseTransaction() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    DatabaseClient shadowDatabaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> mainInstanceId = mock(ValueProvider.class);
    ValueProvider<String> mainDatabaseId = mock(ValueProvider.class);
    ValueProvider<String> shadowInstanceId = mock(ValueProvider.class);
    ValueProvider<String> shadowDatabaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);

    when(spannerConfig.getInstanceId()).thenReturn(mainInstanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(mainDatabaseId);
    when(mainInstanceId.get()).thenReturn("main-instance");
    when(mainDatabaseId.get()).thenReturn("main-database");

    when(shadowSpannerConfig.getInstanceId()).thenReturn(shadowInstanceId);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(shadowDatabaseId);
    when(shadowInstanceId.get()).thenReturn("shadow-instance");
    when(shadowDatabaseId.get()).thenReturn("shadow-database");

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);

    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);

    com.google.cloud.spanner.ResultSet resultSet = mock(com.google.cloud.spanner.ResultSet.class);
    when(resultSet.next()).thenReturn(false);
    when(shadowTransactionContext.executeQuery(any(Statement.class))).thenReturn(resultSet);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, shadowSpannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(shadowDatabaseClientMock, times(1)).readWriteTransaction(any(), any(), any());
    verify(databaseClientMock, times(1)).readWriteTransaction(any(), any(), any());
  }

  @Test
  public void testProcessCrossDatabaseTransaction_SequenceMismatch() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);
    PCollectionView<Ddl> ddlView = mock(PCollectionView.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    DatabaseClient shadowDatabaseClientMock = mock(DatabaseClient.class);
    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    TransactionContext transactionContext = mock(TransactionContext.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    ValueProvider<Options.RpcPriority> rpcPriorityValueProviderMock = mock(ValueProvider.class);
    ValueProvider<String> mainInstanceId = mock(ValueProvider.class);
    ValueProvider<String> mainDatabaseId = mock(ValueProvider.class);
    ValueProvider<String> shadowInstanceId = mock(ValueProvider.class);
    ValueProvider<String> shadowDatabaseId = mock(ValueProvider.class);

    String[] args = new String[] {"--jobId=123"};
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowWorkerHarnessOptions.class);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Depp");
    outputObject.put("age", 13);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 10000L);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 100L);
    outputObject.put("_metadata_timestamp", 12345L);
    outputObject.put("_metadata_read_timestamp", 12346L);
    outputObject.put("_metadata_dataflow_timestamp", 12347L);
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    Ddl ddl = getTestDdl();

    when(processContextMock.element()).thenReturn(KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(any())).thenReturn(ddl);
    when(processContextMock.getPipelineOptions()).thenReturn(options);
    when(rpcPriorityValueProviderMock.get()).thenReturn(Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);

    when(spannerConfig.getInstanceId()).thenReturn(mainInstanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(mainDatabaseId);
    when(mainInstanceId.get()).thenReturn("main-instance");
    when(mainDatabaseId.get()).thenReturn("main-database");

    when(shadowSpannerConfig.getInstanceId()).thenReturn(shadowInstanceId);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(shadowDatabaseId);
    when(shadowInstanceId.get()).thenReturn("shadow-instance");
    when(shadowDatabaseId.get()).thenReturn("shadow-database");

    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);

    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContext);
            });
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionCallableMock);

    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);

    com.google.cloud.spanner.ResultSet resultSet1 = mock(com.google.cloud.spanner.ResultSet.class);
    com.google.cloud.spanner.ResultSet resultSet2 = mock(com.google.cloud.spanner.ResultSet.class);

    when(resultSet1.next()).thenReturn(true).thenReturn(false);
    com.google.cloud.spanner.Struct struct1 = mock(com.google.cloud.spanner.Struct.class);
    when(resultSet1.getCurrentRowAsStruct()).thenReturn(struct1);
    when(struct1.getLong(0)).thenReturn(5000L);
    when(struct1.getString(1)).thenReturn("mysql-bin.000001");
    when(struct1.getLong(2)).thenReturn(50L);

    when(resultSet2.next()).thenReturn(true).thenReturn(false);
    com.google.cloud.spanner.Struct struct2 = mock(com.google.cloud.spanner.Struct.class);
    when(resultSet2.getCurrentRowAsStruct()).thenReturn(struct2);
    when(struct2.getLong(0)).thenReturn(20000L); // newer! sequence mismatch
    when(struct2.getString(1)).thenReturn("mysql-bin.000001");
    when(struct2.getLong(2)).thenReturn(200L);

    org.mockito.Mockito.doReturn(resultSet1)
        .doReturn(resultSet2)
        .when(shadowTransactionContext)
        .executeQuery(any());

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, shadowSpannerConfig, ddlView, ddlView, "shadow", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    // It should throw an exception and be caught by the catch block, and output to permanent error
    // tag
    verify(processContextMock, times(1))
        .output(
            eq(
                com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants
                    .PERMANENT_ERROR_TAG),
            any(FailsafeElement.class));
  }

  @Test
  public void testProcessCrossDatabaseTransaction_WithDataDml() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    SpannerAccessor spannerAccessor = mock(SpannerAccessor.class);
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    org.apache.beam.sdk.values.PCollectionView<com.google.cloud.teleport.v2.spanner.ddl.Ddl>
        ddlView = mock(org.apache.beam.sdk.values.PCollectionView.class);

    TransactionContext mainTransactionContext = mock(TransactionContext.class);
    TransactionContext shadowTransactionContext = mock(TransactionContext.class);
    TransactionRunner transactionRunnerMock = mock(TransactionRunner.class);
    TransactionRunner shadowTransactionCallableMock = mock(TransactionRunner.class);
    com.google.cloud.spanner.DatabaseClient shadowDatabaseClientMock =
        mock(com.google.cloud.spanner.DatabaseClient.class);
    SpannerAccessor shadowTableSpannerAccessor = mock(SpannerAccessor.class);

    org.apache.beam.sdk.options.ValueProvider<String> instanceId =
        mock(org.apache.beam.sdk.options.ValueProvider.class);
    org.apache.beam.sdk.options.ValueProvider<String> databaseId =
        mock(org.apache.beam.sdk.options.ValueProvider.class);
    when(instanceId.get()).thenReturn("main-instance");
    when(databaseId.get()).thenReturn("main-database");
    when(spannerConfig.getInstanceId()).thenReturn(instanceId);
    when(spannerConfig.getDatabaseId()).thenReturn(databaseId);
    org.apache.beam.sdk.options.ValueProvider<com.google.cloud.spanner.Options.RpcPriority>
        rpcPriorityValueProviderMock = mock(org.apache.beam.sdk.options.ValueProvider.class);
    when(rpcPriorityValueProviderMock.get())
        .thenReturn(com.google.cloud.spanner.Options.RpcPriority.LOW);
    when(spannerConfig.getRpcPriority()).thenReturn(rpcPriorityValueProviderMock);

    org.apache.beam.sdk.options.ValueProvider<String> shadowInstanceId =
        mock(org.apache.beam.sdk.options.ValueProvider.class);
    org.apache.beam.sdk.options.ValueProvider<String> shadowDatabaseId =
        mock(org.apache.beam.sdk.options.ValueProvider.class);
    when(shadowInstanceId.get()).thenReturn("shadow-instance");
    when(shadowDatabaseId.get()).thenReturn("shadow-database");
    when(shadowSpannerConfig.getInstanceId()).thenReturn(shadowInstanceId);
    when(shadowSpannerConfig.getDatabaseId()).thenReturn(shadowDatabaseId);

    com.google.cloud.spanner.DatabaseClient databaseClientMock =
        mock(com.google.cloud.spanner.DatabaseClient.class);
    when(spannerAccessor.getDatabaseClient()).thenReturn(databaseClientMock);
    when(databaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(transactionRunnerMock);
    when(transactionRunnerMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable callable = invocation.getArgument(0);
              return callable.run(mainTransactionContext);
            });

    when(shadowTableSpannerAccessor.getDatabaseClient()).thenReturn(shadowDatabaseClientMock);
    when(shadowDatabaseClientMock.readWriteTransaction(any(), any(), any()))
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.allowNestedTransaction())
        .thenReturn(shadowTransactionCallableMock);
    when(shadowTransactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable callable = invocation.getArgument(0);
              return callable.run(shadowTransactionContext);
            });

    com.google.cloud.teleport.v2.spanner.ddl.Ddl ddl =
        com.google.cloud.teleport.v2.spanner.ddl.Ddl.builder()
            .createTable("userswithgenpk")
            .column("id")
            .string()
            .max()
            .endColumn()
            .column("name")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("shadow_userswithgenpk")
            .column("id")
            .string()
            .max()
            .endColumn()
            .column("log_file")
            .string()
            .max()
            .endColumn()
            .column("log_position")
            .int64()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    com.fasterxml.jackson.databind.node.ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, Constants.MYSQL_SOURCE_TYPE);
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "userswithgenpk");
    outputObject.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "INSERT");
    outputObject.put("id", "123");
    outputObject.put("name", "Test");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_TIMESTAMP_KEY, 12345);
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGFILE_KEY, "mysql-bin.000001");
    outputObject.put(MySqlDsToSpSourceConnector.MYSQL_LOGPOSITION_KEY, 100);

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContextMock.element())
        .thenReturn(org.apache.beam.sdk.values.KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);

    org.apache.beam.sdk.options.PipelineOptions optionsMock =
        mock(org.apache.beam.sdk.options.PipelineOptions.class);
    org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions harnessOptionsMock =
        mock(org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions.class);
    when(optionsMock.as(
            org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions.class))
        .thenReturn(harnessOptionsMock);
    when(harnessOptionsMock.getJobId()).thenReturn("123");
    when(processContextMock.getPipelineOptions()).thenReturn(optionsMock);

    com.google.cloud.spanner.ResultSet resultSet = mock(com.google.cloud.spanner.ResultSet.class);
    when(resultSet.next()).thenReturn(true).thenReturn(false);
    com.google.cloud.spanner.Struct struct = mock(com.google.cloud.spanner.Struct.class);
    when(resultSet.getCurrentRowAsStruct()).thenReturn(struct);
    when(struct.getLong(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(5000L)
        .thenReturn(50L);
    when(struct.getString(org.mockito.ArgumentMatchers.anyString())).thenReturn("mysql-bin.000001");

    org.mockito.Mockito.doReturn(resultSet).when(shadowTransactionContext).executeQuery(any());

    com.google.cloud.spanner.ResultSet mockMainResultSet =
        mock(com.google.cloud.spanner.ResultSet.class);
    when(mainTransactionContext.executeQuery(any(com.google.cloud.spanner.Statement.class)))
        .thenReturn(mockMainResultSet);
    when(mockMainResultSet.next()).thenReturn(false);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, shadowSpannerConfig, ddlView, ddlView, "shadow_", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setSpannerAccessor(spannerAccessor);
    spannerTransactionWriterDoFn.setShadowTableSpannerAccessor(shadowTableSpannerAccessor);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    verify(mainTransactionContext, times(0))
        .executeUpdate(any(com.google.cloud.spanner.Statement.class));
    verify(mainTransactionContext, times(1)).buffer(any(com.google.cloud.spanner.Mutation.class));
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), any());
  }

  @Test
  public void testProcessElementWithDroppedTableException() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    org.apache.beam.sdk.values.PCollectionView<com.google.cloud.teleport.v2.spanner.ddl.Ddl>
        ddlView = mock(org.apache.beam.sdk.values.PCollectionView.class);
    when(spannerConfig.getInstanceId())
        .thenReturn(
            org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(
            org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.of("test-database"));

    com.google.cloud.teleport.v2.spanner.ddl.Ddl ddl =
        com.google.cloud.teleport.v2.spanner.ddl.Ddl.builder().build(); // Empty DDL

    com.fasterxml.jackson.databind.node.ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "mysql");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "NonExistentTable");
    outputObject.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "INSERT");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContextMock.element())
        .thenReturn(org.apache.beam.sdk.values.KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, shadowSpannerConfig, ddlView, ddlView, "shadow_", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    // Verify that no error was output to DLQ (since it's ignored)
    verify(processContextMock, never())
        .output(any(org.apache.beam.sdk.values.TupleTag.class), any());
  }

  @Test
  public void testProcessElementWithChangeEventConvertorException() throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    SpannerConfig spannerConfig = mock(SpannerConfig.class);
    SpannerConfig shadowSpannerConfig = mock(SpannerConfig.class);
    DoFn.ProcessContext processContextMock = mock(DoFn.ProcessContext.class);
    org.apache.beam.sdk.values.PCollectionView<com.google.cloud.teleport.v2.spanner.ddl.Ddl>
        ddlView = mock(org.apache.beam.sdk.values.PCollectionView.class);

    when(spannerConfig.getInstanceId())
        .thenReturn(
            org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.of("test-instance"));
    when(spannerConfig.getDatabaseId())
        .thenReturn(
            org.apache.beam.sdk.options.ValueProvider.StaticValueProvider.of("test-database"));

    com.google.cloud.teleport.v2.spanner.ddl.Ddl ddl =
        com.google.cloud.teleport.v2.spanner.ddl.Ddl.builder()
            .createTable("Users")
            .column("id")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    com.fasterxml.jackson.databind.node.ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_SOURCE_TYPE_KEY, "mysql");
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put(DatastreamConstants.EVENT_CHANGE_TYPE_KEY, "INSERT");
    // Missing "id" column which is required by DDL as primary key

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContextMock.element())
        .thenReturn(org.apache.beam.sdk.values.KV.of(1L, failsafeElement));
    when(processContextMock.sideInput(eq(ddlView))).thenReturn(ddl);

    SpannerTransactionWriterDoFn spannerTransactionWriterDoFn =
        new SpannerTransactionWriterDoFn(
            spannerConfig, shadowSpannerConfig, ddlView, ddlView, "shadow_", "mysql", true);
    spannerTransactionWriterDoFn.setMapper(mapper);
    spannerTransactionWriterDoFn.setIsInTransaction(new AtomicBoolean(false));
    spannerTransactionWriterDoFn.setTransactionAttemptCount(new AtomicLong(0));

    spannerTransactionWriterDoFn.processElement(processContextMock);

    // Verify that error was output to DLQ (permanent error tag)
    verify(processContextMock, times(1))
        .output(eq(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }
}
