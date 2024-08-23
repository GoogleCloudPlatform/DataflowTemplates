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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import com.google.cloud.teleport.v2.utils.CurrentTimestampGetter;
import com.google.cloud.teleport.v2.utils.SpannerFactory;
import com.google.cloud.teleport.v2.utils.SpannerFactory.DatabaseClientManager;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public final class SpannerScdMutationTypeDoFnTest {

  private CurrentTimestampGetter timestampMock;
  private SpannerConfig spannerConfigMock;
  private SpannerFactory spannerFactoryMock;
  private TransactionContext transactionContextMock;

  private ResultSet resultSetMock;

  @Before
  public void setUp() {
    timestampMock = mock(CurrentTimestampGetter.class);
    when(timestampMock.now()).thenReturn(Timestamp.ofTimeMicroseconds(777));
    when(timestampMock.nowPlusNanos(anyInt()))
        .thenAnswer(call -> Timestamp.ofTimeMicroseconds(777 + (Integer) call.getArgument(0)));

    spannerConfigMock = mock(SpannerConfig.class);
    when(spannerConfigMock.withExecuteStreamingSqlRetrySettings(any()))
        .thenReturn(spannerConfigMock);
    when(spannerConfigMock.getRpcPriority())
        .thenReturn(ValueProvider.StaticValueProvider.of(RpcPriority.HIGH));

    spannerFactoryMock = mock(SpannerFactory.class);
    DatabaseClientManager databaseClientManagerMock = mock(DatabaseClientManager.class);
    when(spannerFactoryMock.getDatabaseClientManager()).thenReturn(databaseClientManagerMock);
    when(spannerFactoryMock.getSpannerConfig()).thenReturn(spannerConfigMock);

    DatabaseClient databaseClientMock = mock(DatabaseClient.class);
    when(databaseClientManagerMock.getDatabaseClient()).thenReturn(databaseClientMock);

    TransactionRunner transactionCallableMock = mock(TransactionRunner.class);
    when(databaseClientMock.readWriteTransaction()).thenReturn(transactionCallableMock);
    when(databaseClientMock.readWriteTransaction(any())).thenReturn(transactionCallableMock);
    when(transactionCallableMock.allowNestedTransaction()).thenReturn(transactionCallableMock);

    transactionContextMock = mock(TransactionContext.class);
    when(transactionCallableMock.run(any()))
        .thenAnswer(
            invocation -> {
              TransactionRunner.TransactionCallable<Void> callable = invocation.getArgument(0);
              return callable.run(transactionContextMock);
            });

    resultSetMock = mock(ResultSet.class);
    when(transactionContextMock.read(any(), any(), any())).thenReturn(resultSetMock);
  }

  @Test
  public void testProcessElement_scdType1_createsInsertOrUpdateMutation() throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("Nito").build(),
            Struct.newBuilder().set("id").to(2).set("name").to("Beam").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_1)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setPrimaryKeyColumnNames(ImmutableList.of("id"))
            /* Not required for SCD TYPE_1. */
            .setStartDateColumnName(null)
            .setEndDateColumnName(null)
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(2)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            Mutation.newInsertOrUpdateBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .build(),
            Mutation.newInsertOrUpdateBuilder("tableName")
                .set("id")
                .to(2)
                .set("name")
                .to("Beam")
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withEndDate_withNoRowMatch_createsInserts()
      throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.FALSE); // No results.
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("Nito").build(),
            Struct.newBuilder().set("id").to(2).set("name").to("Beam").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName(null)
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(2)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(2)
                .set("name")
                .to("Beam")
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withStartAndEndDate_withNoRowMatch_createsInserts()
      throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.FALSE); // No results.
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("Nito").build(),
            Struct.newBuilder().set("id").to(2).set("name").to("Beam").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName("start_date")
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(2)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(2)
                .set("name")
                .to("Beam")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withEndDate_withRowMatch_createsUpdates()
      throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(resultSetMock.getCurrentRowAsStruct())
        .thenReturn(
            Struct.newBuilder()
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("NewName").build(),
            Struct.newBuilder().set("id").to(2).set("name").to("Beam").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName(null)
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(4)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            // Updates via delete and 2x insert.
            Mutation.delete(
                "tableName", Key.newBuilder().append(1).append(NullTypes.NULL_TIMESTAMP).build()),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("end_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            // Inserts non-existing row.
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(2)
                .set("name")
                .to("Beam")
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withStartAndEndDate_withRowMatch_createsUpdates()
      throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(resultSetMock.getCurrentRowAsStruct())
        .thenReturn(
            Struct.newBuilder()
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(123))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("NewName").build(),
            Struct.newBuilder().set("id").to(2).set("name").to("Beam").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName("start_date")
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(4)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            // Updates via delete and 2x insert.
            Mutation.delete(
                "tableName", Key.newBuilder().append(1).append(NullTypes.NULL_TIMESTAMP).build()),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(123))
                .set("end_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            // Inserts non-existing row.
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(2)
                .set("name")
                .to("Beam")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withNoRowMatch_handlesSequentialUpdates()
      throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.FALSE); // No results.
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("NewName").build(),
            Struct.newBuilder().set("id").to(1).set("name").to("OtherNewName").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName("start_date")
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(4)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            // Insert row as there are no matches.
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            // Updates row via delete and 2x inserts.
            Mutation.delete(
                "tableName", Key.newBuilder().append(1).append(NullTypes.NULL_TIMESTAMP).build()),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("OtherNewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }

  @Test
  public void testProcessElement_scdType2_withRowMatch_handlesSequentialUpdates() throws Exception {
    ArgumentCaptor<Mutation> mutationBufferCapture = ArgumentCaptor.forClass(Mutation.class);
    when(resultSetMock.next()).thenReturn(Boolean.TRUE, Boolean.FALSE);
    when(resultSetMock.getCurrentRowAsStruct())
        .thenReturn(
            Struct.newBuilder()
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(123))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
    Iterable<Struct> input =
        ImmutableList.of(
            Struct.newBuilder().set("id").to(1).set("name").to("NewName").build(),
            Struct.newBuilder().set("id").to(1).set("name").to("OtherNewName").build());

    SpannerScdMutationDoFn spannerScdMutationTransform =
        SpannerScdMutationDoFn.builder()
            .setScdType(ScdType.TYPE_2)
            .setSpannerConfig(spannerConfigMock)
            .setTableName("tableName")
            .setPrimaryKeyColumnNames(ImmutableList.of("id", "end_date"))
            .setStartDateColumnName("start_date")
            .setEndDateColumnName("end_date")
            .setTableColumnNames(ImmutableList.of("id", "name"))
            .setSpannerFactory(spannerFactoryMock)
            .setCurrentTimestampGetter(timestampMock)
            .build();
    spannerScdMutationTransform.setup();
    spannerScdMutationTransform.writeBatchChanges(input);
    spannerScdMutationTransform.teardown();

    verify(transactionContextMock, times(6)).buffer(mutationBufferCapture.capture());
    List<Mutation> outputMutations = mutationBufferCapture.getAllValues();
    assertThat(outputMutations)
        .containsExactly(
            // Updates via delete and 2x inserts.
            Mutation.delete(
                "tableName", Key.newBuilder().append(1).append(NullTypes.NULL_TIMESTAMP).build()),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("Nito")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(123))
                .set("end_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build(),
            // Updates again via delete and 2x inserts.
            Mutation.delete(
                "tableName", Key.newBuilder().append(1).append(NullTypes.NULL_TIMESTAMP).build()),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("NewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(777))
                .set("end_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .build(),
            Mutation.newInsertBuilder("tableName")
                .set("id")
                .to(1)
                .set("name")
                .to("OtherNewName")
                .set("start_date")
                .to(Timestamp.ofTimeMicroseconds(778))
                .set("end_date")
                .to(NullTypes.NULL_TIMESTAMP)
                .build());
  }
}
