/*
 * Copyright (C) 2021 Google LLC
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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.transforms.DeleteBigQueryDataFn.Options;
import com.google.cloud.teleport.v2.utils.SerializableSchemaSupplier;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DeleteBigQueryDataFn}. */
@RunWith(JUnit4.class)
public class DeleteBigQueryDataFnTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public final TestPipeline testPipeline = TestPipeline.create();
  @Mock private static BigQuery bqMock;

  private BigQueryTable table;
  private BigQueryTable partitionedTable;
  private BigQueryTablePartition partition;
  private DeleteBigQueryDataFn fnUnderTest;
  private Coder<KV<BigQueryTable, BigQueryTablePartition>> fnCoder;

  @Before
  public void setUp() throws CannotProvideCoderException {
    partition =
        BigQueryTablePartition.builder()
            .setLastModificationTime(System.currentTimeMillis() * 1000)
            .setPartitionName("p1")
            .build();

    table =
        BigQueryTable.builder()
            .setTableName("t1")
            .setProject("pr1")
            .setDataset("d1")
            .setLastModificationTime(123L)
            .setSchemaSupplier(SerializableSchemaSupplier.of(Schema.create(Schema.Type.STRING)))
            .build();

    partitionedTable =
        table.toBuilder()
            .setTableName("t1p")
            .setPartitions(Collections.singletonList(partition))
            .setPartitioningColumn("column-name-doesnt-matter")
            .build();

    fnUnderTest = new DeleteBigQueryDataFn().withTestBqClientFactory(() -> bqMock);

    CoderRegistry cr = testPipeline.getCoderRegistry();
    fnCoder =
        KvCoder.of(cr.getCoder(BigQueryTable.class), cr.getCoder(BigQueryTablePartition.class));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataEnabled_truncatesData()
      throws InterruptedException {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    options.setDeleteSourceData(true);

    PCollection<Void> actual =
        testPipeline
            .apply(
                "CreateInput",
                Create.of(
                        KV.of(partitionedTable, partition),
                        KV.of(table, (BigQueryTablePartition) null))
                    .withCoder(fnCoder))
            .apply("TestDeleteBigQueryDataFn", ParDo.of(fnUnderTest));
    PAssert.that(actual).empty();
    testPipeline.run(options);

    verify(bqMock, times(1))
        .query(QueryJobConfiguration.newBuilder("truncate table `pr1.d1.t1`").build());
    verify(bqMock, times(1)).delete(TableId.of("pr1", "d1", "t1p$p1"));
    verifyNoMoreInteractions(bqMock);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataDisabled_doesntTruncateData() {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    options.setDeleteSourceData(false);

    BigQueryTable partitionedTable =
        table.toBuilder()
            .setPartitions(Collections.singletonList(partition))
            .setPartitioningColumn("column-name-doesnt-matter")
            .build();

    DeleteBigQueryDataFn fn = new DeleteBigQueryDataFn().withTestBqClientFactory(() -> bqMock);

    PCollection<Void> actual =
        testPipeline
            .apply(
                "CreateInput",
                Create.of(
                        KV.of(partitionedTable, partition),
                        KV.of(table, (BigQueryTablePartition) null))
                    .withCoder(fnCoder))
            .apply("TestDeleteBigQueryDataFn", ParDo.of(fn));
    PAssert.that(actual).empty();
    testPipeline.run(options);

    verifyNoMoreInteractions(bqMock);
  }

  /**
   * Test that DeleteBigQueryDataFn doesn't attempt to delete special BigQuery partitions even if
   * {@code deleteSourceData = true}.
   *
   * <p>As per <a
   * href="https://cloud.google.com/bigquery/docs/managing-partitioned-tables#delete_a_partition">
   * this documentation</a>, special partitions "__NULL__" and "__UNPARTITIONED__" cannot be
   * deleted.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataEnabled_doesntTruncateSpecialPartitions() {
    Options options = TestPipeline.testingPipelineOptions().as(Options.class);
    options.setDeleteSourceData(true);

    BigQueryTablePartition.Builder builder =
        BigQueryTablePartition.builder().setLastModificationTime(System.currentTimeMillis() * 1000);
    BigQueryTablePartition p1 = builder.setPartitionName("__NULL__").build();
    BigQueryTablePartition p2 = builder.setPartitionName("__UNPARTITIONED__").build();
    BigQueryTablePartition p3 = builder.setPartitionName("NORMAL_PARTITION").build();

    BigQueryTable t1 =
        table.toBuilder()
            .setPartitions(Arrays.asList(p1, p2, p3))
            .setPartitioningColumn("column-name-doesnt-matter")
            .build();

    DeleteBigQueryDataFn fn = new DeleteBigQueryDataFn().withTestBqClientFactory(() -> bqMock);

    testPipeline
        .apply(
            "CreateInput",
            Create.of(KV.of(t1, p1), KV.of(t1, p2), KV.of(t1, p3)).withCoder(fnCoder))
        .apply("TestDeleteBigQueryDataFn", ParDo.of(fn));
    testPipeline.run(options);

    verify(bqMock, times(1)).delete(TableId.of("pr1", "d1", "t1$NORMAL_PARTITION"));
    verifyNoMoreInteractions(bqMock);
  }
}
