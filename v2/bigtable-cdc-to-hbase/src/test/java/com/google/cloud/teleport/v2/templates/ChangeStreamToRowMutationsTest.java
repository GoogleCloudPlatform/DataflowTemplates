/*
 * Copyright (C) 2023 Google LLC
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

import static com.google.cloud.teleport.v2.templates.utils.TestConstants.cbtQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.hbaseQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.timeT;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value2;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutationBuilder;
import com.google.cloud.teleport.v2.templates.transforms.ChangeStreamToRowMutations;
import com.google.cloud.teleport.v2.templates.utils.HashUtils;
import com.google.cloud.teleport.v2.templates.utils.HashUtils.HashHbaseRowMutations;
import com.google.cloud.teleport.v2.templates.utils.HbaseUtils;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.util.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for the ConvertChangeStream transformation. */
@RunWith(JUnit4.class)
public class ChangeStreamToRowMutationsTest {
  private static final Logger log = LoggerFactory.getLogger(ChangeStreamToRowMutationsTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertsSetCellToHbasePut() throws Exception {
    ChangeStreamMutation setCellMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value2, timeT * 1000)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), setCellMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            new Put(rowKey.getBytes())
                .addColumn(colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes())
                .addColumn(
                    colFamily2.getBytes(), colQualifier2.getBytes(), timeT, value2.getBytes()));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testConvertsDeleteCellsToHbaseDelete() throws Exception {
    ChangeStreamMutation deleteCellsMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, 0, timeT * 1000)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), deleteCellsMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            new Put(rowKey.getBytes())
                .addColumn(colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes()),
            new Delete(rowKey.getBytes())
                .addColumns(colFamily2.getBytes(), colQualifier2.getBytes(), timeT));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSkipsDeleteTimestampRange() throws Exception {
    ChangeStreamMutation deleteCellsMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, timeT, timeT + 1)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), deleteCellsMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            new Put(rowKey.getBytes())
                .addColumn(colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes()));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testConvertsDeleteFamilyToHbaseDelete() throws Exception {
    ChangeStreamMutation deleteFamilyMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000).deleteFamily(colFamily2).build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), deleteFamilyMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    // Note that this timestamp is a placeholder and not compared by hash function.
    // DeleteFamily change stream entries are enriched by a Time.now() timestamp
    // during conversion.
    Long now = Time.now();

    List<Mutation> expectedMutations =
        Arrays.asList(new Delete(rowKey.getBytes()).addFamily(colFamily2.getBytes(), now));
    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testConvertsMultipleRows() throws Exception {
    ChangeStreamMutation rowMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily, colQualifier, 0L, timeT * 1000)
            .deleteFamily(colFamily)
            .build();
    ChangeStreamMutation rowMutation2 =
        new ChangeStreamMutationBuilder(rowKey2, timeT * 1000)
            .deleteCells(colFamily, colQualifier, 0L, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, 0L, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value, timeT * 1000)
            .build();

    // Note that this timestamp is a placeholder and not compared by hash function.
    // DeleteFamily change stream entries are enriched by a Time.now() timestamp
    // during conversion.
    Long now = Time.now();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(
                    KV.of(ByteString.copyFromUtf8(rowKey), rowMutation),
                    KV.of(ByteString.copyFromUtf8(rowKey2), rowMutation2)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> rowMutations =
        Arrays.asList(
            new Put(rowKey.getBytes())
                .addColumn(colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes()),
            new Delete(rowKey.getBytes())
                .addColumns(colFamily.getBytes(), colQualifier.getBytes(), timeT)
                .addFamily(colFamily.getBytes(), now));

    List<Mutation> rowMutations2 =
        Arrays.asList(
            new Delete(rowKey2.getBytes())
                .addColumns(colFamily.getBytes(), colQualifier.getBytes(), timeT)
                .addColumns(colFamily2.getBytes(), colQualifier2.getBytes(), timeT),
            new Put(rowKey2.getBytes())
                .addColumn(
                    colFamily2.getBytes(), colQualifier2.getBytes(), timeT, value.getBytes()));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(rowKey, HashUtils.hashMutationList(rowMutations)),
            KV.of(rowKey2, HashUtils.hashMutationList(rowMutations2)));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testAddsSpecialMutationInBidirectionalReplication() throws Exception {
    ChangeStreamMutation setCellMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value2, timeT * 1000)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), setCellMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream()
                    .withBidirectionalReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            new Put(rowKey.getBytes())
                .addColumn(colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes())
                .addColumn(
                    colFamily2.getBytes(), colQualifier2.getBytes(), timeT, value2.getBytes()),
            // Special mutation that denotes origin of replication.
            new Delete(rowKey.getBytes())
                .addColumns(colFamily2.getBytes(), cbtQualifier.getBytes(), 0L));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFiltersOutHbaseReplicatedMutations() {
    ChangeStreamMutation setCellMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            // Special mutation, indicates that this mutation batch should be filtered out.
            .deleteCells(colFamily, hbaseQualifier, 0L, 0L)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), setCellMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream()
                    .withBidirectionalReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    PAssert.that(output).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFiltersAndReplicatesMultipleRowsWithBidirectionalReplication() throws Exception {
    ChangeStreamMutation rowMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .build();
    ChangeStreamMutation rowMutation2 =
        new ChangeStreamMutationBuilder(rowKey2, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value, timeT * 1000)
            // Special mutation, indicates that this mutation batch should be filtered out.
            .deleteCells(colFamily, hbaseQualifier, 0L, 0L)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(
                    KV.of(ByteString.copyFromUtf8(rowKey), rowMutation),
                    KV.of(ByteString.copyFromUtf8(rowKey2), rowMutation2)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ChangeStreamToRowMutations.convertChangeStream()
                    .withBidirectionalReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> rowMutations =
        Arrays.asList(
            HbaseUtils.HbaseMutationBuilder.createPut(
                rowKey, colFamily, colQualifier, value, timeT),
            // Special mutation that denotes origin of replication.
            HbaseUtils.HbaseMutationBuilder.createDelete(rowKey, colFamily, cbtQualifier, 0L));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(rowMutations)));

    pipeline.run().waitUntilFinish();
  }
}
