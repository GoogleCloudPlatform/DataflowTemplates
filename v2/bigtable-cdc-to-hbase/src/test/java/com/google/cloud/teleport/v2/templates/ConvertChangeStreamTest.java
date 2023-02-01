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

import static com.google.cloud.teleport.v2.templates.constants.TestConstants.cbtQualifier;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colFamily2;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colQualifier2;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.hbaseQualifier;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.rowKey2;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.timeT;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.value;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.value2;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.teleport.v2.templates.transforms.ConvertChangeStream;
import com.google.cloud.teleport.v2.templates.utils.HashUtils;
import com.google.cloud.teleport.v2.templates.utils.HashUtils.HashHbaseRowMutations;
import com.google.cloud.teleport.v2.templates.utils.MutationBuilderUtils.ChangeStreamMutationBuilder;
import com.google.cloud.teleport.v2.templates.utils.MutationBuilderUtils.HbaseMutationBuilder;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsCoder;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for the ConvertChangeStreamMutation transformation. */
@RunWith(JUnit4.class)
public class ConvertChangeStreamTest {
  private static final Logger log = LoggerFactory.getLogger(ConvertChangeStreamTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    // Provide custom encoder to non-serializable RowMutations class.
    pipeline.getCoderRegistry().registerCoderForClass(RowMutations.class, RowMutationsCoder.of());
  }

  @Test
  public void convertsSetCellToHbasePut() throws Exception {

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
                ConvertChangeStream.convertChangeStreamMutation())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            HbaseMutationBuilder.createPut(rowKey, colFamily2, colQualifier2, value2, timeT));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsDeleteCellsToHbaseDelete() throws Exception {
    // Create two change stream mutations on a single row key.
    ChangeStreamMutation deleteCellsMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, timeT, timeT * 1000)
            .build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), deleteCellsMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ConvertChangeStream.convertChangeStreamMutation())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            HbaseMutationBuilder.createDelete(rowKey, colFamily2, colQualifier2, timeT));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsDeleteFamilyToHbaseDelete() throws Exception {
    // Create two change stream mutations on a single row key.
    ChangeStreamMutation deleteFamilyMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000).deleteFamily(colFamily2).build();

    PCollection<KV<String, List<String>>> output =
        pipeline
            .apply(
                "Create change stream mutations",
                Create.of(KV.of(ByteString.copyFromUtf8(rowKey), deleteFamilyMutation)))
            .apply(
                "Convert change stream mutations to hbase mutations",
                ConvertChangeStream.convertChangeStreamMutation())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    Long now = Time.now();

    List<Mutation> expectedMutations =
        Arrays.asList(HbaseMutationBuilder.createDeleteFamily(rowKey, colFamily2, now));
    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void convertsMultipleRows() throws Exception {

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
                ConvertChangeStream.convertChangeStreamMutation())
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> rowMutations =
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            HbaseMutationBuilder.createDelete(rowKey, colFamily, colQualifier, timeT),
            HbaseMutationBuilder.createDeleteFamily(rowKey, colFamily, now));

    List<Mutation> rowMutations2 =
        Arrays.asList(
            HbaseMutationBuilder.createDelete(rowKey2, colFamily, colQualifier, timeT),
            HbaseMutationBuilder.createDelete(rowKey2, colFamily2, colQualifier2, timeT),
            HbaseMutationBuilder.createPut(rowKey2, colFamily2, colQualifier2, value, timeT));
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of(rowKey, HashUtils.hashMutationList(rowMutations)),
            KV.of(rowKey2, HashUtils.hashMutationList(rowMutations2)));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void addsSpecialMutationInTwoWayReplication() throws Exception {

    // Create two change stream mutations on a single row key.
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
                ConvertChangeStream.convertChangeStreamMutation()
                    .withTwoWayReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> expectedMutations =
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            HbaseMutationBuilder.createPut(rowKey, colFamily2, colQualifier2, value2, timeT),
            // Special mutation that denotes origin of replication.
            HbaseMutationBuilder.createDelete(rowKey, colFamily2, cbtQualifier, 0L));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(expectedMutations)));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void filtersOutHbaseReplicatedMutations() {

    // Create two change stream mutations on a single row key.
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
                ConvertChangeStream.convertChangeStreamMutation()
                    .withTwoWayReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    PAssert.that(output).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void filtersAndReplicatesMultipleRowsWithTwoWayReplication() throws Exception {

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
                ConvertChangeStream.convertChangeStreamMutation()
                    .withTwoWayReplication(true, cbtQualifier, hbaseQualifier))
            .apply("Hash hbase mutation for comparison purposes", new HashHbaseRowMutations());

    List<Mutation> rowMutations =
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            // Special mutation that denotes origin of replication.
            HbaseMutationBuilder.createDelete(rowKey, colFamily, cbtQualifier, 0L));

    PAssert.that(output)
        .containsInAnyOrder(KV.of(rowKey, HashUtils.hashMutationList(rowMutations)));

    pipeline.run().waitUntilFinish();
  }
}
