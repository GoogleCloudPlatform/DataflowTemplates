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
import com.google.cloud.teleport.v2.templates.transforms.HbaseRowMutationIO;
import com.google.cloud.teleport.v2.templates.utils.HbaseUtils;
import com.google.cloud.teleport.v2.templates.utils.MutationBuilderUtils.ChangeStreamMutationBuilder;
import com.google.cloud.teleport.v2.templates.utils.MutationBuilderUtils.HbaseMutationBuilder;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsCoder;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for Hbase row mutation IO. */
@RunWith(JUnit4.class)
public class HbaseRowMutationIOTest {

  private static final Logger log = LoggerFactory.getLogger(HbaseRowMutationIOTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static HBaseTestingUtility hbaseTestingUtil;

  public HbaseRowMutationIOTest() {}

  @BeforeClass
  public static void setUpCluster() throws Exception {
    // Create an HBase test cluster with one table.
    hbaseTestingUtil = new HBaseTestingUtility();
    hbaseTestingUtil.startMiniCluster();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    hbaseTestingUtil.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    // Provide custom encoder to non-serializable RowMutations class.
    pipeline.getCoderRegistry().registerCoderForClass(RowMutations.class, RowMutationsCoder.of());
  }

  @Test
  public void writesSingleRowPutsInOrder() throws Exception {

    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    RowMutations rowMutationsOnTwoColFamilies = new RowMutations(rowKey.getBytes());
    rowMutationsOnTwoColFamilies.add(
        Arrays.asList(
            HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
            HbaseMutationBuilder.createPut(rowKey, colFamily2, colQualifier2, value2, timeT)));

    RowMutations overwritingRowMutations =
        new RowMutations(rowKey2.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createPut(rowKey2, colFamily, colQualifier, value, timeT),
                    HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier, value2, timeT)));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey.getBytes(), rowMutationsOnTwoColFamilies),
                KV.of(rowKey2.getBytes(), overwritingRowMutations)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertEquals(2, HbaseUtils.getRowResult(table, rowKey).size());
    Assert.assertEquals(value, HbaseUtils.getCell(table, rowKey, colFamily, colQualifier));
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily2, colQualifier2));

    Assert.assertEquals(1, HbaseUtils.getRowResult(table, rowKey2).size());
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey2, colFamily, colQualifier));
  }

  @Test
  public void writesDeletesInOrder() throws Exception {
    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    // Expect deletes to result in empty row.
    RowMutations deleteCellMutation =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT),
                    HbaseMutationBuilder.createDelete(rowKey, colFamily, colQualifier, timeT)));
    // Expect delete family to delete entire row.
    RowMutations deleteColFamilyMutation =
        new RowMutations(rowKey2.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createPut(rowKey2, colFamily, colQualifier, value, timeT),
                    HbaseMutationBuilder.createPut(
                        rowKey2, colFamily, colQualifier2, value2, timeT),
                    HbaseMutationBuilder.createDeleteFamily(rowKey2, colFamily, Long.MAX_VALUE)));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey.getBytes(), deleteCellMutation),
                KV.of(rowKey2.getBytes(), deleteColFamilyMutation)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(HbaseUtils.getRowResult(table, rowKey).isEmpty());
    Assert.assertTrue(HbaseUtils.getRowResult(table, rowKey2).isEmpty());
  }

  @Test
  public void writesDeletesThenPutsInOrder() throws Exception {
    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    // We cannot group the 2 mutations in the same RowMutations object because Hbase delete operates
    // by timestamp rather than RowMutations.mutations list ordering.
    // This is a known issue with HBase, see https://issues.apache.org/jira/browse/HBASE-2256
    RowMutations put =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createPut(rowKey, colFamily, colQualifier, value, timeT)));
    RowMutations delete =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createDeleteFamily(rowKey, colFamily, timeT + 1)));
    RowMutations put2 =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    HbaseMutationBuilder.createPut(
                        rowKey, colFamily, colQualifier, value2, timeT + 2)));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey.getBytes(), put),
                KV.of(rowKey.getBytes(), delete),
                KV.of(rowKey.getBytes(), put2)))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily, colQualifier));
  }

  // TODO: move this to its own test file.
  //  Spinning up 2 HbaseTestingUtils in 2 test files causes competition, so
  //  all such tests are in one file right now.
  @Test
  public void pipelineWritesToHbase() throws IOException {

    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    ChangeStreamMutation rowMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .build();

    ChangeStreamMutation rowMutation2 =
        new ChangeStreamMutationBuilder(rowKey, (timeT + 1) * 1000)
            .deleteCells(colFamily, colQualifier, 0L, (timeT + 1) * 1000)
            .build();

    ChangeStreamMutation rowMutation3 =
        new ChangeStreamMutationBuilder(rowKey, (timeT + 2) * 1000)
            .setCell(colFamily, colQualifier, value2, (timeT + 2) * 1000)
            .build();

    pipeline
        // TODO swap in bigtableToHbasePipeline fn after CDC GA
        .apply(
            "Create change stream mutations",
            Create.of(
                KV.of(ByteString.copyFromUtf8(rowKey), rowMutation),
                KV.of(ByteString.copyFromUtf8(rowKey), rowMutation2),
                KV.of(ByteString.copyFromUtf8(rowKey), rowMutation3)))
        .apply(
            "Convert change stream mutations to hbase mutations",
            ConvertChangeStream.convertChangeStreamMutation())
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();
    Assert.assertEquals(1, HbaseUtils.getRowResult(table, rowKey).size());
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily, colQualifier));
  }

  //
  // TODO: move this to its own test file.
  //  Spinning up 2 HbaseTestingUtils in 2 test files causes competition, so
  //  all such tests are in one file right now.
  @Test
  public void pipelineWritesToHbaseWithTwoWayReplication() throws IOException {

    Table table = HbaseUtils.createTable(hbaseTestingUtil);

    ChangeStreamMutation shouldBeFilteredOut =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily, hbaseQualifier, 0L, 0L)
            .build();

    ChangeStreamMutation rowMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value2, timeT * 1000)
            .build();

    pipeline
        // TODO swap in bigtableToHbasePipeline fn after CDC GA
        .apply(
            "Create change stream mutations",
            Create.of(
                KV.of(ByteString.copyFromUtf8(rowKey), shouldBeFilteredOut),
                KV.of(ByteString.copyFromUtf8(rowKey), rowMutation)))
        .apply(
            "Convert change stream mutations to hbase mutations",
            ConvertChangeStream.convertChangeStreamMutation()
                .withTwoWayReplication(true, cbtQualifier, hbaseQualifier))
        .apply(
            "Write to hbase",
            HbaseRowMutationIO.writeRowMutations()
                .withConfiguration(hbaseTestingUtil.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();
    Assert.assertEquals(1, HbaseUtils.getRowResult(table, rowKey).size());
    Assert.assertEquals(value2, HbaseUtils.getCell(table, rowKey, colFamily2, colQualifier2));
  }
}
