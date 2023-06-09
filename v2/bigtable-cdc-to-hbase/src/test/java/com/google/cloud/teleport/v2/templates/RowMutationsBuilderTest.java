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

import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colFamily2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.colQualifier2;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.timeT;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value;
import static com.google.cloud.teleport.v2.templates.utils.TestConstants.value2;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutationBuilder;
import com.google.cloud.teleport.v2.templates.transforms.ChangeStreamToRowMutations;
import com.google.cloud.teleport.v2.templates.utils.HashUtils;
import com.google.cloud.teleport.v2.templates.utils.RowMutationsBuilder;
import java.util.Arrays;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.util.Time;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Explicit testing on {@link ChangeStreamToRowMutations} individual convert functions. */
@RunWith(JUnit4.class)
public class RowMutationsBuilderTest {

  @Test
  public void testConvertsSetCellToHbasePut() throws Exception {
    ChangeStreamMutation changeStreamMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .setCell(colFamily2, colQualifier2, value2, timeT * 1000)
            .build();

    RowMutations convertedMutations = RowMutationsBuilder.buildRowMutations(changeStreamMutation);

    RowMutations expectedMutations =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    new Put(rowKey.getBytes())
                        .addColumn(
                            colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes())
                        .addColumn(
                            colFamily2.getBytes(),
                            colQualifier2.getBytes(),
                            timeT,
                            value2.getBytes())));

    HashUtils.assertRowMutationsEquals(convertedMutations, expectedMutations);
  }

  @Test
  public void testConvertsDeleteCellsToHbaseDelete() throws Exception {
    ChangeStreamMutation changeStreamMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, 0, timeT * 1000)
            .build();

    RowMutations convertedMutations = RowMutationsBuilder.buildRowMutations(changeStreamMutation);

    RowMutations expectedMutations =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    new Put(rowKey.getBytes())
                        .addColumn(
                            colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes()),
                    new Delete(rowKey.getBytes())
                        .addColumns(colFamily2.getBytes(), colQualifier2.getBytes(), timeT)));
    HashUtils.assertRowMutationsEquals(convertedMutations, expectedMutations);
  }

  @Test
  public void testSkipsDeleteTimestampRange() throws Exception {
    ChangeStreamMutation changeStreamMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            // Delete operation should be filtered out because it has a timestamp range
            .deleteCells(colFamily2, colQualifier2, timeT * 1000, (timeT + 1) * 1000)
            .build();

    RowMutations convertedMutations = RowMutationsBuilder.buildRowMutations(changeStreamMutation);

    RowMutations expectedMutations =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    new Put(rowKey.getBytes())
                        .addColumn(
                            colFamily.getBytes(),
                            colQualifier.getBytes(),
                            timeT,
                            value.getBytes())));
    HashUtils.assertRowMutationsEquals(convertedMutations, expectedMutations);
  }

  @Test
  public void testConvertsDeleteFamilyToHbaseDelete() throws Exception {
    ChangeStreamMutation changeStreamMutation =
        new ChangeStreamMutationBuilder(rowKey, timeT * 1000)
            .setCell(colFamily, colQualifier, value, timeT * 1000)
            .deleteCells(colFamily2, colQualifier2, timeT, timeT * 1000)
            .deleteFamily(colFamily2)
            .build();

    RowMutations convertedMutations = RowMutationsBuilder.buildRowMutations(changeStreamMutation);

    // Note that this timestamp is a placeholder and not compared by hash function.
    // DeleteFamily change stream entries are enriched by a Time.now() timestamp
    // during conversion.
    Long now = Time.now();

    RowMutations expectedMutations =
        new RowMutations(rowKey.getBytes())
            .add(
                Arrays.asList(
                    new Put(rowKey.getBytes())
                        .addColumn(
                            colFamily.getBytes(), colQualifier.getBytes(), timeT, value.getBytes()),
                    new Delete(rowKey.getBytes())
                        // .addColumns(colFamily2.getBytes(),colQualifier2.getBytes(), timeT)
                        .addFamily(colFamily2.getBytes(), now)));

    HashUtils.assertRowMutationsEquals(convertedMutations, expectedMutations);
  }
}
