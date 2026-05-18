/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.v2.templates.dofn.SelectTableFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SelectTable}. */
@RunWith(JUnit4.class)
public class SelectTableTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testWeightedSelectionLogic() throws Exception {
    // Schema:
    // Root A (10 QPS) has Child B (100 QPS)
    // Root C (50 QPS)
    // Total QPS = 160.
    // Weight A = 110 (10+100). Prob = 110/160 = 0.6875
    // Weight C = 50. Prob = 50/160 = 0.3125

    DataGeneratorTable tableA =
        DataGeneratorTable.builder()
            .name("A")
            .insertQps(10)
            .isRoot(true)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable tableB =
        DataGeneratorTable.builder()
            .name("B")
            .insertQps(100)
            .isRoot(false)
            .interleavedInTable("A")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable tableC =
        DataGeneratorTable.builder()
            .name("C")
            .insertQps(50)
            .isRoot(true)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("A", tableA, "B", tableB, "C", tableC))
            .build();

    // Build DAG to populate childTables and isRoot
    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    EnumeratedDistribution<DataGeneratorTable> distribution =
        SelectTableFn.buildDistribution(dagSchema);

    assertNotNull(distribution);
    List<Pair<DataGeneratorTable, Double>> pmf = distribution.getPmf();
    assertEquals(2, pmf.size());

    double probA = 0;
    double probC = 0;

    for (Pair<DataGeneratorTable, Double> pair : pmf) {
      if (pair.getKey().name().equals("A")) {
        probA = pair.getValue();
      } else if (pair.getKey().name().equals("C")) {
        probC = pair.getValue();
      }
    }

    assertEquals(110.0 / 160.0, probA, 0.0001);
    assertEquals(50.0 / 160.0, probC, 0.0001);
  }

  @Test
  public void testSelectTableProcessElement() {
    DataGeneratorTable tableA =
        DataGeneratorTable.builder()
            .name("A")
            .insertQps(10)
            .isRoot(true)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("A", tableA)).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline
            .apply("CreateSchema", org.apache.beam.sdk.transforms.Create.of(schema))
            .apply("AsSingleton", org.apache.beam.sdk.transforms.View.asSingleton());

    PCollection<DataGeneratorTable> output =
        pipeline
            .apply("CreateInput", org.apache.beam.sdk.transforms.Create.of(1L, 2L))
            .apply("SelectTable", new SelectTable(schemaView));

    org.apache.beam.sdk.testing.PAssert.that(output)
        .satisfies(
            iterable -> {
              int count = 0;
              for (DataGeneratorTable t : iterable) {
                count++;
                assertEquals("A", t.name());
              }
              assertEquals(2, count);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testSelectTableEmptySchema() {
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline
            .apply("CreateSchema", org.apache.beam.sdk.transforms.Create.of(schema))
            .apply("AsSingleton", org.apache.beam.sdk.transforms.View.asSingleton());

    PCollection<DataGeneratorTable> output =
        pipeline
            .apply("CreateInput", org.apache.beam.sdk.transforms.Create.of(1L))
            .apply("SelectTable", new SelectTable(schemaView));

    org.apache.beam.sdk.testing.PAssert.that(output).empty();

    pipeline.run();
  }
}
