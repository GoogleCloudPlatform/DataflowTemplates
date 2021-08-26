/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test cases for the {@link KuduToBigQuery} class. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class KuduToBigQueryTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void kuduRowResultShouldConvertedToBigQueryTableRowAsExpected() {
    // BigQuery TableRows expected result
    TableRow expectedTableRow = new TableRow();
    expectedTableRow.set("INT32", 1);
    expectedTableRow.set("BOOL", true);
    expectedTableRow.set("DOUBLE", 1.2);
    expectedTableRow.set("FLOAT", 1.3);
    expectedTableRow.set("INT8", 8);
    expectedTableRow.set("INT16", 16);
    expectedTableRow.set("INT64", 64);
    expectedTableRow.set("STRING", "string");
    expectedTableRow.set("UNIXTIME_MICROS", 1586422251000L);
    // Using Mockito to mock Kudu RowResults. The objects needs to be in column order
    RowResult kuduResult = Mockito.mock(RowResult.class);
    Mockito.doReturn(createTestKuduSchema()).when(kuduResult).getSchema();
    Mockito.doReturn(1).when(kuduResult).getObject(0);
    Mockito.doReturn(true).when(kuduResult).getObject(1);
    Mockito.doReturn(1.2).when(kuduResult).getObject(2);
    Mockito.doReturn(1.3).when(kuduResult).getObject(3);
    Mockito.doReturn(8).when(kuduResult).getObject(4);
    Mockito.doReturn(16).when(kuduResult).getObject(5);
    Mockito.doReturn(64).when(kuduResult).getObject(6);
    Mockito.doReturn("string").when(kuduResult).getObject(7);
    Mockito.doReturn(1586422251000000000L).when(kuduResult).getLong(8);

    // Convert using KuduConterters Class
    KuduToBigQuery.KuduConverters kuduConverters = new KuduToBigQuery.KuduConverters();
    TableRow convertedTableRow = kuduConverters.apply(kuduResult);
    List<TableRow> rows = ImmutableList.of(convertedTableRow);
    PCollection<TableRow> resultRows = pipeline.apply("CreateInput", Create.of(rows));

    // Check if TableRow expected equal to convertion result
    assertThat(convertedTableRow, equalTo(expectedTableRow));
    PAssert.that(resultRows)
        .satisfies(
            collection -> {
              TableRow result = collection.iterator().next();
              assertThat(result.get("INT32"), equalTo(1));
              assertThat(result.get("BOOL"), equalTo(true));
              assertThat(result.get("DOUBLE"), equalTo(1.2));
              assertThat(result.get("FLOAT"), equalTo(1.3));
              assertThat(result.get("INT8"), equalTo(8));
              assertThat(result.get("INT16"), equalTo(16));
              assertThat(result.get("INT64"), equalTo(64));
              assertThat(result.get("STRING"), equalTo("string"));
              assertThat(result.get("UNIXTIME_MICROS"), equalTo(1586422251000L));
              return null;
            });

    // Execute pipeline
    pipeline.run();
  }

  /**
   * The schemas for the main input of the kudu rows transformation. Checking all possible supported
   * data types
   */
  private Schema createTestKuduSchema() {
    List<ColumnSchema> columns =
        ImmutableList.<ColumnSchema>builder()
            .add(new ColumnSchema.ColumnSchemaBuilder("INT32", Type.INT32).key(true).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("BOOL", Type.BOOL).nullable(false).build())
            .add(
                new ColumnSchema.ColumnSchemaBuilder("DOUBLE", Type.DOUBLE).nullable(false).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("FLOAT", Type.FLOAT).nullable(true).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("INT8", Type.INT8).nullable(true).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("INT16", Type.INT16).nullable(true).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("INT64", Type.INT64).nullable(true).build())
            .add(new ColumnSchema.ColumnSchemaBuilder("STRING", Type.STRING).nullable(true).build())
            .add(
                new ColumnSchema.ColumnSchemaBuilder("UNIXTIME_MICROS", Type.UNIXTIME_MICROS)
                    .nullable(true)
                    .build())
            .build();

    return new Schema(columns);
  }
}
