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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.cloud.teleport.v2.templates.BigQueryToParquet.ReadSessionFactory;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Unit tests for {@link BigQueryToParquet}. */
@RunWith(JUnit4.class)
public class BigQueryToParquetTest {

  private static final String SCHEMA =
      "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
          + "[{\"name\":\"device_id\",\"type\":[\"null\",\"string\"]},"
          + "{\"name\":\"temperature_c\",\"type\":[\"null\",\"double\"]},"
          + "{\"name\":\"temperature_f\",\"type\":[\"null\",\"double\"]},"
          + "{\"name\":\"sample_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
          + "{\"name\":\"humidity\",\"type\":[\"null\",\"double\"]}]}";
  private static final String TABLE = "fantasmic-999999:great_data.table";
  private BigQueryReadClient client = mock(BigQueryReadClient.class);

  /** Test {@link ReadSessionFactory} creates a v1 ReadSession request with Avro format. */
  @Test
  public void testReadSessionFactoryCreate() {
    ReadSession expectedSession =
        ReadSession.newBuilder().setAvroSchema(AvroSchema.newBuilder().setSchema(SCHEMA)).build();
    when(client.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(expectedSession);

    TableReadOptions tableReadOptions =
        TableReadOptions.newBuilder()
            .addSelectedFields("device_id")
            .setRowRestriction("humidity > 0.5")
            .build();
    BigQueryToParquet.BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquet.BigQueryToParquetOptions.class);
    options.as(BigQueryOptions.class).setProject("parent-project");
    options.setTableRef(TABLE);

    ReadSession session = ReadSessionFactory.create(client, options, tableReadOptions);

    assertEquals(expectedSession, session);
    ArgumentCaptor<CreateReadSessionRequest> captor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(client).createReadSession(captor.capture());
    CreateReadSessionRequest request = captor.getValue();
    assertEquals("projects/parent-project", request.getParent());
    assertEquals(
        "projects/fantasmic-999999/datasets/great_data/tables/table",
        request.getReadSession().getTable());
    assertEquals(DataFormat.AVRO, request.getReadSession().getDataFormat());
    assertEquals(tableReadOptions, request.getReadSession().getReadOptions());
  }

  /** Test {@link ReadSessionFactory} falls back to parent project when tableRef omits project. */
  @Test
  public void testReadSessionFactoryDefaultTableProject() {
    ReadSession expectedSession =
        ReadSession.newBuilder().setAvroSchema(AvroSchema.newBuilder().setSchema(SCHEMA)).build();
    when(client.createReadSession(any(CreateReadSessionRequest.class))).thenReturn(expectedSession);

    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    BigQueryToParquet.BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquet.BigQueryToParquetOptions.class);
    options.as(BigQueryOptions.class).setProject("default-project");
    options.as(BigQueryOptions.class).setBigQueryProject("bq-project");
    options.setTableRef("great_data.table");

    ReadSessionFactory.create(client, options, tableReadOptions);

    ArgumentCaptor<CreateReadSessionRequest> captor =
        ArgumentCaptor.forClass(CreateReadSessionRequest.class);
    verify(client).createReadSession(captor.capture());
    CreateReadSessionRequest request = captor.getValue();
    assertEquals("projects/bq-project", request.getParent());
    assertEquals(
        "projects/bq-project/datasets/great_data/tables/table",
        request.getReadSession().getTable());
  }

  /** Test {@link ReadSessionFactory} throws exception when no project ID is configured. */
  @Test(expected = IllegalArgumentException.class)
  public void testReadSessionFactoryMissingProject() {
    TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    BigQueryToParquet.BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquet.BigQueryToParquetOptions.class);
    options.as(BigQueryOptions.class).setProject(null);
    options.as(BigQueryOptions.class).setBigQueryProject(null);
    options.setTableRef(TABLE);

    ReadSessionFactory.create(client, options, tableReadOptions);
  }

  /** Test {@link ReadSessionFactory} throws exception when invalid table reference is provided. */
  @Test(expected = IllegalArgumentException.class)
  public void testReadSessionFactoryBadTable() {

    // Test input
    final String badTableRef = "fantasmic-999999;great_data.table";
    final TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    BigQueryToParquet.BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquet.BigQueryToParquetOptions.class);
    options.setTableRef(badTableRef);
    ReadSessionFactory trsf = new ReadSessionFactory();
    ReadSession trs = trsf.create(client, options, tableReadOptions);
  }

  /** Test Schema Parser is working as expected. */
  @Test
  public void testSchemaParse() {
    Schema schema = new Schema.Parser().parse(SCHEMA);
    assertEquals(schema.toString(), SCHEMA);
  }
}
