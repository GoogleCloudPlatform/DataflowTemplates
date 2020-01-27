/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.teleport.v2.templates.BigQueryToParquet.ReadSessionFactory;
import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryToParquet}. */
@RunWith(JUnit4.class)
public class BigQueryToParquetTest {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":" +
          "[{\"name\":\"device_id\",\"type\":[\"null\",\"string\"]}," +
          "{\"name\":\"temperature_c\",\"type\":[\"null\",\"double\"]}," +
          "{\"name\":\"temperature_f\",\"type\":[\"null\",\"double\"]}," +
          "{\"name\":\"sample_time\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}," +
          "{\"name\":\"humidity\",\"type\":[\"null\",\"double\"]}]}";
  private static final String TABLE = "fantasmic-999999:great_data.table";
  private BigQueryStorageClient client = mock(BigQueryStorageClient.class);


  /** Test {@link ReadSessionFactory} throws exception when invalid table reference is provided. */
  @Test(expected = IllegalArgumentException.class)
  public void testReadSessionFactoryBadTable() {

    // Test input
    final String badTableRef = "fantasmic-999999;great_data.table";
    final TableReadOptions tableReadOptions = TableReadOptions.newBuilder().build();
    ReadSessionFactory trsf = new ReadSessionFactory();
    ReadSession trs = trsf.create(client, badTableRef, tableReadOptions);
  }

  /** Test Schema Parser is working as expected. */
  @Test
  public void testSchemaParse() {
    Schema schema = new Schema.Parser().parse(SCHEMA);
    assertEquals(schema.toString(), SCHEMA);
  }

}
