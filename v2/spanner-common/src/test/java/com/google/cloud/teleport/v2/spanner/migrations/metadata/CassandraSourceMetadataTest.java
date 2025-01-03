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
package com.google.cloud.teleport.v2.spanner.migrations.metadata;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CassandraSourceMetadataTest {

  @Mock private ResultSet mockResultSet;
  @Mock private Row mockRow1;
  @Mock private Row mockRow2;
  @Mock private Schema mockSchema;

  private CassandraSourceMetadata.Builder builder;

  @Before
  public void setUp() {
    builder = new CassandraSourceMetadata.Builder();
  }

  @Test
  public void testGenerateSourceSchema() {
    doAnswer(
            invocation -> {
              Iterable<Row> iterable = Arrays.asList(mockRow1, mockRow2);
              iterable.forEach(invocation.getArgument(0));
              return null;
            })
        .when(mockResultSet)
        .forEach(any());

    when(mockRow1.getString("table_name")).thenReturn("table1");
    when(mockRow1.getString("column_name")).thenReturn("column1");
    when(mockRow1.getString("type")).thenReturn("text");
    when(mockRow1.getString("kind")).thenReturn("partition_key");

    when(mockRow2.getString("table_name")).thenReturn("table1");
    when(mockRow2.getString("column_name")).thenReturn("column2");
    when(mockRow2.getString("type")).thenReturn("int");
    when(mockRow2.getString("kind")).thenReturn("clustering");

    CassandraSourceMetadata metadata = builder.setResultSet(mockResultSet).build();

    assertNotNull("Metadata should be generated successfully", metadata);
  }
}
