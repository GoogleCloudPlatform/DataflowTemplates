/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TableReadSpecificationTest {

  @Test
  public void testTableReadSpecificationBuilder() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("test_table").build();
    JdbcIO.RowMapper<String> mockMapper = mock(JdbcIO.RowMapper.class);

    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .setFetchSize(1000)
            .build();

    assertThat(spec.tableIdentifier()).isEqualTo(tableId);
    assertThat(spec.rowMapper()).isEqualTo(mockMapper);
    assertThat(spec.fetchSize()).isEqualTo(1000);
  }

  @Test
  public void testTableReadSpecificationDefaultFetchSize() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("test_table").build();
    JdbcIO.RowMapper<String> mockMapper = mock(JdbcIO.RowMapper.class);

    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    assertThat(spec.fetchSize()).isEqualTo(50000);
  }
}
