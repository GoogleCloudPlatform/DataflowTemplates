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
package com.google.cloud.teleport.v2.source.sqlserver.reader.io.jdbc.dialectadapter.sqlserver;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the boundary hooks of {@link SqlServerDialectAdapter}. */
@RunWith(JUnit4.class)
public class SqlServerDialectAdapterBoundaryTest {

  private static final String GUID = "00112233-4455-6677-8899-aabbccddeeff";

  /** {@link #GUID} in SQL Server's comparison order: groups g4, g3, then g2, g1, g0 reversed. */
  private static final byte[] SORTABLE =
      new byte[] {
        (byte) 0xaa,
        (byte) 0xbb,
        (byte) 0xcc,
        (byte) 0xdd,
        (byte) 0xee,
        (byte) 0xff,
        (byte) 0x88,
        (byte) 0x99,
        0x77,
        0x66,
        0x55,
        0x44,
        0x33,
        0x22,
        0x11,
        0x00
      };

  private final SqlServerDialectAdapter adapter = new SqlServerDialectAdapter();

  @Test
  public void testExtractBinaryBoundary_uniqueIdentifierInSortOrder() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(1)).thenReturn(GUID);
    when(rs.getString(2)).thenReturn(null);

    assertThat(adapter.extractBinaryBoundary(rs, 1, column("uniqueidentifier")))
        .isEqualTo(SORTABLE);
    assertThat(adapter.extractBinaryBoundary(rs, 2, column("UNIQUEIDENTIFIER"))).isNull();
  }

  @Test
  public void testExtractBinaryBoundary_otherBinaryTypesAsRawBytes() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getBytes(1)).thenReturn(new byte[] {1, 2});

    assertThat(adapter.extractBinaryBoundary(rs, 1, column("varbinary")))
        .isEqualTo(new byte[] {1, 2});
  }

  @Test
  public void testToQueryParameter() {
    assertThat(adapter.toQueryParameter(SORTABLE, column("uniqueidentifier"))).isEqualTo(GUID);
    byte[] bytes = new byte[] {1, 2};
    assertThat(adapter.toQueryParameter(bytes, column("varbinary"))).isSameInstanceAs(bytes);
    assertThat(adapter.toQueryParameter(null, column("uniqueidentifier"))).isNull();
  }

  @Test
  public void testSortableBytesRoundTrip() {
    assertThat(SqlServerUniqueIdentifier.toSortableBytes(GUID)).isEqualTo(SORTABLE);
    assertThat(SqlServerUniqueIdentifier.fromSortableBytes(SORTABLE)).isEqualTo(GUID);
    assertThat(SqlServerUniqueIdentifier.toSortableBytes(null)).isNull();
    assertThrows(
        IllegalArgumentException.class,
        () -> SqlServerUniqueIdentifier.fromSortableBytes(new byte[15]));
  }

  private static PartitionColumn column(String typeName) {
    return PartitionColumn.builder()
        .setColumnName("col")
        .setColumnClass(byte[].class)
        .setColumnTypeName(typeName)
        .build();
  }
}
