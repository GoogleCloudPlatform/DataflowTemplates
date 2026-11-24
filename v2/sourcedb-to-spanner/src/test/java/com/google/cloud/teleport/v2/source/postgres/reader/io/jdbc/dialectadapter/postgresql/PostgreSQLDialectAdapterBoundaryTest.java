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
package com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.dialectadapter.postgresql;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.postgres.reader.io.jdbc.dialectadapter.postgresql.PostgreSQLDialectAdapter.PostgreSQLVersion;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PGobject;

/** Tests for the boundary hooks of {@link PostgreSQLDialectAdapter}. */
@RunWith(JUnit4.class)
public class PostgreSQLDialectAdapterBoundaryTest {

  private static final UUID UUID_VALUE = UUID.fromString("01234567-89ab-cdef-0123-456789abcdef");

  private final PostgreSQLDialectAdapter adapter =
      new PostgreSQLDialectAdapter(PostgreSQLVersion.DEFAULT);

  @Test
  public void testExtractBinaryBoundary_uuidAsSixteenBytes() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getObject(1, UUID.class)).thenReturn(UUID_VALUE);
    when(rs.getObject(2, UUID.class)).thenReturn(null);

    assertThat(adapter.extractBinaryBoundary(rs, 1, column("uuid", byte[].class)))
        .isEqualTo(uuidBytes(UUID_VALUE));
    assertThat(adapter.extractBinaryBoundary(rs, 2, column("UUID", byte[].class))).isNull();
  }

  @Test
  public void testExtractBinaryBoundary_otherBinaryTypesAsRawBytes() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getBytes(1)).thenReturn(new byte[] {1, 2, 3});

    assertThat(adapter.extractBinaryBoundary(rs, 1, column("bytea", byte[].class)))
        .isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  public void testToQueryParameter_uuidBytesAsUuid() {
    assertThat(adapter.toQueryParameter(uuidBytes(UUID_VALUE), column("uuid", byte[].class)))
        .isEqualTo(UUID_VALUE);
    // Bytes of other binary columns are bound unchanged.
    byte[] bytes = new byte[] {1, 2, 3};
    assertThat(adapter.toQueryParameter(bytes, column("bytea", byte[].class)))
        .isSameInstanceAs(bytes);
  }

  @Test
  public void testToQueryParameter_bitAsPgObject() {
    Object bound = adapter.toQueryParameter("0101", column("bit", String.class));

    assertThat(bound).isInstanceOf(PGobject.class);
    assertThat(((PGobject) bound).getType()).isEqualTo("bit");
    assertThat(((PGobject) bound).getValue()).isEqualTo("0101");
  }

  @Test
  public void testToQueryParameter_endOfDay() {
    Object time = adapter.toQueryParameter(LocalTime.MAX, column("time", LocalTime.class));
    Object timetz =
        adapter.toQueryParameter(
            OffsetTime.of(LocalTime.MAX, ZoneOffset.ofHours(5)),
            column("timetz", OffsetTime.class));

    assertThat(((PGobject) time).getType()).isEqualTo("time");
    assertThat(((PGobject) time).getValue()).isEqualTo("24:00:00");
    assertThat(((PGobject) timetz).getType()).isEqualTo("timetz");
    assertThat(((PGobject) timetz).getValue()).isEqualTo("24:00:00+05:00");
  }

  @Test
  public void testToQueryParameter_otherValuesUnchanged() {
    LocalTime noon = LocalTime.NOON;

    assertThat(adapter.toQueryParameter(null, column("uuid", byte[].class))).isNull();
    assertThat(adapter.toQueryParameter(42L, column("int8", Long.class))).isEqualTo(42L);
    assertThat(adapter.toQueryParameter(noon, column("time", LocalTime.class))).isEqualTo(noon);
  }

  private static PartitionColumn column(String typeName, Class<?> columnClass) {
    PartitionColumn.Builder builder =
        PartitionColumn.builder()
            .setColumnName("col")
            .setColumnClass(columnClass)
            .setColumnTypeName(typeName);
    if (columnClass == LocalTime.class || columnClass == OffsetTime.class) {
      builder.setDatetimePrecision(6);
    }
    return builder.build();
  }

  private static byte[] uuidBytes(UUID uuid) {
    return ByteBuffer.allocate(16)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
  }
}
