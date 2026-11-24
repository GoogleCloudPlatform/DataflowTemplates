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

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Converts SQL Server {@code UNIQUEIDENTIFIER} values to and from 16-byte arrays that sort, as
 * unsigned big-endian bytes, exactly like SQL Server sorts them. The uniform splitter computes
 * split points on these bytes in Java, so the order must match the database's {@code <}, {@code >}
 * and {@code ORDER BY}.
 */
public final class SqlServerUniqueIdentifier {

  /**
   * Permutation mapping Java big-endian UUID bytes {@code raw[0..15]} to SQL Server's {@code
   * UNIQUEIDENTIFIER} comparison order.
   *
   * <p>SQL Server stores a {@code UNIQUEIDENTIFIER} {@code g0-g1-g2-g3-g4} in mixed-endian form
   * ({@code g0}, {@code g1} and {@code g2} little-endian, {@code g3} and {@code g4} big-endian) and
   * compares the stored bytes in group order {@code g4, g3, g2, g1, g0}. Relative to the canonical
   * big-endian byte layout this yields: {@code g4} (10..15), {@code g3} (8, 9), and the bytes of
   * {@code g2} (7, 6), {@code g1} (5, 4) and {@code g0} (3, 2, 1, 0) from least to most
   * significant. For example, in SQL Server {@code '01000000-0000-0000-0000-000000000000'} sorts
   * before {@code '00000001-0000-0000-0000-000000000000'}.
   */
  public static final int[] BYTE_ORDER = {10, 11, 12, 13, 14, 15, 8, 9, 7, 6, 5, 4, 3, 2, 1, 0};

  private SqlServerUniqueIdentifier() {}

  /**
   * Converts a canonical UUID string to bytes in SQL Server's comparison order.
   *
   * @param uuidString canonical UUID string, or null.
   * @return 16 bytes in SQL Server's comparison order, or null.
   */
  @Nullable
  public static byte[] toSortableBytes(@Nullable String uuidString) {
    if (uuidString == null) {
      return null;
    }
    UUID uuid = UUID.fromString(uuidString);
    ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
    bb.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
    byte[] raw = bb.array();
    byte[] sorted = new byte[16];
    for (int i = 0; i < 16; i++) {
      sorted[i] = raw[BYTE_ORDER[i]];
    }
    return sorted;
  }

  /**
   * Converts bytes in SQL Server's comparison order back to a canonical UUID string. Inverse of
   * {@link #toSortableBytes(String)}.
   *
   * @param sorted 16 bytes in SQL Server's comparison order.
   * @return canonical UUID string.
   */
  public static String fromSortableBytes(byte[] sorted) {
    Preconditions.checkArgument(sorted != null && sorted.length == 16);
    byte[] raw = new byte[16];
    for (int i = 0; i < 16; i++) {
      raw[BYTE_ORDER[i]] = sorted[i];
    }
    ByteBuffer bb = ByteBuffer.wrap(raw);
    return new UUID(bb.getLong(), bb.getLong()).toString();
  }
}
