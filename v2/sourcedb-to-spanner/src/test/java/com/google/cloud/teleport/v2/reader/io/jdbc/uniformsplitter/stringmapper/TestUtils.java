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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper;

import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_EMPTY_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_SPACE_COL;
import static com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.WEIGHT_COL;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TestUtils {

  public static int wireMockResultSet(List<CollationOrderRow> rows, ResultSet mockResultSet)
      throws SQLException {
    Iterator<CollationOrderRow> lineIterator = rows.iterator();
    AtomicReference<CollationOrderRow> currentRow = new AtomicReference<>();

    when(mockResultSet.next())
        .thenAnswer(
            invocation -> {
              boolean ret = lineIterator.hasNext();
              if (ret) {
                currentRow.set(lineIterator.next());
              }
              return ret;
            });

    when(mockResultSet.getString(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              CollationOrderRow row = currentRow.get();
              if (row == null) {
                return null;
              }
              switch (colName) {
                case CHARSET_CHAR_COL:
                  return row.charsetChar();
                case EQUIVALENT_CHARSET_CHAR_COL:
                  return row.equivalentChar();
                case EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL:
                  return row.equivalentCharPadSpace();
                default:
                  return null;
              }
            });

    when(mockResultSet.getLong(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              CollationOrderRow row = currentRow.get();
              if (row == null) {
                return 0L;
              }
              switch (colName) {
                case CODEPOINT_RANK_COL:
                  return row.codepointRank();
                case CODEPOINT_RANK_PAD_SPACE_COL:
                  return row.codepointRankPadSpace();
                default:
                  return 0L;
              }
            });

    when(mockResultSet.getBoolean(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              CollationOrderRow row = currentRow.get();
              if (row == null) {
                return false;
              }
              switch (colName) {
                case IS_EMPTY_COL:
                  return row.isEmpty();
                case IS_SPACE_COL:
                  return row.isSpace();
                default:
                  return false;
              }
            });

    when(mockResultSet.getBytes(anyString()))
        .thenAnswer(
            invocation -> {
              String colName = invocation.getArgument(0);
              CollationOrderRow row = currentRow.get();
              if (row == null) {
                return null;
              }
              if (WEIGHT_COL.equals(colName)) {
                if (row.isEmpty()) {
                  return null;
                }
                return ByteBuffer.allocate(8).putLong(row.codepointRank()).array();
              }
              return null;
            });

    return rows.size();
  }
}
