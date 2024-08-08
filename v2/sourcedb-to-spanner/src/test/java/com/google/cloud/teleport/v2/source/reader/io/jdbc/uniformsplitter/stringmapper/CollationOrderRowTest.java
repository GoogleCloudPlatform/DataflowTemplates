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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_EMPTY_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_SPACE_COL;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CollationOrderRow}. */
@RunWith(MockitoJUnitRunner.class)
public class CollationOrderRowTest {
  @Mock ResultSet mockResultSet;

  @Test
  public void testCollationOrderRowBasic() {
    CollationOrderRow collationOrderRow =
        CollationOrderRow.builder()
            .setCharsetChar('a')
            .setEquivalentChar('A')
            .setEquivalentCharPadSpace('A')
            .setCodepointRank(1L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(false)
            .setIsSpace(false)
            .build();
    assertThat(collationOrderRow.codepointRank()).isEqualTo(1L);
    assertThat(collationOrderRow.codepointRankPadSpace()).isEqualTo(0L);
    assertThat(collationOrderRow.charsetChar()).isEqualTo('a');
    assertThat(collationOrderRow.equivalentChar()).isEqualTo('A');
    assertThat(collationOrderRow.equivalentCharPadSpace()).isEqualTo('A');
    assertThat(collationOrderRow.isEmpty()).isFalse();
    assertThat(collationOrderRow.isSpace()).isFalse();
  }

  @Test
  public void testCollationOrderRowFromRsBasic() throws SQLException {

    when(mockResultSet.getString(CHARSET_CHAR_COL)).thenReturn("a");
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_COL)).thenReturn("a");
    when(mockResultSet.getLong(CODEPOINT_RANK_COL)).thenReturn(0L);
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL)).thenReturn("a");
    when(mockResultSet.getLong(CODEPOINT_RANK_PAD_SPACE_COL)).thenReturn(0L);
    when(mockResultSet.getBoolean(IS_EMPTY_COL)).thenReturn(false);
    when(mockResultSet.getBoolean(IS_SPACE_COL)).thenReturn(false);

    CollationOrderRow collationOrderRow = CollationOrderRow.fromRS(mockResultSet);
    assertThat(collationOrderRow)
        .isEqualTo(
            CollationOrderRow.builder()
                .setCharsetChar('a')
                .setEquivalentChar('a')
                .setEquivalentCharPadSpace('a')
                .setCodepointRank(0L)
                .setCodepointRankPadSpace(0L)
                .setIsEmpty(false)
                .setIsSpace(false)
                .build());
  }

  @Test
  public void testCollationOrderRowFromRsException() throws SQLException {
    int expcetedIllegalArgumentExceptionCount = 0;
    when(mockResultSet.getString(CHARSET_CHAR_COL)).thenReturn("aa").thenReturn("a");
    expcetedIllegalArgumentExceptionCount++;
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_COL)).thenReturn("a").thenReturn("aa");
    expcetedIllegalArgumentExceptionCount++;
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL))
        .thenReturn("a")
        .thenReturn("a")
        .thenReturn("aa");
    expcetedIllegalArgumentExceptionCount++;
    when(mockResultSet.getLong(CODEPOINT_RANK_COL)).thenReturn(0L);
    when(mockResultSet.getLong(CODEPOINT_RANK_COL)).thenReturn(0L);
    when(mockResultSet.getBoolean(IS_EMPTY_COL)).thenReturn(false);
    when(mockResultSet.getBoolean(IS_SPACE_COL)).thenReturn(false);

    for (int i = 0; i < expcetedIllegalArgumentExceptionCount; i++) {
      assertThrows(IllegalArgumentException.class, () -> CollationOrderRow.fromRS(mockResultSet));
    }
  }
}
