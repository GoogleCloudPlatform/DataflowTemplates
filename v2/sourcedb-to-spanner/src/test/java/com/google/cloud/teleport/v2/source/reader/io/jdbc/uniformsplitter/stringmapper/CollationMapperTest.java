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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

/** Test class for {@link CollationMapper}. */
@RunWith(MockitoJUnitRunner.class)
public class CollationMapperTest {
  @Mock Connection mockConnection;

  @Mock Statement mockStatement;

  @Mock ResultSet mockResultSet;

  @Test
  public void testCollationMapperBasic() {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();
    /* Initialize a basic case insensitive collation of english alphabet. Lower case characters map to upper case ones. */
    CollationMapper.Builder collationMapperBuilder =
        CollationMapper.builder(testCollationReference);
    ImmutableList.Builder<Character> enAlphabetsBuilder = ImmutableList.builder();
    for (Character c = 'a'; c <= 'z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Character c = 'A'; c <= 'Z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Character c : enAlphabetsBuilder.build()) {
      CollationOrderRow collationOrderRow =
          CollationOrderRow.builder()
              .setCharsetChar(c)
              .setEquivalentChar(Character.toUpperCase(c))
              .setEquivalentCharPadSpace(Character.toUpperCase(c))
              .setCodepointRank((long) (Character.toUpperCase(c) - 'A'))
              .setCodepointRankPadSpace((long) (Character.toUpperCase(c) - 'A'))
              .setIsEmpty(false)
              .setIsSpace(false)
              .build();
      collationMapperBuilder.addCharacter(collationOrderRow);
    }
    /** Add blank character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar('\0')
            .setEquivalentChar('\0')
            .setEquivalentCharPadSpace('\0')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(true)
            .setIsSpace(false)
            .build());
    CollationMapper collationMapper = collationMapperBuilder.build();

    assertThat(collationMapper.emptyCharacters().size()).isEqualTo(1);
    assertThat(collationMapper.allPositionsIndex().characterToIndex().size()).isEqualTo(52);
    assertThat(collationMapper.allPositionsIndex().indexToCharacter().size()).isEqualTo(26);
    assertThat(collationMapper.collationReference().dbCollation()).isEqualTo("testCollation");
    assertThat(collationMapper.collationReference().dbCharacterSet()).isEqualTo("testCharSet");
    assertThat(collationMapper.unMapString(collationMapper.mapString("google", 6)))
        .isEqualTo("GOOGLE");
    // midpoint of cat and mat is HAT.
    BigInteger catAsBigInt = collationMapper.mapString("cat", 3);
    BigInteger matAsBigInt = collationMapper.mapString("mat", 3);
    BigInteger average = (catAsBigInt.add(matAsBigInt)).divide(BigInteger.valueOf(2));
    String averageString = collationMapper.unMapString(average);
    assertThat(averageString).isEqualTo("HAT");
    // Test Blank Characters.
    assertThat(collationMapper.unMapString(collationMapper.mapString("goo" + '\0' + "gle", 6)))
        .isEqualTo("GOOGLE");
    // Test Padding.
    assertThat(collationMapper.unMapString(collationMapper.mapString("google", 7)))
        .isEqualTo("GOOGLEA");
  }

  @Test
  public void testCollationMapperPadSpace() {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();
    /* Initialize a basic case insensitive collation of english alphabet. Lower case characters map to upper case ones. */
    CollationMapper.Builder collationMapperBuilder =
        CollationMapper.builder(testCollationReference);
    ImmutableList.Builder<Character> enAlphabetsBuilder = ImmutableList.builder();
    for (Character c = 'a'; c <= 'z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Character c = 'A'; c <= 'Z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Character c : enAlphabetsBuilder.build()) {
      CollationOrderRow collationOrderRow =
          CollationOrderRow.builder()
              .setCharsetChar(c)
              .setEquivalentChar(Character.toUpperCase(c))
              .setEquivalentCharPadSpace(Character.toUpperCase(c))
              .setCodepointRank((long) (Character.toUpperCase(c) - 'A' + 1))
              .setCodepointRankPadSpace((long) (Character.toUpperCase(c) - 'A'))
              .setIsEmpty(false)
              .setIsSpace(false)
              .build();
      collationMapperBuilder.addCharacter(collationOrderRow);
    }
    /** Add Space Character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar(' ')
            .setEquivalentChar(' ')
            .setEquivalentCharPadSpace('\0')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(false)
            .setIsSpace(true)
            .build());

    CollationMapper collationMapper = collationMapperBuilder.build();
    assertThat(collationMapper.unMapString(collationMapper.mapString("google ", 6)))
        .isEqualTo("GOOGLE");
    // midpoint of cat and mat is HAT.
    BigInteger catAsBigInt = collationMapper.mapString("cat", 3);
    BigInteger matAsBigInt = collationMapper.mapString("mat", 3);
    BigInteger average = (catAsBigInt.add(matAsBigInt)).divide(BigInteger.valueOf(2));
    String averageString = collationMapper.unMapString(average);
    assertThat(averageString).isEqualTo("HAT");
  }

  @Test
  public void testCollationMapperEmptyStrings() {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();
    CollationMapper.Builder collationMapperBuilder =
        CollationMapper.builder(testCollationReference);

    /* Add space character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar(' ')
            .setEquivalentChar(' ')
            .setEquivalentCharPadSpace('\0')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(false)
            .setIsSpace(true)
            .build());
    /* Add empty Character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar('\0')
            .setEquivalentChar('\0')
            .setEquivalentCharPadSpace('\0')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(true)
            .setIsSpace(true)
            .build());
    CollationMapper collationMapper = collationMapperBuilder.build();
    assertThat(collationMapper.unMapString(collationMapper.mapString("", 0))).isEqualTo("");
    assertThat(collationMapper.unMapString(collationMapper.mapString("\0", 0))).isEqualTo("");
    assertThat(collationMapper.unMapString(collationMapper.mapString(null, 0))).isEqualTo("");
    assertThat(collationMapper.unMapString(collationMapper.mapString("    ", 0))).isEqualTo("");
  }

  @Test
  public void testCollationMapperSingleCharacterString() {

    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();
    CollationMapper.Builder collationMapperBuilder =
        CollationMapper.builder(testCollationReference);

    /* Add Single Character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar('a')
            .setEquivalentChar('a')
            .setEquivalentCharPadSpace('a')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(false)
            .setIsSpace(false)
            .build());
    CollationMapper collationMapper = collationMapperBuilder.build();
    assertThat(collationMapper.unMapString(collationMapper.mapString("a", 1))).isEqualTo("a");
  }

  @Test
  public void testCollationFromDbBasic() throws SQLException {

    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString(CHARSET_CHAR_COL)).thenReturn("a");
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_COL)).thenReturn("a");
    when(mockResultSet.getLong(CODEPOINT_RANK_COL)).thenReturn(0L);
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL)).thenReturn("a");
    when(mockResultSet.getLong(CODEPOINT_RANK_PAD_SPACE_COL)).thenReturn(0L);
    when(mockResultSet.getBoolean(IS_EMPTY_COL)).thenReturn(false);
    when(mockResultSet.getBoolean(IS_SPACE_COL)).thenReturn(false);

    CollationMapper collationMapper =
        CollationMapper.fromDB(
            mockConnection, new MysqlDialectAdapter(MySqlVersion.DEFAULT), testCollationReference);

    assertThat(collationMapper.allPositionsIndex().characterToIndex().size()).isEqualTo(1);
    assertThat(collationMapper.collationReference().dbCollation()).isEqualTo("testCollation");
    assertThat(collationMapper.collationReference().dbCharacterSet()).isEqualTo("testCharSet");
  }

  @Test
  public void testCollationFromDbSqlException() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharset")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();
    int expectedSqlExceptionsCount = 0;
    when(mockConnection.createStatement())
        .thenThrow(new SQLException("test"))
        .thenReturn(mockStatement);
    expectedSqlExceptionsCount++;
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenThrow(new SQLException()).thenReturn(mockResultSet);
    expectedSqlExceptionsCount++;
    when(mockResultSet.next()).thenThrow(new SQLException("test"));
    expectedSqlExceptionsCount++;
    for (int i = 0; i < expectedSqlExceptionsCount; i++) {
      assertThrows(
          SQLException.class,
          () ->
              CollationMapper.fromDB(
                  mockConnection,
                  new MysqlDialectAdapter(MySqlVersion.DEFAULT),
                  testCollationReference));
    }
  }

  @Test
  public void testCollationFromDbNoResultSetExceptionBasic() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharset")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(-1);
    assertThrows(
        RuntimeException.class,
        () ->
            CollationMapper.fromDB(
                mockConnection,
                new MysqlDialectAdapter(MySqlVersion.DEFAULT),
                testCollationReference));
  }

  @Test
  public void testCollationFromDbNoResultSetExceptionAfterExhaustedLoop() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharset")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    assertThrows(
        RuntimeException.class,
        () ->
            CollationMapper.fromDB(
                mockConnection,
                new MysqlDialectAdapter(MySqlVersion.DEFAULT),
                testCollationReference));
  }

  @Test
  public void testUtf8Mb40900AiCi() throws SQLException, IOException {

    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_ai_ci")
            .setPadSpace(false)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-0900-ai-ci.tsv", mockResultSet);
    CollationMapper collationMapper =
        CollationMapper.fromDB(
            mockConnection, new MysqlDialectAdapter(MySqlVersion.DEFAULT), collationReference);
    // All characters are mapped.
    assertThat(
            collationMapper.allPositionsIndex().characterToIndex().size()
                + collationMapper.emptyCharacters().size())
        .isEqualTo(numRows);
    assertThat(
            collationMapper.trailingPositionsPadSpace().characterToIndex().size()
                + collationMapper.emptyCharacters().size()
                + collationMapper.spaceCharacters().size())
        .isEqualTo(numRows);

    // The average of cat and mat for uf8mb4_0900_ai_ci won't be HAT!
    // utf8mb4 has a wide range of characters and the latin gamma
    // (https://en.wikipedia.org/wiki/Latin_gamma) sorts at the midpoint of C and H.
    // refer to the indexes in TestCollations/collation-output-mysql-utf8mb4-0900-ai-ci.tsv , or
    // just try
    // ` SELECT _utf8mb4'ƔAT' > _utf8mb4'cat' COLLATE utf8mb4_0900_ai_ci ` and ` SELECT
    // _utf8mb4'ƔAT' < _utf8mb4'hat' COLLATE utf8mb4_0900_ai_ci `
    // on any Mysql instance.
    BigInteger catAsBigInt = collationMapper.mapString("cat", 3);
    BigInteger matAsBigInt = collationMapper.mapString("mat", 3);
    BigInteger average = (catAsBigInt.add(matAsBigInt)).divide(BigInteger.valueOf(2));
    String averageString = collationMapper.unMapString(average);
    assertThat(averageString).isEqualTo("ƔAT");
    // Check insensitive comparisons with padding
    assertThat(collationMapper.unMapString(collationMapper.mapString("cát", 4))).isEqualTo("CAT\t");
    // Check that tab compares before space at all positions.
    assertThat(collationMapper.mapString("a\t", 1).compareTo(collationMapper.mapString("a ", 1)))
        .isLessThan(0);
    assertThat(collationMapper.mapString("a\ta", 1).compareTo(collationMapper.mapString("a a", 1)))
        .isLessThan(0);
  }

  @Test
  public void testUtf8Mb40900AsCs() throws SQLException, IOException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_as_cs")
            .setPadSpace(false)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-0900-as-cs.tsv", mockResultSet);
    CollationMapper collationMapper =
        CollationMapper.fromDB(
            mockConnection, new MysqlDialectAdapter(MySqlVersion.DEFAULT), collationReference);
    // All characters are mapped.
    assertThat(
            collationMapper.allPositionsIndex().characterToIndex().size()
                + collationMapper.emptyCharacters().size())
        .isEqualTo(numRows);
    assertThat(
            collationMapper.trailingPositionsPadSpace().characterToIndex().size()
                + collationMapper.emptyCharacters().size()
                + collationMapper.spaceCharacters().size())
        .isEqualTo(numRows);

    // Check case and accent sensitive comparisons with padding
    assertThat(collationMapper.unMapString(collationMapper.mapString("cát", 4))).isEqualTo("cát̲");
    // Check that tab compares before space at all positions.
    assertThat(collationMapper.mapString("a\t", 1).compareTo(collationMapper.mapString("a ", 1)))
        .isLessThan(0);
    assertThat(collationMapper.mapString("a\ta", 1).compareTo(collationMapper.mapString("a a", 1)))
        .isLessThan(0);
  }

  @Test
  public void testUtf8Mb4UnicodeCi() throws SQLException, IOException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(true)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-unicode-ci.tsv", mockResultSet);
    CollationMapper collationMapper =
        CollationMapper.fromDB(
            mockConnection, new MysqlDialectAdapter(MySqlVersion.DEFAULT), collationReference);
    // All characters are mapped.
    assertThat(
            collationMapper.allPositionsIndex().characterToIndex().size()
                + collationMapper.emptyCharacters().size())
        .isEqualTo(numRows);
    assertThat(
            collationMapper.trailingPositionsPadSpace().characterToIndex().size()
                + collationMapper.emptyCharacters().size()
                + collationMapper.spaceCharacters().size())
        .isEqualTo(numRows);

    // Check case and accent insensitive comparisons.
    assertThat(collationMapper.unMapString(collationMapper.mapString("cát", 3))).isEqualTo("CAT");
    // Check that tab compares before space at non-trailing positions.
    assertThat(collationMapper.mapString("a\ta", 1).compareTo(collationMapper.mapString("a a", 1)))
        .isLessThan(0);
    // Check that tab compares after space at non-trailing positions.
    assertThat(collationMapper.mapString("a\t", 1).compareTo(collationMapper.mapString("a ", 1)))
        .isGreaterThan(0);
    // Check that trailing spaces are ignored.
    assertThat(collationMapper.mapString("a", 1).equals(collationMapper.mapString("a ", 1)));
  }
}
