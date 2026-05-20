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
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_EMPTY_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_SPACE_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.WEIGHT_NON_TRAILING_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.WEIGHT_TRAILING_COL;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter.CharacterRank;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  @Mock UniformSplitterDBAdapter mockDbAdapter;

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
    ImmutableList.Builder<Integer> enAlphabetsBuilder = ImmutableList.builder();
    for (int c = 'a'; c <= 'z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Integer c : enAlphabetsBuilder.build()) {
      CollationOrderRow collationOrderRow =
          CollationOrderRow.builder()
              .setCharsetChar(c)
              .setEquivalentChar((int) Character.toUpperCase(c))
              .setEquivalentCharPadSpace((int) Character.toUpperCase(c))
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
            .setCharsetChar((int) '\0')
            .setEquivalentChar((int) '\0')
            .setEquivalentCharPadSpace((int) '\0')
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
    ImmutableList.Builder<Integer> enAlphabetsBuilder = ImmutableList.builder();
    for (int c = 'a'; c <= 'z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      enAlphabetsBuilder.add(c);
    }
    for (Integer c : enAlphabetsBuilder.build()) {
      CollationOrderRow collationOrderRow =
          CollationOrderRow.builder()
              .setCharsetChar(c)
              .setEquivalentChar((int) Character.toUpperCase(c))
              .setEquivalentCharPadSpace((int) Character.toUpperCase(c))
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
            .setCharsetChar((int) ' ')
            .setEquivalentChar((int) ' ')
            .setEquivalentCharPadSpace((int) '\0')
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
            .setCharsetChar((int) ' ')
            .setEquivalentChar((int) ' ')
            .setEquivalentCharPadSpace((int) '\0')
            .setCodepointRank(0L)
            .setCodepointRankPadSpace(0L)
            .setIsEmpty(false)
            .setIsSpace(true)
            .build());
    /* Add empty Character */
    collationMapperBuilder.addCharacter(
        CollationOrderRow.builder()
            .setCharsetChar((int) '\0')
            .setEquivalentChar((int) '\0')
            .setEquivalentCharPadSpace((int) '\0')
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
            .setCharsetChar((int) 'a')
            .setEquivalentChar((int) 'a')
            .setEquivalentCharPadSpace((int) 'a')
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
    // The MySQL adapter now uses CollationQueryResultType.WEIGHT_BYTES: the result set returns
    // raw WEIGHT_STRING sort-key bytes rather than pre-computed ranks and equivalent chars.
    // Java (CollationMapper.fromResultSetWithWeights) does all grouping, ranking and
    // equivalent-character resolution.
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
    // weight bytes: a single non-zero byte so the character is ordered at rank 0.
    byte[] weightBytes = new byte[] {0x01, 0x00};
    when(mockResultSet.getBytes(WEIGHT_NON_TRAILING_COL)).thenReturn(weightBytes);
    when(mockResultSet.getBytes(WEIGHT_TRAILING_COL)).thenReturn(weightBytes);
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
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_ai_ci")
            .setPadSpace(false)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-0900-ai-ci.tsv", mockResultSet);
    CollationMapper collationMapper = fromResultSetWithRanks(mockResultSet, collationReference);
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
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_0900_as_cs")
            .setPadSpace(false)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-0900-as-cs.tsv", mockResultSet);
    CollationMapper collationMapper = fromResultSetWithRanks(mockResultSet, collationReference);
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
    CollationReference collationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(true)
            .build();
    int numRows =
        TestUtils.wireMockResultSet(
            "TestCollations/collation-output-mysql-utf8mb4-unicode-ci.tsv", mockResultSet);
    CollationMapper collationMapper = fromResultSetWithRanks(mockResultSet, collationReference);
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

  @Test
  public void testCollationFromDb_MysqlJavaDriven_Success() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("latin1")
            .setDbCollation("latin1_swedish_ci")
            .setPadSpace(false)
            .build();

    when(mockDbAdapter.supportsRanksRetrieval()).thenReturn(true);

    List<CharacterRank> ranks = new ArrayList<>();
    // Add ranks for Latin-1 codepoints (0 to 255)
    for (int cp = 0; cp < 256; cp++) {
      long rank = cp;
      ranks.add(new CharacterRank(cp, rank, rank, false, false));
    }
    when(mockDbAdapter.getRanks(any(), any(), any())).thenReturn(ranks);

    CollationMapper collationMapper =
        CollationMapper.fromDB(mockConnection, mockDbAdapter, testCollationReference);

    assertThat(collationMapper.allPositionsIndex().characterToIndex().size()).isEqualTo(256);
    assertThat(collationMapper.collationReference().dbCollation()).isEqualTo("latin1_swedish_ci");
  }

  @Test
  public void testCollationFromDb_PostgresJavaDriven_Success() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("latin1")
            .setDbCollation("en_US.utf8")
            .setPadSpace(false)
            .build();

    when(mockDbAdapter.supportsRanksRetrieval()).thenReturn(true);

    Map<Integer, Integer> cpToEquiv = new HashMap<>();
    for (int cp = 0; cp < 256; cp++) {
      int equiv = (cp >= 'a' && cp <= 'z') ? (cp - 32) : cp;
      cpToEquiv.put(cp, equiv);
    }
    List<Integer> uniqueEquivs =
        cpToEquiv.values().stream().distinct().sorted().collect(Collectors.toList());
    Map<Integer, Long> equivToRank = new HashMap<>();
    for (int i = 0; i < uniqueEquivs.size(); i++) {
      equivToRank.put(uniqueEquivs.get(i), (long) i);
    }

    List<Integer> uniqueEquivsPs =
        cpToEquiv.entrySet().stream()
            .filter(entry -> entry.getKey() != ' ')
            .map(Map.Entry::getValue)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    Map<Integer, Long> equivToRankPs = new HashMap<>();
    for (int i = 0; i < uniqueEquivsPs.size(); i++) {
      equivToRankPs.put(uniqueEquivsPs.get(i), (long) i);
    }

    List<CharacterRank> ranks = new ArrayList<>();
    for (int cp = 0; cp < 256; cp++) {
      int equiv = cpToEquiv.get(cp);
      long rank = equivToRank.get(equiv);
      long rankPs = (cp == ' ') ? 0L : equivToRankPs.get(equiv);
      ranks.add(new CharacterRank(cp, rank, rankPs, false, cp == ' '));
    }
    when(mockDbAdapter.getRanks(any(), any(), any())).thenReturn(ranks);

    CollationMapper collationMapper =
        CollationMapper.fromDB(mockConnection, mockDbAdapter, testCollationReference);

    assertThat(collationMapper.allPositionsIndex().characterToIndex().size()).isEqualTo(256);
    assertThat(collationMapper.collationReference().dbCollation()).isEqualTo("en_US.utf8");
  }

  @Test
  public void testCollationFromDb_MysqlJavaDriven_FallbackToSql() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("latin1")
            .setDbCollation("latin1_swedish_ci")
            .setPadSpace(false)
            .build();

    when(mockDbAdapter.supportsRanksRetrieval()).thenReturn(true);
    when(mockDbAdapter.getRanks(any(), any(), any()))
        .thenThrow(new SQLException("Mocked connection failure"));

    // Fallback mock settings
    when(mockDbAdapter.getCharsetMaxLength(any(), any())).thenReturn(1);
    when(mockDbAdapter.getCollationsOrderQuery(anyString(), anyString(), anyBoolean(), anyInt()))
        .thenReturn("SELECT 1");
    when(mockDbAdapter.collationQueryResultType())
        .thenReturn(UniformSplitterDBAdapter.CollationQueryResultType.WEIGHT_BYTES);

    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(true);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);

    List<CharacterRank> ranks = new java.util.ArrayList<>();
    ranks.add(new CharacterRank('a', 0L, 0L, false, false));
    when(mockDbAdapter.processCollationResultSet(any(), any())).thenReturn(ranks);

    CollationMapper collationMapper =
        CollationMapper.fromDB(mockConnection, mockDbAdapter, testCollationReference);

    assertThat(collationMapper.allPositionsIndex().characterToIndex().size()).isEqualTo(1);
    assertThat(collationMapper.collationReference().dbCollation()).isEqualTo("latin1_swedish_ci");
  }

  @Test
  public void testCollationFromDb_FallbackSqlCappedTo3Bytes() throws SQLException {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(false)
            .build();

    // Force fallback by disabling ranks retrieval support
    when(mockDbAdapter.supportsRanksRetrieval()).thenReturn(false);

    // Mock 4-byte charset length, but we expect it to be capped at 3
    when(mockDbAdapter.getCharsetMaxLength(any(), eq("utf8mb4"))).thenReturn(4);
    when(mockDbAdapter.getCollationsOrderQuery(anyString(), anyString(), anyBoolean(), anyInt()))
        .thenReturn("SELECT 1");
    when(mockDbAdapter.collationQueryResultType())
        .thenReturn(UniformSplitterDBAdapter.CollationQueryResultType.WEIGHT_BYTES);

    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(true);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);

    List<CharacterRank> ranks = new java.util.ArrayList<>();
    ranks.add(new CharacterRank('a', 0L, 0L, false, false));
    when(mockDbAdapter.processCollationResultSet(any(), any())).thenReturn(ranks);

    CollationMapper.fromDB(mockConnection, mockDbAdapter, testCollationReference);

    // Verify that maxBytes is capped at 3 when querying the database adapter
    verify(mockDbAdapter)
        .getCollationsOrderQuery(eq("utf8mb4"), eq("utf8mb4_unicode_ci"), eq(false), eq(3));
  }

  private static CollationMapper fromResultSetWithRanks(
      ResultSet rs, CollationReference collationReference) throws SQLException {
    List<UniformSplitterDBAdapter.CharacterRank> list = new ArrayList<>();
    while (rs.next()) {
      String charsetChar = rs.getString(CHARSET_CHAR_COL);
      if (charsetChar == null || charsetChar.isEmpty()) {
        continue;
      }
      int c = charsetChar.codePointAt(0);
      com.google.common.base.Preconditions.checkArgument(
          charsetChar.length() == Character.charCount(c),
          "Expected single character from collation query, got: %s",
          charsetChar);
      long rankVal = rs.getLong(CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_COL);
      long rankPsVal =
          rs.getLong(CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_PAD_SPACE_COL);
      boolean isEmpty = rs.getBoolean(IS_EMPTY_COL);
      boolean isSpace = rs.getBoolean(IS_SPACE_COL);
      list.add(new UniformSplitterDBAdapter.CharacterRank(c, rankVal, rankPsVal, isEmpty, isSpace));
    }
    return CollationMapper.fromRanksCollection(list, collationReference);
  }
}
