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

import com.google.auto.value.AutoValue;
import com.google.auto.value.extension.memoized.Memoized;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter.CollationQueryResultType;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationIndex.CollationIndexType;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Map characters of a string read from the database to a {@link BigInteger} based on the collation
 * ordering.
 *
 * <p><b>Basic Requirement for mapping</b>
 *
 * <p>Consider two strings read from the database - stringLeft and stringRight, and say {@link
 * CollationMapper#mapString(String, int)} maps them to bigIntLeft and bigIntRight. Consider
 * BigIntMean as the mean of BigIntLeft and BitIntRight, and stringSplit as the output of {@link
 * CollationMapper#unMapString(BigInteger)} for BigIntMean, then,
 *
 * <p>{@code SELECT StringLeft <= StringSplit} and {@code SELECT StringRight >= StringSplit} must
 * always hold true. Also split points of ranges of same column should never compare equal.
 */
@AutoValue
public abstract class CollationMapper implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(CollationMapper.class);

  /** Details about the collation. */
  public abstract CollationReference collationReference();

  /**
   * Map of character to it's index position based on collation order. Helps us map a string to big
   * integer for all positions. {@link #allPositionsIndex()} is the primarily referred index except
   * for the case of trailing position in a non-pad comparison.
   */
  public abstract CollationIndex allPositionsIndex();

  /**
   * Map of character to it's index position based on collation order. Helps us map a string to big
   * integer for trailing position in case a pad-space comparison is needed. Pad Space comparison is
   * needed in case of a PAD Space collation in MYSQL, or a CHAR column in PG.
   *
   * <p>{@link #trailingPositionsPadSpace()} is referred only for trailing position in a non-pad
   * comparison.
   *
   * @return
   */
  public abstract CollationIndex trailingPositionsPadSpace();

  /**
   * Empty Characters. MySQL ignores empty characters in comparisons. For example consider the below
   * query that adds a control-z between a and b. {@code SELECT CONCAT('a', CONVERT(UNHEX('001A')
   * using utf8mb4), 'b') = 'ab' COLLATE <collation>;} returns 1. TODO(vardhanvthigle): Check this
   * behavior for PG and other databases.
   */
  public abstract ImmutableSet<Integer> emptyCharacters();

  /**
   * Space Characters. MySQL ignores trailing space characters in comparisons for PAD space
   * collations and PG has the same behavior for CHAR columns. Note that there are various
   * codepoints that can potentially represent a space, like the ascii space or non-breaking space
   * (UNHEX(C2H0)) when the collation is Pad Space. These have same behavior to ascii space as far
   * as trailing or non-trailing comparison is concerned.
   */
  public abstract ImmutableSet<Integer> spaceCharacters();

  @Memoized
  String allSpaceCharacters() {
    return this.spaceCharacters().stream()
        .map(c -> new String(Character.toChars(c)))
        .collect(Collectors.joining(""));
  }

  @Memoized
  String emptyReplacePattern() {
    if (this.emptyCharacters().isEmpty()) {
      return "";
    }
    return "["
        + Pattern.quote(
            this.emptyCharacters().stream()
                .map(c -> new String(Character.toChars(c)))
                .collect(Collectors.joining("")))
        + "]";
  }

  /**
   * Map a {@link String} to {@link BigInteger}.
   *
   * @param element String.
   * @param lengthToPad maximum length of the string as per the column width.
   * @return mapped big integer.
   *     <p><b>Note:</b>
   *     <p>The logic for mapping th string has to take care of various database nuances like:
   *     <ul>
   *       <li>Control Characters like control-z are ignored for comparisons at all positions.
   *       <li>Space is ignored for comparison at trailing positions. Depending on the
   *           character-set, there could be more than one character that represents a space like <a
   *           href=https://www.compart.com/en/unicode/U+2001>EM quad</a>, <a
   *           href=https://www.compart.com/en/unicode/U+00A0#>non-breaking space</a>.
   *       <li>Space is not ignored at non-trailing positions in the comparison and characters like
   *           tab or new-line could compare less then space at non-trailing positions.
   *     </ul>
   */
  public BigInteger mapString(@Nullable String element, int lengthToPad) {

    if (element == null) {
      return BigInteger.valueOf(-1);
    }
    BigInteger ret = BigInteger.ZERO;

    // MySQL ignores empty character in string comparisons.
    // For example (adding control-z between a and b):
    // SELECT CONCAT('a', CONVERT(UNHEX('001A') using utf8mb4), 'b') = 'ab' COLLATE
    // utf8mb4_0900_ai_ci;
    // returns 1.
    // Remove all the empty characters.
    element = element.replaceAll(emptyReplacePattern(), "");

    // Remove trailing spaces for padSpace comparison.
    if (this.collationReference().padSpace()) {
      element = StringUtils.stripEnd(element, allSpaceCharacters());
    }
    if (element.isEmpty()) {
      return BigInteger.valueOf(-1);
    }

    // Convert the string to BigInteger.
    int length = element.codePointCount(0, element.length());
    int codePointIndex = 0;
    for (int index = 0; index < element.length(); ) {
      int codepoint = element.codePointAt(index);
      boolean isLast = (codePointIndex == length - 1);
      ret =
          ret.multiply(BigInteger.valueOf(getCharsetSize(isLast)))
              .add(BigInteger.valueOf(getOrdinalPosition(codepoint, isLast)));
      index += Character.charCount(codepoint);
      codePointIndex++;
    }
    for (int index = element.length(); index < lengthToPad; index++) {
      ret = ret.multiply(BigInteger.valueOf(getCharsetSize(index == (element.length() - 1))));
    }
    return ret;
  }

  /**
   * Unmap a {@link BigInteger} back to {@link String}.
   *
   * @param element BigInteger to unmap.
   * @return mapped String.
   *     <p><b>Note:</b>
   *     <p>The logic for mapping th string has to take care of various database nuances like:
   *     <ul>
   *       <li>Control Characters like control-z are ignored for comparisons at all positions.
   *       <li>Space is ignored for comparison at trailing positions. Depending on the
   *           character-set, there could be more than one character that represents a space like <a
   *           href=https://www.compart.com/en/unicode/U+2001>EM quad</a>, <a
   *           href=https://www.compart.com/en/unicode/U+00A0#>non-breaking space</a>.
   *       <li>Space is not ignored at non-trailing positions in the comparison and characters like
   *           tab or new-line could compare less then space at non-trailing positions.
   *     </ul>
   */
  public String unMapString(BigInteger element) {
    StringBuilder word = new StringBuilder();
    int index = 0;

    if (element.equals(BigInteger.valueOf(-1))) {
      return "";
    }

    // Base Case that the string just represents single character
    if (element.equals(BigInteger.ZERO)) {
      int c = getCharacterFromPosition(element.longValue(), true);
      return new String(Character.toChars(c));
    }

    while (!element.equals(BigInteger.ZERO)) {
      long charsetSize = getCharsetSize(index == 0);

      BigInteger reminder = element.mod(BigInteger.valueOf(charsetSize));
      int c = getCharacterFromPosition(reminder.longValue(), (index == 0));
      word.appendCodePoint(c);

      element = element.divide(BigInteger.valueOf(charsetSize));
      index++;
    }
    String ret = word.reverse().toString();
    return ret;
  }

  public static Builder builder(CollationReference collationReference) {
    Builder builder =
        new AutoValue_CollationMapper.Builder().setCollationReference(collationReference);

    builder
        .allPositionsIndexBuilder()
        .setIndexType(CollationIndexType.ALL_POSITIONS)
        .setCollationReference(collationReference);
    builder
        .trailingPositionsPadSpaceBuilder()
        .setIndexType(CollationIndexType.TRAILING_POSITION_PAD_SPACE)
        .setCollationReference(collationReference);
    return builder;
  }

  public static CollationMapper fromDB(
      Connection connection,
      UniformSplitterDBAdapter dbAdapter,
      CollationReference collationReference)
      throws SQLException {

    // 1. Attempt Java-side codepoint generation
    Optional<Charset> javaCharsetOpt =
        CharsetMapper.toJavaCharset(collationReference.dbCharacterSet());
    if (javaCharsetOpt.isPresent()) {
      // Limit of 10000 codepoints for Java-side processing to prevent large charset overheads.
      // If a custom charset is larger than 10000, we fall back to dynamic SQL.
      Optional<List<Integer>> codepointsOpt =
          CodepointGenerator.getValidCodepoints(javaCharsetOpt.get(), 10000);
      if (codepointsOpt.isPresent()) {
        List<Integer> codepoints = codepointsOpt.get();

        // 1a. MySQL Batch retrieval path
        if (dbAdapter.supportsBatchedWeightRetrieval()) {
          try {
            logger.info(
                "Running Java-driven batch weight retrieval for collation {}", collationReference);
            List<UniformSplitterDBAdapter.CharacterWeight> weights =
                dbAdapter.getWeights(connection, codepoints, collationReference.dbCollation());
            return fromWeightsCollection(weights, collationReference);
          } catch (Exception e) {
            logger.warn(
                "Java-driven batch weight retrieval failed for {}, falling back to SQL path",
                collationReference,
                e);
          }
        }

        // 1b. PostgreSQL Direct ranking path
        if (dbAdapter.supportsDirectRanking()) {
          try {
            logger.info("Running Java-driven direct ranking for collation {}", collationReference);
            List<UniformSplitterDBAdapter.CharacterRank> ranks =
                dbAdapter.getDirectRanks(connection, codepoints, collationReference.dbCollation());
            return fromRanksCollection(ranks, collationReference);
          } catch (Exception e) {
            logger.warn(
                "Java-driven direct ranking failed for {}, falling back to SQL path",
                collationReference,
                e);
          }
        }
      }
    }

    // 2. Fallback SQL-driven path
    logger.info("Using SQL-driven fallback query for collation {}", collationReference);
    int maxBytes = 3;
    try {
      maxBytes =
          Math.min(
              dbAdapter.getCharsetMaxLength(connection, collationReference.dbCharacterSet()), 3);
    } catch (Exception e) {
      logger.warn("Failed to query max character length, defaulting to 3 bytes", e);
    }

    String query =
        dbAdapter.getCollationsOrderQuery(
            collationReference.dbCharacterSet(),
            collationReference.dbCollation(),
            collationReference.padSpace(),
            maxBytes);

    CollationQueryResultType resultType = dbAdapter.collationQueryResultType();
    CollationMapper mapper = null;
    try (Statement statement = connection.createStatement()) {
      statement.setEscapeProcessing(false);
      boolean foundResultSet = statement.execute(query);
      for (int i = 0; i < query.lines().count() + 1; i++) {
        if (foundResultSet) {
          ResultSet rs = statement.getResultSet();
          if (resultType == CollationQueryResultType.WEIGHT_BYTES) {
            mapper = fromResultSetWithWeights(rs, collationReference);
          } else {
            mapper = fromResultSetWithRanks(rs, collationReference);
          }
          break;
        }
        foundResultSet = statement.getMoreResults();
        if (!foundResultSet && statement.getUpdateCount() == -1) {
          Preconditions.checkState(
              false, "No result sets found while querying collation for " + collationReference);
        }
      }
    } catch (SQLException e) {
      logger.error(
          "Exception while getting collation order for {}, exception = {}, query = {}",
          collationReference,
          e,
          query);
      throw e;
    }
    if (mapper == null) {
      Preconditions.checkState(
          false, "No result sets found while querying collation for " + collationReference);
    }
    return mapper;
  }

  private static CollationMapper fromWeightsCollection(
      List<UniformSplitterDBAdapter.CharacterWeight> rows, CollationReference collationReference) {

    Comparator<String> weightKeyOrder = Comparator.naturalOrder();

    // Phase 2a: build all-positions equivalence groups from weightNonTrailing.
    TreeMap<String, TreeMap<Integer, Integer>> ntGroups = new TreeMap<>(weightKeyOrder);
    for (UniformSplitterDBAdapter.CharacterWeight row : rows) {
      if (!row.isEmpty()) {
        byte[] wNt = row.weightNonTrailing();
        String keyNt = (wNt != null) ? new String(wNt, StandardCharsets.ISO_8859_1) : "";
        ntGroups.computeIfAbsent(keyNt, k -> new TreeMap<>()).put(row.codepoint(), row.codepoint());
      }
    }
    Map<Integer, Long> ntRank = new HashMap<>();
    Map<Integer, Integer> ntEquiv = new HashMap<>();
    long rank = 0;
    for (TreeMap<Integer, Integer> group : ntGroups.values()) {
      int equiv = group.firstEntry().getValue();
      for (Integer c : group.values()) {
        ntRank.put(c, rank);
        ntEquiv.put(c, equiv);
      }
      rank++;
    }

    // Phase 2b: build PAD-SPACE trailing equivalence groups from weightTrailing.
    TreeMap<String, TreeMap<Integer, Integer>> tGroups = new TreeMap<>(weightKeyOrder);
    for (UniformSplitterDBAdapter.CharacterWeight row : rows) {
      if (!row.isEmpty() && !row.isSpace()) {
        byte[] wT = row.weightTrailing();
        String keyT = (wT != null) ? new String(wT, StandardCharsets.ISO_8859_1) : "";
        tGroups.computeIfAbsent(keyT, k -> new TreeMap<>()).put(row.codepoint(), row.codepoint());
      }
    }
    Map<Integer, Long> tRank = new HashMap<>();
    Map<Integer, Integer> tEquiv = new HashMap<>();
    long tRankCounter = 0;
    for (TreeMap<Integer, Integer> group : tGroups.values()) {
      int equiv = group.firstEntry().getValue();
      for (Integer c : group.values()) {
        tRank.put(c, tRankCounter);
        tEquiv.put(c, equiv);
      }
      tRankCounter++;
    }

    Builder builder = builder(collationReference);
    for (UniformSplitterDBAdapter.CharacterWeight row : rows) {
      int equivChar = ntEquiv.getOrDefault(row.codepoint(), row.codepoint());
      long codepointRank = ntRank.getOrDefault(row.codepoint(), 0L);
      int equivCharPs = tEquiv.getOrDefault(row.codepoint(), row.codepoint());
      long codepointRankPs = tRank.getOrDefault(row.codepoint(), 0L);

      builder.addCharacter(
          CollationOrderRow.builder()
              .setCharsetChar(row.codepoint())
              .setEquivalentChar(equivChar)
              .setCodepointRank(codepointRank)
              .setEquivalentCharPadSpace(equivCharPs)
              .setCodepointRankPadSpace(codepointRankPs)
              .setIsEmpty(row.isEmpty())
              .setIsSpace(row.isSpace())
              .build());
    }
    return builder.build();
  }

  private static CollationMapper fromResultSetWithWeights(
      ResultSet rs, CollationReference collationReference) throws SQLException {
    List<UniformSplitterDBAdapter.CharacterWeight> list = new ArrayList<>();
    while (rs.next()) {
      String charsetChar = rs.getString(CollationsOrderQueryColumns.CHARSET_CHAR_COL);
      if (charsetChar == null || charsetChar.isEmpty()) {
        continue;
      }
      int c = charsetChar.codePointAt(0);
      Preconditions.checkArgument(
          charsetChar.length() == Character.charCount(c),
          "Expected single character from collation query, got: %s",
          charsetChar);
      byte[] wNt = rs.getBytes(CollationsOrderQueryColumns.WEIGHT_NON_TRAILING_COL);
      byte[] wT = rs.getBytes(CollationsOrderQueryColumns.WEIGHT_TRAILING_COL);
      boolean isEmpty = rs.getBoolean(CollationsOrderQueryColumns.IS_EMPTY_COL);
      boolean isSpace = rs.getBoolean(CollationsOrderQueryColumns.IS_SPACE_COL);

      if (wNt == null && !isEmpty) {
        logger.warn(
            "Skipping character codepoint={} for {} because weight_non_trailing is NULL",
            c,
            collationReference);
        continue;
      }
      list.add(new UniformSplitterDBAdapter.CharacterWeight(c, wNt, wT, isEmpty, isSpace));
    }
    return fromWeightsCollection(list, collationReference);
  }

  private static CollationMapper fromRanksCollection(
      List<UniformSplitterDBAdapter.CharacterRank> rows, CollationReference collationReference) {

    Map<Long, Integer> rankToMinCodepoint = new HashMap<>();
    Map<Long, Integer> rankPsToMinCodepoint = new HashMap<>();
    for (UniformSplitterDBAdapter.CharacterRank row : rows) {
      if (!row.isEmpty()) {
        rankToMinCodepoint.merge(row.rank(), row.codepoint(), Math::min);
        if (!row.isSpace()) {
          rankPsToMinCodepoint.merge(row.rankPadSpace(), row.codepoint(), Math::min);
        }
      }
    }

    Builder builder = builder(collationReference);
    for (UniformSplitterDBAdapter.CharacterRank row : rows) {
      int equivChar = row.isEmpty() ? row.codepoint() : rankToMinCodepoint.get(row.rank());
      int equivCharPs =
          (row.isEmpty() || row.isSpace())
              ? row.codepoint()
              : rankPsToMinCodepoint.get(row.rankPadSpace());

      builder.addCharacter(
          CollationOrderRow.builder()
              .setCharsetChar(row.codepoint())
              .setEquivalentChar(equivChar)
              .setCodepointRank(row.rank())
              .setEquivalentCharPadSpace(equivCharPs)
              .setCodepointRankPadSpace(row.rankPadSpace())
              .setIsEmpty(row.isEmpty())
              .setIsSpace(row.isSpace())
              .build());
    }
    return builder.build();
  }

  public static CollationMapper fromResultSetWithRanks(
      ResultSet rs, CollationReference collationReference) throws SQLException {
    List<UniformSplitterDBAdapter.CharacterRank> list = new ArrayList<>();
    while (rs.next()) {
      String charsetChar = rs.getString(CollationsOrderQueryColumns.CHARSET_CHAR_COL);
      if (charsetChar == null || charsetChar.isEmpty()) {
        continue;
      }
      int c = charsetChar.codePointAt(0);
      Preconditions.checkArgument(
          charsetChar.length() == Character.charCount(c),
          "Expected single character from collation query, got: %s",
          charsetChar);
      long rankVal = rs.getLong(CollationsOrderQueryColumns.CODEPOINT_RANK_COL);
      long rankPsVal = rs.getLong(CollationsOrderQueryColumns.CODEPOINT_RANK_PAD_SPACE_COL);
      boolean isEmpty = rs.getBoolean(CollationsOrderQueryColumns.IS_EMPTY_COL);
      boolean isSpace = rs.getBoolean(CollationsOrderQueryColumns.IS_SPACE_COL);
      list.add(new UniformSplitterDBAdapter.CharacterRank(c, rankVal, rankPsVal, isEmpty, isSpace));
    }
    return fromRanksCollection(list, collationReference);
  }

  private long getCharsetSize(boolean lastCharacter) {
    return (lastCharacter && collationReference().padSpace())
        ? this.trailingPositionsPadSpace().getCharsetSize()
        : this.allPositionsIndex().getCharsetSize();
  }

  private long getOrdinalPosition(Integer c, boolean lastCharacter) {
    return (lastCharacter && collationReference().padSpace())
        ? this.trailingPositionsPadSpace().getOrdinalPosition(c)
        : this.allPositionsIndex().getOrdinalPosition(c);
  }

  private Integer getCharacterFromPosition(long ordinalPosition, boolean firstIteration) {
    return (firstIteration && collationReference().padSpace())
        ? this.trailingPositionsPadSpace().getCharacterFromPosition(ordinalPosition)
        : this.allPositionsIndex().getCharacterFromPosition(ordinalPosition);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setCollationReference(CollationReference collationReference);

    abstract CollationReference collationReference();

    abstract CollationIndex.Builder allPositionsIndexBuilder();

    abstract CollationIndex.Builder trailingPositionsPadSpaceBuilder();

    abstract ImmutableSet.Builder<Integer> emptyCharactersBuilder();

    abstract ImmutableSet.Builder<Integer> spaceCharactersBuilder();

    public Builder addCharacter(CollationOrderRow collationOrderRow) {

      logger.debug(
          "Registering character order for {}, character-details = {}",
          collationReference(),
          collationOrderRow);
      if (collationOrderRow.isEmpty()) {
        emptyCharactersBuilder().add(collationOrderRow.charsetChar());
        return this;
      }
      if (collationOrderRow.isSpace()) {
        spaceCharactersBuilder().add(collationOrderRow.charsetChar());
      }
      allPositionsIndexBuilder()
          .addCharacter(
              collationOrderRow.charsetChar(),
              collationOrderRow.equivalentChar(),
              collationOrderRow.codepointRank());
      if (!collationOrderRow.isSpace()) {
        trailingPositionsPadSpaceBuilder()
            .addCharacter(
                collationOrderRow.charsetChar(),
                collationOrderRow.equivalentCharPadSpace(),
                collationOrderRow.codepointRankPadSpace());
      }
      return this;
    }

    abstract CollationMapper autoBuild();

    public CollationMapper build() {
      return autoBuild();
    }
  }
}
