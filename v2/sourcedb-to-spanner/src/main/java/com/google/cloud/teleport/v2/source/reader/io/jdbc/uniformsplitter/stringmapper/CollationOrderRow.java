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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a row of the minimum required columns from the collations output query. You can refer
 * to src/test/resources/TestCollations/collation-output-mysql-utf8mb4-0900-ai-ci.tsv for example of
 * the columns.
 */
@AutoValue
public abstract class CollationOrderRow {

  private static final Logger logger = LoggerFactory.getLogger(CollationOrderRow.class);

  /** Character in the character set. */
  public abstract Character charsetChar();

  /** A character with lowest rank charset_char character is equal to as per the collation. */
  public abstract Character equivalentChar();

  /** 0 offset rank of this character as per the collation sort ordering at all positions. */
  public abstract Long codepointRank();

  /**
   * A character with lowest rank charset_char character is equal to as per the collation at
   * trailing position, in case a PAD SPACE comparison is needed. Unless you are looking at space
   * like characters, this will be exactly same as equivalent_character.
   */
  public abstract Character equivalentCharPadSpace();

  /**
   * A character with lowest rank charset_char character is equal to as per the collation at
   * trailing position, in case a PAD SPACE comparison is needed. Unless you are looking at space
   * like characters, this will be exactly same as equivalent_character.
   */
  public abstract Long codepointRankPadSpace();

  /**
   * A boolean columns, true if the character is equal to '\0' at all positions of comparison. False
   * otherwise.
   */
  public abstract Boolean isEmpty();

  /**
   * A boolean columns, true if the character is equal to ' ' at all positions of comparison. False
   * otherwise.
   */
  public abstract Boolean isSpace();

  public static Builder builder() {
    return new AutoValue_CollationOrderRow.Builder();
  }

  public abstract Builder toBuilder();

  /**
   * Construct a {@link CollationOrderRow} from a result set for the collation order query. It is
   * expected that the caller handlers iteration of resultSet via {@link ResultSet#next()} and
   * exceptions if any.
   *
   * @param rs
   * @return fields of the output enclosed in {@link CollationOrderRow}.
   * @throws SQLException if thrown by the {@link ResultSet ResultSet api}.
   */
  public static CollationOrderRow fromRS(ResultSet rs) throws SQLException {

    String charSetChar = rs.getString(CHARSET_CHAR_COL);
    String equivalentCharsetChar = rs.getString(EQUIVALENT_CHARSET_CHAR_COL);
    Long codePointRank = rs.getLong(CODEPOINT_RANK_COL);
    Boolean isEmpty = rs.getBoolean(IS_EMPTY_COL);
    Boolean isSpace = rs.getBoolean(IS_SPACE_COL);
    String equivalentCharsetCharPadSpace = rs.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL);
    Long codePointRankPadSpace = rs.getLong(CODEPOINT_RANK_PAD_SPACE_COL);

    logger.debug(
        "Going to register collation query row charSetChar = {} with length = {}, equivalentCharSetChar = {} with length - {}, codePointRank = {}, isEmpty = {} isSpace = {}",
        charSetChar,
        charSetChar.length(),
        equivalentCharsetChar,
        equivalentCharsetChar.length(),
        codePointRank,
        isEmpty,
        isSpace);

    Preconditions.checkArgument(
        charSetChar.length() <= 1, "Found a long character in collation output " + charSetChar);
    Preconditions.checkArgument(
        equivalentCharsetChar.length() <= 1,
        "Found a long equivalent character in collation output " + equivalentCharsetChar);
    Preconditions.checkArgument(
        equivalentCharsetCharPadSpace.length() <= 1,
        "Found a long equivalent character for pad space in collation output "
            + equivalentCharsetChar);

    return CollationOrderRow.builder()
        .setCharsetChar(charSetChar.charAt(0))
        .setEquivalentChar(equivalentCharsetChar.charAt(0))
        .setCodepointRank(codePointRank)
        .setEquivalentCharPadSpace(equivalentCharsetCharPadSpace.charAt(0))
        .setCodepointRankPadSpace(codePointRankPadSpace)
        .setIsEmpty(isEmpty)
        .setIsSpace(isSpace)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCharsetChar(Character value);

    public abstract Builder setEquivalentChar(Character value);

    public abstract Builder setCodepointRank(Long value);

    public abstract Builder setEquivalentCharPadSpace(Character value);

    public abstract Builder setCodepointRankPadSpace(Long value);

    public abstract Builder setIsEmpty(Boolean value);

    public abstract Builder setIsSpace(Boolean value);

    public abstract CollationOrderRow build();
  }

  /** Column names that must be returned by the collations order query. */
  public static class CollationsOrderQueryColumns {

    /** Character in the character set. */
    public static final String CHARSET_CHAR_COL = "charset_char";

    /** A character with lowest rank charset_char character is equal to as per the collation. */
    public static final String EQUIVALENT_CHARSET_CHAR_COL = "equivalent_charset_char";

    /**
     * A boolean columns, true if the character is equal to '\0' at all positions of comparison.
     * False otherwise.
     */
    public static final String IS_EMPTY_COL = "is_empty";

    /**
     * A boolean columns, true if the character is equal to ' ' at all positions of comparison.
     * False otherwise.
     */
    public static final String IS_SPACE_COL = "is_space";

    /** 0 offset rank of this character as per the collation sort ordering at all positions. */
    public static final String CODEPOINT_RANK_COL = "codepoint_rank";

    /**
     * A character with lowest rank charset_char character is equal to as per the collation at
     * trailing position, in case a PAD SPACE comparison is needed. Unless you are looking at space
     * like characters, this will be exactly same as equivalent_character.
     */
    public static final String EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL =
        "equivalent_charset_char_pad_space";

    /**
     * 0 offset rank of this character as per the collation sort ordering at trailing position in
     * case a PAD SPACE comparison is needed.
     */
    public static final String CODEPOINT_RANK_PAD_SPACE_COL = "codepoint_rank_pad_space";

    private CollationsOrderQueryColumns() {}
  }
}
