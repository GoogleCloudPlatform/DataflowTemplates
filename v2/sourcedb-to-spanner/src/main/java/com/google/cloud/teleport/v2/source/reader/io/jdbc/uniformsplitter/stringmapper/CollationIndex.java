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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains maps from characters to their positions as per the collation order and a reverse map to
 * map a position to back to a character.
 */
@AutoValue
public abstract class CollationIndex implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(CollationIndex.class);

  /** Details about the collation. */
  public abstract CollationReference collationReference();

  /** Index type. */
  public abstract CollationIndexType indexType();

  /**
   * Map of character to it's index position based on collation order. Helps us map a string to big
   * integer.
   */
  public abstract ImmutableMap<Character, Long> characterToIndex();

  /**
   * Map if Index back to character based on collation order. Helps us unmap a big integer to
   * string. Note this maps the index back to a minimum set of characters. For example in
   * case-insensitive collations, 'a' and 'A' will have the same index in {@link
   * #characterToIndex()} and {@link #indexToCharacter()} will map the index to 'A'.
   */
  public abstract ImmutableMap<Long, Character> indexToCharacter();

  public static CollationIndex.Builder builder() {
    return new AutoValue_CollationIndex.Builder();
  }

  public long getCharsetSize() {
    return indexToCharacter().size();
  }

  public long getOrdinalPosition(Character c) {
    return characterToIndex().get(c);
  }

  public Character getCharacterFromPosition(Long position) {
    return indexToCharacter().get(position);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCollationReference(CollationReference value);

    abstract CollationReference collationReference();

    public abstract Builder setIndexType(CollationIndexType value);

    abstract CollationIndexType indexType();

    private Map<Character, Long> charToIndexCache = new HashMap<>();
    private Map<Long, Character> indexToCharacterCache = new HashMap<>();
    private Map<Character, Long> indexToCharacterReverseCache = new HashMap<>();

    abstract Builder setIndexToCharacter(ImmutableMap<Long, Character> value);

    abstract Builder setCharacterToIndex(ImmutableMap<Character, Long> value);

    public Builder addCharacter(Character charsetChar, Character equivalentChar, Long index) {
      logger.debug(
          "Registering character order for {}, index-type = {}, character = {}, equivalentCharacter = {}, index = {}, isBlank = {}",
          collationReference(),
          indexType(),
          charsetChar,
          equivalentChar,
          index);

      if (this.indexToCharacterCache.containsKey(index)
          || this.indexToCharacterReverseCache.containsKey(equivalentChar)) {
        if (!(this.indexToCharacterCache.containsKey(index)
                && this.indexToCharacterCache.get(index).equals(equivalentChar))
            || !(this.indexToCharacterReverseCache.containsKey(equivalentChar)
                && this.indexToCharacterReverseCache.get(equivalentChar).equals(index))) {
          throw new IllegalStateException(
              "Duplicate Equivalent characters share same index. Please check the collation order query. "
                  + "passed index: "
                  + index
                  + " passed equivalent char: "
                  + equivalentChar
                  + " existing equivalent char: "
                  + (this.indexToCharacterCache.containsKey(index)
                      ? this.indexToCharacterCache.get(index)
                      : "not-present")
                  + " existing index: "
                  + (this.indexToCharacterReverseCache.containsKey(equivalentChar)
                      ? this.indexToCharacterReverseCache.get(equivalentChar)
                      : "not-present")
                  + " passed charsetChar: "
                  + charsetChar
                  + " for "
                  + collationReference()
                  + "index-type = "
                  + indexType());
        }
      } else {
        this.indexToCharacterCache.put(index, equivalentChar);
        this.indexToCharacterReverseCache.put(equivalentChar, index);
      }
      if (this.charToIndexCache.containsKey(charsetChar)) {
        throw new IllegalStateException(
            "Duplicate Charset characters added. Please check the collation order query. "
                + "new index: "
                + index
                + " existing idx: "
                + this.charToIndexCache.get(charsetChar)
                + " equivalent char: "
                + equivalentChar
                + " charsetChar: "
                + charsetChar
                + " for "
                + collationReference()
                + "index-type = "
                + indexType());
      }
      this.charToIndexCache.put(charsetChar, index);
      return this;
    }

    abstract CollationIndex autoBuild();

    public CollationIndex build() {
      this.setIndexToCharacter(ImmutableMap.copyOf(this.indexToCharacterCache));
      this.setCharacterToIndex(ImmutableMap.copyOf(this.charToIndexCache));

      ImmutableList<Long> indexes =
          this.indexToCharacterCache.keySet().stream()
              .sorted()
              .collect(ImmutableList.toImmutableList());
      for (int i = 0; i < indexes.size(); i++) {
        if (indexes.get(i) != i) {
          throw new IllegalStateException(
              "Discontinuous index found at position "
                  + i
                  + " please check the collation order query"
                  + " for "
                  + collationReference()
                  + " for character set = "
                  + "index-type = "
                  + indexType());
        }
        if (!charToIndexCache.containsKey(indexToCharacterCache.get(indexes.get(i)))) {
          throw new IllegalStateException(
              "index which is not part of the character set found at position "
                  + i
                  + " index is "
                  + indexes.get(i)
                  + " index character is "
                  + indexToCharacterCache.get(indexes.get(i))
                  + " please check the collation order query"
                  + " for "
                  + collationReference()
                  + " for character set = "
                  + "index-type = "
                  + indexType());
        }
        if (charToIndexCache.get(indexToCharacterCache.get(indexes.get(i))) != indexes.get(i)) {
          throw new IllegalStateException(
              "index not mapping onto itself found at position "
                  + i
                  + " index is "
                  + indexes.get(i)
                  + " index character is "
                  + indexToCharacterCache.get(indexes.get(i))
                  + " index character is mapped to "
                  + charToIndexCache.get(indexToCharacterCache.get(indexes.get(i)))
                  + " please check the collation order query"
                  + " for "
                  + collationReference()
                  + " for character set = "
                  + "index-type = "
                  + indexType());
        }
      }
      CollationIndex index = autoBuild();
      logger.info(
          "Initialized Index {} for {}, with {} characters, {} unique characters, and {} empty characters",
          index.indexType(),
          index.collationReference(),
          index.characterToIndex().size(),
          index.indexToCharacter().size());
      return index;
    }
  }

  public enum CollationIndexType {
    /**
     * An index that tracks the collation ordering for all positions, except for the trailing
     * position for the specific case of a PAD SPACE comparison.
     */
    ALL_POSITIONS,
    /**
     * An index that tracks the collation ordering for trailing position in case of a PAD SPACE
     * comparison.
     */
    TRAILING_POSITION_PAD_SPACE
  }
}
