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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundaryExtractorFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import javax.annotation.Nullable;

@AutoValue
/**
 * Information About Source Column Indexes. Each index is identified with a unique Index Name. When
 * a list of {@llink SourceColumnIndexInfo} is discovered, a composite index will have multiple
 * columns associated with the same indexName with unique ordinal positions.
 */
public abstract class SourceColumnIndexInfo implements Comparable<SourceColumnIndexInfo> {

  /**
   * @return name of the column.
   */
  public abstract String columnName();

  /**
   * Whether this index is primary. Note: for primary index, the isUniqe will always be true, and
   * index name will typically be "PRIMARY".
   *
   * @return true if the index is primary.
   */
  public abstract boolean isPrimary();

  /**
   * @return true if the index is unique.
   */
  public abstract boolean isUnique();

  /**
   * @return approximate cardinality of the index.
   */
  public abstract long cardinality();

  /**
   * @return ordinal position of {@link SourceColumnIndexInfo#columnName()} within the {@link
   *     SourceColumnIndexInfo#indexName()}.
   */
  public abstract long ordinalPosition();

  /**
   * @return name of the index.
   */
  public abstract String indexName();

  /**
   * A general classification of this index column's data type for choosing right {@link
   * org.apache.beam.sdk.values.TypeDescriptor} for {@link org.apache.beam.sdk.io.jdbc.JdbcIO}.
   *
   * @return index type.
   */
  public abstract IndexType indexType();

  /** Collation details for string columns. Null if the column is not of string type. */
  @Nullable
  public abstract CollationReference collationReference();

  /** Maximum Length for String Columns. Null for other types. */
  @Nullable
  public abstract Integer stringMaxLength();

  /**
   * Builder for {@link SourceColumnIndexInfo}.
   *
   * @return builder.
   */
  public static Builder builder() {
    return new AutoValue_SourceColumnIndexInfo.Builder();
  }

  @Override
  public int compareTo(SourceColumnIndexInfo other) {
    if (this.equals(other)) {
      return 0;
    }
    int nameCompare = this.indexName().compareTo(other.indexName());
    if (nameCompare != 0) {
      return nameCompare;
    }
    // Within the same index, check the ordinal position comparison.
    int ordinalCompare =
        new Long(this.ordinalPosition()).compareTo(new Long(other.ordinalPosition()));
    return ordinalCompare;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setColumnName(String value);

    public abstract Builder setIsPrimary(boolean value);

    public abstract Builder setIsUnique(boolean value);

    public abstract Builder setCardinality(long value);

    public abstract Builder setOrdinalPosition(long value);

    public abstract Builder setIndexName(String value);

    public abstract Builder setIndexType(IndexType value);

    public abstract Builder setCollationReference(CollationReference value);

    public abstract Builder setStringMaxLength(@Nullable Integer value);

    abstract SourceColumnIndexInfo autoBuild();

    public SourceColumnIndexInfo build() {
      SourceColumnIndexInfo indexInfo = autoBuild();
      Preconditions.checkState(
          (indexInfo.isPrimary()) ? indexInfo.isUnique() : true, "Primary Index must be unique.");
      return indexInfo;
    }
  }

  public enum IndexType {
    NUMERIC,
    BIG_INT_UNSIGNED,
    BINARY,
    STRING,
    DATE_TIME,
    OTHER
  };

  // TODO(vardhanvthigle): handle other types
  public static final ImmutableMap<IndexType, Class> INDEX_TYPE_TO_CLASS =
      ImmutableMap.of(
          IndexType.NUMERIC, Long.class,
          IndexType.STRING, String.class,
          IndexType.BIG_INT_UNSIGNED, BigDecimal.class,
          IndexType.BINARY, BoundaryExtractorFactory.BYTE_ARRAY_CLASS);
}
