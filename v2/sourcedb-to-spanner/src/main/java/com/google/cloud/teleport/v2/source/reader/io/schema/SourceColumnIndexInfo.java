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
import com.google.common.base.Preconditions;

@AutoValue
public abstract class SourceColumnIndexInfo {
  public abstract String columnName();

  public abstract boolean isPrimary();

  public abstract boolean isUnique();

  public abstract long cardinality();

  public abstract long ordinalPosition();

  public abstract String indexName();

  public abstract IndexType indexType();

  public static Builder builder() {
    return new AutoValue_SourceColumnIndexInfo.Builder();
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
    DATE_TIME,
    OTHER
  };
}
