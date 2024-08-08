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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Details about a partition column. */
@AutoValue
public abstract class PartitionColumn implements Serializable {

  /**
   * @return name of the column.
   */
  public abstract String columnName();

  /**
   * @return class of the column.
   */
  public abstract Class columnClass();

  /**
   * String Collation. Must be set for if {@link PartitionColumn#columnClass()} is {@link String}
   * and must not be set otherwise. Defaults to null.
   *
   * @return string collation for this column if it's a string column. Null otherwise.
   */
  @Nullable
  public abstract CollationReference stringCollation();

  /** Max Length of a string column. Defaults to null for non-string columns. */
  @Nullable
  public abstract Integer stringMaxLength();

  public static Builder builder() {
    return new AutoValue_PartitionColumn.Builder()
        .setStringCollation(null)
        .setStringMaxLength(null);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setColumnName(String value);

    public abstract Builder setColumnClass(Class value);

    public abstract Builder setStringCollation(CollationReference value);

    public abstract Builder setStringMaxLength(Integer value);

    abstract PartitionColumn autoBuild();

    public PartitionColumn build() {
      PartitionColumn partitionColumn = this.autoBuild();
      Preconditions.checkState(
          (partitionColumn.columnClass() == String.class
                  && partitionColumn.stringCollation() != null
                  && partitionColumn.stringMaxLength() != null)
              || (partitionColumn.columnClass() != String.class
                  && partitionColumn.stringCollation() == null
                  && partitionColumn.stringMaxLength() == null),
          "String columns must specify collation, and non string columns must not specify colaltion. PartitionColum = "
              + partitionColumn);
      return partitionColumn;
    }
  }
}
