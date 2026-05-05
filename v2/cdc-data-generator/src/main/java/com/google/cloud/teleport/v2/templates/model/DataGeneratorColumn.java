/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Represents a column in the data generator schema. */
@AutoValue
public abstract class DataGeneratorColumn implements Serializable {

  /** The name of the column. */
  public abstract String name();

  /** The logical type of the column for data generation. */
  public abstract LogicalType logicalType();

  /** Whether the column is nullable. */
  public abstract boolean isNullable();

  /** Whether the column is a primary key. */
  public abstract boolean isPrimaryKey();

  /** Whether the column is skipped. */
  public abstract boolean isSkipped();

  /** Whether the column is a generated column. */
  public abstract boolean isGenerated();

  /** The size/length of the column (e.g., for strings or bytes). */
  @Nullable
  public abstract Long size();

  /** The precision of the column (for numeric types). */
  @Nullable
  public abstract Integer precision();

  /** The scale of the column (for numeric types). */
  @Nullable
  public abstract Integer scale();

  /** The custom generator for this column (e.g., Faker expression). */
  @Nullable
  public abstract Object fakerExpression();

  public static Builder builder() {
    return new AutoValue_DataGeneratorColumn.Builder().isSkipped(false).isPrimaryKey(false);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public abstract Builder logicalType(LogicalType logicalType);

    public abstract Builder isNullable(boolean isNullable);

    public abstract Builder isSkipped(boolean isSkipped);

    public abstract Builder isGenerated(boolean isGenerated);

    public abstract Builder size(Long size);

    public abstract Builder precision(Integer precision);

    public abstract Builder scale(Integer scale);

    public abstract Builder fakerExpression(Object fakerExpression);

    public abstract Builder isPrimaryKey(boolean isPrimaryKey);

    public abstract DataGeneratorColumn build();
  }
}
