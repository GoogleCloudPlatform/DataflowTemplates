/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static com.google.cloud.teleport.spanner.common.NameUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner sequence. */
@AutoValue
public abstract class Sequence implements Serializable {

  private static final long serialVersionUID = 1L;
  public static final long SEQUENCE_COUNTER_BUFFER = 1000L;
  public static final String SEQUENCE_START_WITH_COUNTER = "start_with_counter";
  public static final String SEQUENCE_KIND = "sequence_kind";

  public abstract String name();

  @Nullable
  public abstract ImmutableList<String> options();

  // For PostgreSQL dialect only. These fields are stored in
  // `options` in GoogleSQL dialect.
  @Nullable
  public abstract String sequenceKind();

  @Nullable
  public abstract Long counterStartValue();

  @Nullable
  public abstract Long skipRangeMin();

  @Nullable
  public abstract Long skipRangeMax();

  public abstract Dialect dialect();

  public abstract Builder toBuilder();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_Sequence.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL && dialect() != Dialect.POSTGRESQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }
    appendable.append("CREATE SEQUENCE ").append(quoteIdentifier(name(), dialect()));

    if (dialect() == Dialect.GOOGLE_STANDARD_SQL && (options() != null && !options().isEmpty())) {
      String optionsString = String.join(", ", options());
      appendable.append("\n\tOPTIONS (").append(optionsString).append(")");
    }

    if (dialect() == Dialect.POSTGRESQL) {
      if (sequenceKind() != null && !sequenceKind().equalsIgnoreCase("default")) {
        if (sequenceKind().equalsIgnoreCase("bit_reversed_positive")) {
          appendable.append(" BIT_REVERSED_POSITIVE");
        } else {
          throw new IllegalArgumentException(
              String.format("Unrecognized sequence kind: %s.", sequenceKind()));
        }
      }
      if (skipRangeMin() != null && skipRangeMax() != null) {
        appendable
            .append(" SKIP RANGE ")
            .append(String.valueOf(skipRangeMin()))
            .append(" ")
            .append(String.valueOf(skipRangeMax()));
      }
      if (counterStartValue() != null) {
        appendable.append(" START COUNTER WITH ").append(String.valueOf(counterStartValue()));
      }
    }
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return prettyPrint();
  }

  /** A builder for {@link Sequence}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract Builder options(ImmutableList<String> options);

    public abstract Builder sequenceKind(String sequenceKind);

    public abstract Builder counterStartValue(Long value);

    public abstract Builder skipRangeMin(Long value);

    public abstract Builder skipRangeMax(Long value);

    abstract Builder dialect(Dialect dialect);

    public abstract Sequence build();

    public Ddl.Builder endSequence() {
      ddlBuilder.addSequence(build());
      return ddlBuilder;
    }
  }
}
