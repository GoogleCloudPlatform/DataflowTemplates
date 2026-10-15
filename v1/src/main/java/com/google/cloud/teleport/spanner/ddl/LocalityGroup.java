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
package com.google.cloud.teleport.spanner.ddl;

import static com.google.cloud.teleport.spanner.common.NameUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner locality group. */
@AutoValue
public abstract class LocalityGroup implements Serializable {

  private static final long serialVersionUID = 1L;

  public abstract String name();

  @Nullable
  public abstract ImmutableList<String> options();

  public abstract Dialect dialect();

  public abstract Builder toBuilder();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_LocalityGroup.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    if (dialect() != Dialect.GOOGLE_STANDARD_SQL && dialect() != Dialect.POSTGRESQL) {
      throw new IllegalArgumentException(String.format("Unrecognized Dialect: %s.", dialect()));
    }

    boolean isDefault = "default".equalsIgnoreCase(name());
    appendable
        .append(isDefault ? "ALTER LOCALITY GROUP " : "CREATE LOCALITY GROUP ")
        .append(quoteIdentifier(name(), dialect()));

    if (options() != null && !options().isEmpty()) {
      if (dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        String optionsString = String.join(", ", options());
        appendable
            .append(isDefault ? "\n\tSET OPTIONS (" : "\n\tOPTIONS (")
            .append(optionsString)
            .append(")");
      } else {
        String storageClause = null;
        String spillClause = null;
        for (String option : options()) {
          String[] parts = option.split("=", 2);
          if (parts.length != 2) {
            continue;
          }
          String key = parts[0].trim();
          String val = parts[1].trim();
          if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
            val = "'" + val.substring(1, val.length() - 1) + "'";
          }
          if (key.equalsIgnoreCase("storage")) {
            storageClause = "STORAGE " + val;
          } else if (key.equalsIgnoreCase("ssd_to_hdd_spill_timespan")) {
            spillClause = "SSD_TO_HDD_SPILL_TIMESPAN " + val;
          }
        }
        if (storageClause != null) {
          appendable.append(" ").append(storageClause);
        }
        if (spillClause != null) {
          appendable.append(" ").append(spillClause);
        }
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

  /** A builder for {@link LocalityGroup}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract Builder options(ImmutableList<String> options);

    abstract Builder dialect(Dialect dialect);

    public abstract LocalityGroup build();

    public Ddl.Builder endLocalityGroup() {
      ddlBuilder.addLocalityGroup(build());
      return ddlBuilder;
    }
  }
}
