/*
 * Copyright (C) 2020 Google LLC
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

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;

/** Cloud Spanner foreign key definition. */
@AutoValue
public abstract class ForeignKey implements Serializable {
  private static final long serialVersionUID = 519932875L;

  /** Referential actions supported in Foreign Keys. */
  public enum ReferentialAction {
    // Supported actions
    ON_DELETE_NO_ACTION("ON DELETE NO ACTION"),
    ON_DELETE_CASCADE("ON DELETE CASCADE"),
    // Currently unsupported actions, listed here for completeness
    ON_DELETE_RESTRICT("ON DELETE RESTRICT"),
    ON_DELETE_SET_NULL("ON DELETE SET NULL"),
    ON_DELETE_SET_DEFAULT("ON DELETE SET DEFAULT"),
    ON_UPDATE_NO_ACTION("ON UPDATE NO ACTION"),
    ON_UPDATE_CASCADE("ON UPDATE CASCADE"),
    ON_UPDATE_RESTRICT("ON UPDATE RESTRICT"),
    ON_UPDATE_SET_NULL("ON UPDATE SET NULL"),
    ON_UPDATE_SET_DEFAULT("ON UPDATE SET DEFAULT");

    private String sqlString;

    private ReferentialAction(String sqlString) {
      this.sqlString = sqlString;
    }

    public String getSqlString() {
      return sqlString;
    }

    public static ReferentialAction getReferentialAction(String changeType, String action) {
      if (isNullOrEmpty(changeType) || isNullOrEmpty(action)) {
        throw new IllegalArgumentException(
            String.format("Empty changeType [%s] or action [%s]", changeType, action));
      }
      if ("DELETE".equalsIgnoreCase(changeType.trim())) {
        switch (action.trim().toUpperCase()) {
          case "CASCADE":
            return ReferentialAction.ON_DELETE_CASCADE;
          case "NO ACTION":
            return ReferentialAction.ON_DELETE_NO_ACTION;
          default:
            throw new IllegalArgumentException(
                "ON DELETE referential action not supported: " + action);
        }
      } else {
        throw new IllegalArgumentException(
            "ON " + changeType + " referential action not supported: " + action);
      }
    }
  }

  abstract String name();

  abstract String table();

  abstract String referencedTable();

  abstract ImmutableList<String> columns();

  abstract ImmutableList<String> referencedColumns();

  abstract Dialect dialect();

  abstract Optional<ReferentialAction> referentialAction();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_ForeignKey.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  private void prettyPrint(Appendable appendable) throws IOException {
    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect());
    String columnsString =
        columns().stream()
            .map(c -> identifierQuote + c + identifierQuote)
            .collect(Collectors.joining(", "));
    String referencedColumnsString =
        referencedColumns().stream()
            .map(c -> identifierQuote + c + identifierQuote)
            .collect(Collectors.joining(", "));
    appendable
        .append("ALTER TABLE " + identifierQuote)
        .append(table())
        .append(identifierQuote + " ADD CONSTRAINT " + identifierQuote)
        .append(name())
        .append(identifierQuote + " FOREIGN KEY (")
        .append(columnsString)
        .append(") REFERENCES " + identifierQuote)
        .append(referencedTable())
        .append((identifierQuote + " ("))
        .append(referencedColumnsString)
        .append(")");
    Optional<ReferentialAction> action = referentialAction();
    if (action.isPresent()) {
      switch (action.get()) {
        case ON_DELETE_CASCADE:
        case ON_DELETE_NO_ACTION:
          appendable.append(" " + action.get().getSqlString());
          break;
        default:
          throw new IllegalArgumentException(
              "Foreign Key action not supported: " + action.get().getSqlString());
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

  /** A builder for {@link ForeignKey}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder name(String name);

    public abstract Builder table(String name);

    public abstract Builder referencedTable(String name);

    abstract Builder dialect(Dialect dialect);

    public abstract ImmutableList.Builder<String> columnsBuilder();

    public abstract ImmutableList.Builder<String> referencedColumnsBuilder();

    public abstract Builder referentialAction(Optional<ReferentialAction> action);

    public abstract ForeignKey build();
  }
}
