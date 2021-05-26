/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.v2.templates.spanner.ddl;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;

/** Cloud Spanner foreign key definition. */
@AutoValue
public abstract class ForeignKey implements Serializable {
  private static final long serialVersionUID = 286089905L;

  abstract String name();

  abstract String table();

  abstract String referencedTable();

  abstract ImmutableList<String> columns();

  abstract ImmutableList<String> referencedColumns();

  public static Builder builder() {
    return new AutoValue_ForeignKey.Builder();
  }

  private void prettyPrint(Appendable appendable) throws IOException {
    String columnsString =
        columns().stream().map(c -> "`" + c + "`").collect(Collectors.joining(", "));
    String referencedColumnsString =
        referencedColumns().stream().map(c -> "`" + c + "`").collect(Collectors.joining(", "));
    appendable
        .append("ALTER TABLE `")
        .append(table())
        .append("` ADD CONSTRAINT `")
        .append(name())
        .append("` FOREIGN KEY (")
        .append(columnsString)
        .append(") REFERENCES `")
        .append(referencedTable())
        .append(("` ("))
        .append(referencedColumnsString)
        .append(")");
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

    public abstract ImmutableList.Builder<String> columnsBuilder();

    public abstract ImmutableList.Builder<String> referencedColumnsBuilder();

    public abstract ForeignKey build();
  }
}
