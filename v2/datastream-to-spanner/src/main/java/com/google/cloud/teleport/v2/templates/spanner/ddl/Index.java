/*
 * Copyright (C) 2018 Google Inc.
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
import javax.annotation.Nullable;

/** Cloud Spanner index definition. */
@AutoValue
public abstract class Index implements Serializable {

  private static final long serialVersionUID = 7435575480487550039L;

  abstract String name();

  abstract String table();

  abstract ImmutableList<IndexColumn> indexColumns();

  abstract boolean unique();

  abstract boolean nullFiltered();

  @Nullable
  abstract String interleaveIn();

  public static Builder builder() {
    return new AutoValue_Index.Builder().nullFiltered(false).unique(false);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE");
    if (unique()) {
      appendable.append(" UNIQUE");
    }
    if (nullFiltered()) {
      appendable.append(" NULL_FILTERED");
    }
    appendable.append(" INDEX `").append(name()).append("` ON `").append(table()).append("`");

    String indexColumnsString =
        indexColumns()
            .stream()
            .filter(c -> c.order() != IndexColumn.Order.STORING)
            .map(c -> c.prettyPrint())
            .collect(Collectors.joining(", "));
    appendable.append("(").append(indexColumnsString).append(")");

    String storingString =
        indexColumns()
            .stream()
            .filter(c -> c.order() == IndexColumn.Order.STORING)
            .map(c -> "`" + c.name() + "`")
            .collect(Collectors.joining(", "));

    if (!storingString.isEmpty()) {
      appendable.append(" STORING (").append(storingString).append(")");
    }
    if (interleaveIn() != null) {
      appendable.append(", INTERLEAVE IN ").append(interleaveIn());
    }
  }

  abstract Builder autoToBuilder();

  public Builder toBuilder() {
    Builder builder = autoToBuilder();
    for (IndexColumn column : indexColumns()) {
      builder.columnsBuilder.set(column);
    }
    return builder;
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

  /** A builder for {@link Index}. */
  @AutoValue.Builder
  public abstract static class Builder {

    private IndexColumn.IndexColumnsBuilder<Builder> columnsBuilder =
        new IndexColumn.IndexColumnsBuilder<>(this);

    public abstract Builder name(String name);

    public abstract Builder table(String name);

    abstract Builder indexColumns(ImmutableList<IndexColumn> columns);

    public IndexColumn.IndexColumnsBuilder<Builder> columns() {
      return columnsBuilder;
    }

    public abstract Builder unique(boolean unique);

    public Builder unique() {
      return unique(true);
    }

    public abstract Builder nullFiltered(boolean nullFiltered);

    public Builder nullFiltered() {
      return nullFiltered(true);
    }

    public abstract Builder interleaveIn(String interleaveIn);

    abstract Index autoBuild();

    public Index build() {
      return this.indexColumns(columnsBuilder.build()).autoBuild();
    }
  }
}
