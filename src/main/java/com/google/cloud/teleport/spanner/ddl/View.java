/*
 * Copyright (C) 2021 Google LLC
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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner view. */
@AutoValue
public abstract class View implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The access rights used by the view for underlying data, invoker-rights or definer-rights. */
  public enum SqlSecurity {
    INVOKER,
  }

  @Nullable
  public abstract String name();

  @Nullable
  public abstract String query();

  @Nullable
  public abstract SqlSecurity security();

  public abstract Dialect dialect();

  public abstract Builder toBuilder();

  public void prettyPrint(Appendable appendable) throws IOException {
    String identifierQuote = DdlUtilityComponents.identifierQuote(dialect());
    appendable.append("CREATE VIEW " + identifierQuote).append(name()).append(identifierQuote);
    SqlSecurity rights = security();
    if (rights != null) {
      appendable.append(" SQL SECURITY ").append(rights.toString());
    }
    appendable.append(" AS ").append(query());
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

  public static Builder builder(Dialect dialect) {
    return new AutoValue_View.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  /** A builder for {@link View}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract String name();

    public abstract Builder query(String query);

    public abstract String query();

    public abstract Builder security(SqlSecurity rights);

    public abstract SqlSecurity security();

    abstract Builder dialect(Dialect dialect);

    public abstract View build();

    public Ddl.Builder endView() {
      ddlBuilder.addView(build());
      return ddlBuilder;
    }
  }
}
