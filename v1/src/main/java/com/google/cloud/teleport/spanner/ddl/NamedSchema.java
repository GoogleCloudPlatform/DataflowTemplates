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
import java.io.IOException;
import java.io.Serializable;

@AutoValue
public abstract class NamedSchema implements Serializable {

  private static final long serialVersionUID = -5156046721891763991L;

  public abstract String name();

  public abstract Dialect dialect();

  public abstract NamedSchema.Builder toBuilder();

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE SCHEMA ").append(quoteIdentifier(name(), dialect()));
  }

  @Override
  public String toString() {
    return prettyPrint();
  }

  public static Builder builder() {
    return new AutoValue_NamedSchema.Builder();
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_NamedSchema.Builder().dialect(dialect);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    private Ddl.Builder ddlBuilder;

    public NamedSchema.Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String value);

    public abstract Builder dialect(Dialect value);

    public abstract NamedSchema build();

    public Ddl.Builder endNamedSchema() {
      ddlBuilder.addSchema(build());
      return ddlBuilder;
    }
  }
}
