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

import static com.google.cloud.teleport.spanner.common.NameUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import javax.annotation.Nullable;

/** Cloud Spanner user-defined function. */
@AutoValue
public abstract class Udf implements Serializable {

  private static final long serialVersionUID = 1L;

  // Remote UDF definition is printed using: AS '{definition}'.
  // Quotes inside definiton string can be escaped using ''.
  public static final Escaper PG_DEFINITION_ESCAPER =
      Escapers.builder().addEscape('\'', "''").build();

  /** The access rights used by the UDF for underlying data: invoker-rights or definer-rights. */
  public enum SqlSecurity {
    INVOKER,
    DEFINER,
  }

  /**
   * The specific name uniquely identifies the UDF even if its name is overloaded. It can be used to
   * join UDF metadata from multiple sources (e.g. INFORMATION_SCHEMA.ROUTINES and
   * INFORMATION_SCHEMA.PARAMETERS). The specific name is not guaranteed to match the user-specified
   * name() or be the same after export and import.
   */
  public abstract String specificName();

  @Nullable
  public abstract String name();

  public abstract Dialect dialect();

  @Nullable
  public abstract String type();

  @Nullable
  public abstract String definition();

  @Nullable
  public abstract String language();

  @Nullable
  public abstract SqlSecurity security();

  public abstract ImmutableList<UdfParameter> parameters();

  public abstract ImmutableList<String> options();

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE FUNCTION ").append(quoteIdentifier(name(), dialect()));
    appendable.append("(");
    boolean first = true;
    for (UdfParameter parameter : parameters()) {
      if (!first) {
        appendable.append(", ");
      }
      first = false;
      appendable.append(parameter.prettyPrint());
    }
    appendable.append(")");
    if (type() != null) {
      appendable.append(" RETURNS ").append(type());
    }

    // Determinism should be added to INFORMATION_SCHEMA.ROUTINES.
    // For now, we infer it from the language.
    if (language() != null && language().equalsIgnoreCase("REMOTE")) {
      String determinism;
      if (dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        determinism = "NOT DETERMINISTIC";
      } else {
        determinism = "VOLATILE";
      }
      appendable.append(" ").append(determinism);
    }

    if (language() != null && !language().isEmpty()) {
      // GSQL does not accept LANGUAGE SQL even though it reports it.
      if (dialect() != Dialect.GOOGLE_STANDARD_SQL || !language().equalsIgnoreCase("SQL")) {
        appendable.append(" LANGUAGE ").append(language());
      }
    }

    if (security() != null) {
      // Remote UDF don't use SQL SECURITY, but it is marked NOT NULL in IS.
      if (!"REMOTE".equalsIgnoreCase(language())) {
        appendable.append(" SQL SECURITY ").append(security().toString());
      }
    }

    if (!options().isEmpty()) {
      if (dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        appendable.append(" OPTIONS (").append(String.join(", ", options())).append(")");
      } else {
        throw new IllegalArgumentException(
            "Options are not supported in PostgreSQL dialect for non-remote UDFs.");
      }
    }

    if (definition() != null && !definition().isEmpty()) {
      if (dialect() == Dialect.GOOGLE_STANDARD_SQL) {
        appendable.append(" AS (").append(definition()).append(")");
      } else {
        if (language() == null || language().isEmpty() || "SQL".equalsIgnoreCase(language())) {
          appendable.append(" RETURN ").append(definition());
        } else {
          // Other languages use AS definition instead of sql body.
          appendable.append(" AS '").append(PG_DEFINITION_ESCAPER.escape(definition())).append("'");
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

  public abstract Builder autoToBuilder();

  public Builder toBuilder() {
    Builder builder = autoToBuilder().specificName(specificName()).dialect(dialect());
    if (name() != null) {
      builder.name(name());
    }
    if (type() != null) {
      builder.type(type());
    }
    if (language() != null) {
      builder.language(language());
    }
    builder.options(options());
    if (definition() != null) {
      builder.definition(definition());
    }
    if (security() != null) {
      builder.security(security());
    }
    for (UdfParameter parameter : parameters()) {
      builder.addParameter(parameter);
    }
    return builder;
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_Udf.Builder()
        .dialect(dialect)
        .parameters(ImmutableList.of())
        .options(ImmutableList.of());
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  /** A builder for {@link Udf}. */
  @AutoValue.Builder
  public abstract static class Builder {
    private Ddl.Builder ddlBuilder;
    private LinkedHashMap<String, UdfParameter> parametersMap = Maps.newLinkedHashMap();
    private ImmutableList.Builder<UdfParameter> parameters = ImmutableList.builder();

    public Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder specificName(String specificName);

    public abstract String specificName();

    public abstract Builder name(String name);

    public abstract String name();

    public abstract Builder dialect(Dialect dialect);

    public abstract Dialect dialect();

    public abstract Builder type(String type);

    public abstract String type();

    public abstract Builder definition(String definition);

    public abstract String definition();

    public abstract Builder language(String language);

    public abstract String language();

    public abstract Builder security(SqlSecurity rights);

    public abstract SqlSecurity security();

    public abstract Builder options(ImmutableList<String> options);

    public abstract ImmutableList<String> options();

    public abstract Builder parameters(ImmutableList<UdfParameter> parameters);

    public ImmutableList<UdfParameter> parameters() {
      return parameters.build();
    }

    public UdfParameter.Builder parameter(String name) {
      UdfParameter parameter = parametersMap.get(name.toLowerCase());
      if (parameter != null) {
        if (!parameter.functionSpecificName().equals(specificName())) {
          throw new IllegalArgumentException(
              String.format(
                  "Parameter %s has a different function specific name %s than the user-defined"
                      + " function %s.",
                  name, parameter.functionSpecificName(), specificName()));
        }
        return parameter.toBuilder().udfBuilder(this);
      }
      return UdfParameter.builder(dialect())
          .name(name)
          .functionSpecificName(specificName())
          .udfBuilder(this);
    }

    public Builder addParameter(UdfParameter parameter) {
      parameters.add(parameter);
      parametersMap.put(parameter.name().toLowerCase(), parameter);
      return this;
    }

    abstract Udf autoBuild();

    public Udf build() {
      return new AutoValue_Udf.Builder()
          .specificName(specificName())
          .name(name())
          .dialect(dialect())
          .type(type())
          .definition(definition())
          .language(language())
          .security(security())
          .options(options())
          .parameters(ImmutableList.copyOf(parameters()))
          .autoBuild();
    }

    public Ddl.Builder endUdf() {
      ddlBuilder.addUdf(build());
      return ddlBuilder;
    }
  }
}
