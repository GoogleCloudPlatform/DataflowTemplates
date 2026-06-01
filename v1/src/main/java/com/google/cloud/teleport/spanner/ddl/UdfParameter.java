/*
 * Copyright (C) 2018 Google LLC
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

import static com.google.cloud.teleport.spanner.common.NameUtils.identifierQuote;
import static com.google.cloud.teleport.spanner.common.NameUtils.quoteIdentifier;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

/** Cloud Spanner user-defined function parameter. */
@AutoValue
public abstract class UdfParameter implements Serializable {

  private static final long serialVersionUID = -1752579370892365181L;

  public abstract String functionSpecificName();

  public abstract String name();

  public abstract String type();

  public abstract Dialect dialect();

  @Nullable
  public abstract String defaultExpression();

  public static Builder builder(Dialect dialect) {
    return new AutoValue_UdfParameter.Builder().dialect(dialect);
  }

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static UdfParameter parse(String parameter, String functionSpecificName, Dialect dialect) {
    String quote = identifierQuote(dialect);

    // Regex to extract the name (quoted or unquoted) and the rest of the parameter string.
    // Group 1: Name (quoted or unquoted)
    // Group 2: Quoted Name
    // Group 3: Unquoted Name
    // Group 4: The rest (type and optional DEFAULT)
    String regex =
        String.format("^((%1$s[^%1$s]+%1$s)|(\\S+))\\s+(.*)$", java.util.regex.Pattern.quote(quote));
    java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(regex).matcher(parameter);

    if (!matcher.find()) {
      throw new IllegalArgumentException(
          "UDF input parameters are expected to be in the format 'name type [DEFAULT expr]'."
              + " Parameter: "
              + parameter);
    }

    String name = matcher.group(1);
    String rest = matcher.group(4).trim();

    // Split 'rest' into type and defaultExpression using a case-insensitive 'DEFAULT' keyword.
    String[] parts = rest.split("(?i)\\s+DEFAULT\\s+", 2);
    String type = parts[0].trim();
    String defaultExpression = parts.length > 1 ? parts[1].trim() : null;

    // The original logic for GoogleSQL restricted types to be a single word if no DEFAULT was
    // present.
    // We preserve this check for GSQL only to minimize behavioral changes.
    if (dialect == Dialect.GOOGLE_STANDARD_SQL && defaultExpression == null && type.contains(" ")) {
      throw new IllegalArgumentException("Unexpected parameter keyword in " + functionSpecificName);
    }

    UdfParameter.Builder udfParameter =
        UdfParameter.builder(dialect)
            .functionSpecificName(functionSpecificName)
            .name(name)
            .type(type);

    if (defaultExpression != null) {
      if (defaultExpression.isEmpty()) {
        throw new IllegalArgumentException(
            "Missing default parameter expression in " + functionSpecificName);
      }
      udfParameter.defaultExpression(defaultExpression);
    }
    return udfParameter.autoBuild();
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append(quoteIdentifier(name(), dialect()));
    appendable.append(" ");
    appendable.append(type());
    if (defaultExpression() != null) {
      appendable.append(" DEFAULT ");
      appendable.append(defaultExpression());
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

  public abstract Builder toBuilder();

  @Override
  public String toString() {
    return prettyPrint();
  }

  /** A builder for {@link UdfParameter}. */
  @AutoValue.Builder
  public abstract static class Builder {

    private Udf.Builder udfBuilder;

    Builder udfBuilder(Udf.Builder udfBuilder) {
      this.udfBuilder = udfBuilder;
      return this;
    }

    public abstract Builder functionSpecificName(String functionSpecificName);

    public abstract Builder name(String name);

    public abstract Builder type(String type);

    public abstract Builder dialect(Dialect dialect);

    public abstract Builder defaultExpression(String expression);

    abstract UdfParameter autoBuild();

    public Udf.Builder endUdfParameter() {
      UdfParameter udfParameter = this.autoBuild();
      udfBuilder.addParameter(udfParameter);
      return udfBuilder;
    }
  }
}
