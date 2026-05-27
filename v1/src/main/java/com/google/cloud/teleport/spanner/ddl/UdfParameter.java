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
    String name;
    String rest;
    String quote = identifierQuote(dialect);

    if (parameter.startsWith(quote)) {
      int closingQuoteIndex = parameter.indexOf(quote, quote.length());
      if (closingQuoteIndex == -1) {
        throw new IllegalArgumentException(
            "Unterminated quoted parameter name in " + functionSpecificName);
      }
      name = parameter.substring(0, closingQuoteIndex + quote.length());
      rest = parameter.substring(name.length()).trim();
    } else {
      String[] parts = parameter.split(" ", 2);
      if (parts.length < 2) {
        throw new IllegalArgumentException(
            "UDF input parameters are expected to be in the format 'name type [DEFAULT expr]'."
                + " Parameter: "
                + parameter);
      }
      name = parts[0];
      rest = parts[1].trim();
    }

    String[] typeParts = rest.split("(?i)\\s+DEFAULT\\s*", 2);
    String type = typeParts[0].trim();
    String defaultExpression = typeParts.length > 1 ? typeParts[1].trim() : null;

    if (defaultExpression == null && type.contains(" ")) {
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
