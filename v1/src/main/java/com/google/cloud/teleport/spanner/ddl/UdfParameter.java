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
    String[] paramParts = parameter.split(" ");
    if (paramParts.length < 2) {
      throw new IllegalArgumentException(
          "UDF input parameters are expected to be in the format 'name type [DEFAULT expr]'."
              + " Parameter: "
              + parameter);
    }
    UdfParameter.Builder udfParameter =
        UdfParameter.builder(dialect)
            .functionSpecificName(functionSpecificName)
            .name(paramParts[0])
            .type(paramParts[1]);
    if (paramParts.length > 2) {
      if (paramParts[2].equalsIgnoreCase("default")) {
        if (paramParts.length == 3) {
          throw new IllegalArgumentException(
              "Missing default parameter expression in " + functionSpecificName);
        }
        String defaultExpression = "";
        for (int i = 3; i < paramParts.length; i++) {
          if (!defaultExpression.isEmpty()) {
            defaultExpression += " ";
          }
          defaultExpression += paramParts[i];
        }
        udfParameter.defaultExpression(defaultExpression);
      } else {
        throw new IllegalArgumentException(
            "Unexpected parameter keyword \"" + paramParts[2] + "\" in " + functionSpecificName);
      }
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
