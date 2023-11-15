/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.zetasql.Analyzer;
import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.ArrayType;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedLiteral;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Utility class for translating JSON parameters to ZetaSQL values. */
final class ParameterTranslator {

  private ParameterTranslator() {}

  enum ParameterMode {
    NONE,
    NAMED,
    POSITIONAL
  }

  /** Intermediate parameter representation between JSON and ZetaSQL values. */
  @AutoValue
  abstract static class Parameter {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    abstract String name();

    abstract ParameterType parameterType();

    abstract ParameterValue parameterValue();

    @JsonCreator
    static Parameter create(
        @JsonProperty("name") String name,
        @JsonProperty("parameterType") ParameterType parameterType,
        @JsonProperty("parameterValue") ParameterValue parameterValue) {
      return new com.google.cloud.teleport.v2.sqllauncher.AutoValue_ParameterTranslator_Parameter(
          name == null ? "" : name, parameterType, parameterValue);
    }
  }

  @AutoValue
  abstract static class ParameterType {
    abstract String type();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    abstract ParameterType arrayType();

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Nullable
    abstract List<ParameterStructType> structTypes();

    @JsonCreator
    static ParameterType create(
        @JsonProperty("type") String type,
        @JsonProperty("arrayType") ParameterType arrayType,
        @JsonProperty("structTypes") List<ParameterStructType> structTypes) {
      return new com.google.cloud.teleport.v2.sqllauncher.AutoValue_ParameterTranslator_ParameterType(type, arrayType, structTypes);
    }
  }

  @AutoValue
  @JsonInclude(JsonInclude.Include.NON_NULL)
  abstract static class ParameterValue {
    @Nullable
    abstract String value();

    @Nullable
    abstract List<ParameterValue> arrayValues();

    @Nullable
    abstract Map<String, ParameterValue> structValues();

    @JsonCreator
    static ParameterValue create(
        @JsonProperty("value") String value,
        @JsonProperty("arrayValues") List<ParameterValue> arrayValues,
        @JsonProperty("structValues") Map<String, ParameterValue> structValues) {
      return new com.google.cloud.teleport.v2.sqllauncher.AutoValue_ParameterTranslator_ParameterValue(value, arrayValues, structValues);
    }
  }

  @AutoValue
  abstract static class ParameterStructType {
    abstract String name();

    abstract ParameterType type();

    @JsonCreator
    static ParameterStructType create(
        @JsonProperty("name") String name, @JsonProperty("type") ParameterType type) {
      return new com.google.cloud.teleport.v2.sqllauncher.AutoValue_ParameterTranslator_ParameterStructType(name, type);
    }
  }

  static ParameterType createScalarParameterType(String type) {
    return ParameterType.create(type, null, null);
  }

  static ParameterValue createScalarParameterValue(String value) {
    return ParameterValue.create(value, null, null);
  }

  static Parameter createScalarParameter(String name, String type, String value) {
    return Parameter.create(
        name, createScalarParameterType(type), createScalarParameterValue(value));
  }

  static List<Parameter> parseJson(String json) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(json, new TypeReference<List<Parameter>>() {});
  }

  static ParameterMode inferParameterMode(List<Parameter> parameters) {
    if (parameters.isEmpty()) {
      return ParameterMode.NONE;
    }
    int namedCount = 0;
    for (Parameter param : parameters) {
      if (!param.name().isEmpty()) {
        namedCount++;
      }
    }
    if (namedCount == 0) {
      return ParameterMode.POSITIONAL;
    } else if (namedCount == parameters.size()) {
      return ParameterMode.NAMED;
    }
    throw new IllegalArgumentException("Cannot mix named and unnamed parameters.");
  }

  private static Type translateType(ParameterType type) {
    switch (type.type().toUpperCase()) {
      case "STRING":
        return TypeFactory.createSimpleType(TypeKind.TYPE_STRING);
      case "INT64":
        return TypeFactory.createSimpleType(TypeKind.TYPE_INT64);
      case "FLOAT64":
        // fall through
      case "DOUBLE":
        return TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE);
      case "BOOL":
        return TypeFactory.createSimpleType(TypeKind.TYPE_BOOL);
      case "BYTES":
        return TypeFactory.createSimpleType(TypeKind.TYPE_BYTES);
      case "ARRAY":
        return TypeFactory.createArrayType(translateType(type.arrayType()));
      case "STRUCT":
        ImmutableList.Builder<StructType.StructField> structFields = ImmutableList.builder();
        for (ParameterStructType structType : type.structTypes()) {
          structFields.add(new StructField(structType.name(), translateType(structType.type())));
        }
        return TypeFactory.createStructType(structFields.build());
      default:
        throw new IllegalArgumentException(
            String.format("Invalid parameter type '%s'.", type.type()));
    }
  }

  private static Value parseAsType(String parameter, Type type) {
    // TODO: share beam's analyzer options
    String cast = String.format("CAST(%s AS %s)", parameter, type);
    ResolvedExpr expr =
        Analyzer.analyzeExpression(cast, new AnalyzerOptions(), new SimpleCatalog("emptyCatalog"));
    Preconditions.checkArgument(expr.getType().equals(type));
    Preconditions.checkArgument(
        expr instanceof ResolvedLiteral,
        String.format("Failed to resolve parameter '%s'", parameter));
    ResolvedLiteral literal = (ResolvedLiteral) expr;
    return literal.getValue();
  }

  // LINT.IfChange
  private static Value translateParameter(Parameter parameter) {
    // TODO: parse timestamps
    // scalarValue will be null if parameter has a compound type.
    String scalarValue = parameter.parameterValue().value();
    switch (parameter.parameterType().type().toUpperCase()) {
      case "INT64":
        return parseAsType(scalarValue, TypeFactory.createSimpleType(TypeKind.TYPE_INT64));
      case "FLOAT64":
        // fall through
      case "DOUBLE":
        return parseAsType(scalarValue, TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE));
      case "BOOL":
        return parseAsType(scalarValue, TypeFactory.createSimpleType(TypeKind.TYPE_BOOL));
      case "STRING":
        return Value.createStringValue(scalarValue);
      case "BYTES":
        return Value.createBytesValue(ByteString.copyFromUtf8(scalarValue));
      case "ARRAY":
        ParameterType elementType = parameter.parameterType().arrayType();
        ImmutableList.Builder<Value> elementsBuilder = ImmutableList.builder();
        for (ParameterValue elementValue : parameter.parameterValue().arrayValues()) {
          elementsBuilder.add(translateParameter(Parameter.create("", elementType, elementValue)));
        }
        return Value.createArrayValue(
            (ArrayType) translateType(parameter.parameterType()), elementsBuilder.build());
      case "STRUCT":
        Map<String, ParameterValue> structValues = parameter.parameterValue().structValues();
        ImmutableList.Builder<Value> fields = ImmutableList.builder();
        for (ParameterStructType fieldType : parameter.parameterType().structTypes()) {
          if (!structValues.containsKey(fieldType.name())) {
            throw new IllegalArgumentException("Struct missing expected field " + fieldType.name());
          }
          ParameterValue fieldValue = structValues.get(fieldType.name());
          fields.add(translateParameter(Parameter.create("", fieldType.type(), fieldValue)));
        }
        return Value.createStructValue(
            (StructType) translateType(parameter.parameterType()), fields.build());
      default:
        throw new IllegalArgumentException(
            String.format("Invalid parameter type '%s'.", parameter.parameterType().type()));
    }
  }

  // LINT.ThenChange(//depot/google3/cloud/console/web/dataflow/common/components/create_job/sql_query_params/input.ts)
  // SqlQueryParamsInput.supportedSqlTypes must match the types supported here

  static Map<String, Value> translateNamedParameters(List<Parameter> parameters) {
    ImmutableMap.Builder<String, Value> builder = ImmutableMap.builder();
    for (Parameter param : parameters) {
      if (param.name().isEmpty()) {
        throw new IllegalArgumentException("Parameter name cannot be empty.");
      }
      builder.put(param.name(), translateParameter(param));
    }
    return builder.build();
  }

  static List<Value> translatePositionalParameters(List<Parameter> parameters) {
    ImmutableList.Builder<Value> builder = ImmutableList.builder();
    for (Parameter param : parameters) {
      builder.add(translateParameter(param));
    }
    return builder.build();
  }
}
