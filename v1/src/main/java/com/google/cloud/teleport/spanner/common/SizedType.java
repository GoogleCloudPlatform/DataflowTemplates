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
package com.google.cloud.teleport.spanner.common;

import static java.lang.Character.isWhitespace;

import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/** Describes a type with size. */
public final class SizedType {
  public final Type type;
  public final Integer size;
  // Describes the exact length for ARRAY types if needed. Used for embedding vectors.
  public final Integer arrayLength;

  private static final Pattern EMBEDDING_VECTOR_PATTERN =
      Pattern.compile(
          "^ARRAY<([a-zA-Z0-9]+)>\\(vector_length=>(\\d+)\\)$", Pattern.CASE_INSENSITIVE);

  private static final Pattern PG_EMBEDDING_VECTOR_PATTERN =
      Pattern.compile("^(\\D+)\\[\\]\\svector\\slength\\s(\\d+)$", Pattern.CASE_INSENSITIVE);

  public SizedType(Type type, Integer size) {
    this.type = type;
    this.size = size;
    this.arrayLength = null;
  }

  public SizedType(Type type, Integer size, Integer arrayLength) {
    this.type = type;
    this.size = size;
    this.arrayLength = arrayLength;
  }

  public static String typeString(Type type, Integer size) {
    switch (type.getCode()) {
      case BOOL:
        return "BOOL";
      case PG_BOOL:
        return "boolean";
      case INT64:
        return "INT64";
      case PG_INT8:
        return "bigint";
      case FLOAT64:
        return "FLOAT64";
      case PG_FLOAT8:
        return "double precision";
      case STRING:
        return "STRING(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_VARCHAR:
        return "character varying" + (size == -1 ? "" : ("(" + Integer.toString(size) + ")"));
      case PG_TEXT:
        return "text";
      case BYTES:
        return "BYTES(" + (size == -1 ? "MAX" : Integer.toString(size)) + ")";
      case PG_BYTEA:
        return "bytea";
      case DATE:
        return "DATE";
      case PG_DATE:
        return "date";
      case TIMESTAMP:
        return "TIMESTAMP";
      case PG_TIMESTAMPTZ:
        return "timestamp with time zone";
      case PG_SPANNER_COMMIT_TIMESTAMP:
        return "spanner.commit_timestamp";
      case NUMERIC:
        return "NUMERIC";
      case PG_NUMERIC:
        return "numeric";
      case JSON:
        return "JSON";
      case PG_JSONB:
        return "jsonb";
      case ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return "ARRAY<" + typeString(arrayType, size) + ">";
        }
      case STRUCT:
        {
          StringBuilder sb = new StringBuilder();
          for (int i = 0; i < type.getStructFields().size(); ++i) {
            Type.StructField field = type.getStructFields().get(i);
            sb.append(i > 0 ? ", " : "")
                .append(field.getName())
                .append(" ")
                .append(typeString(field.getType(), -1));
          }
          return "STRUCT<" + sb.toString() + ">";
        }
      case PG_ARRAY:
        {
          Type arrayType = type.getArrayElementType();
          return typeString(arrayType, size) + "[]";
        }
    }

    throw new IllegalArgumentException("Unknown type " + type);
  }

  public static String typeString(Type type, Integer size, int arrayLength) {
    switch (type.getCode()) {
      case ARRAY: {
        return typeString(type, size) + "(vector_length=>" + Integer.toString(arrayLength) + ")";
      }
      case PG_ARRAY: {
        return typeString(type, size) + " vector length " + Integer.toString(arrayLength);
      }
    }
    throw new IllegalArgumentException("arrayLength not supported for " + type);
  }

  private static SizedType t(Type type, Integer size) {
    return new SizedType(type, size);
  }

  private static SizedType t(Type type, Integer size, Integer arrayLength) {
    return new SizedType(type, size, arrayLength);
  }

  public static SizedType parseSpannerType(String spannerType, Dialect dialect) {
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        {
          if (spannerType.equals("BOOL")) {
            return t(Type.bool(), null);
          }
          if (spannerType.equals("INT64")) {
            return t(Type.int64(), null);
          }
          if (spannerType.equals("FLOAT64")) {
            return t(Type.float64(), null);
          }
          if (spannerType.startsWith("STRING")) {
            String sizeStr = spannerType.substring(7, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.string(), size);
          }
          if (spannerType.startsWith("BYTES")) {
            String sizeStr = spannerType.substring(6, spannerType.length() - 1);
            int size = sizeStr.equals("MAX") ? -1 : Integer.parseInt(sizeStr);
            return t(Type.bytes(), size);
          }
          if (spannerType.equals("TIMESTAMP")) {
            return t(Type.timestamp(), null);
          }
          if (spannerType.equals("DATE")) {
            return t(Type.date(), null);
          }
          if (spannerType.equals("NUMERIC")) {
            return t(Type.numeric(), null);
          }
          if (spannerType.equals("JSON")) {
            return t(Type.json(), null);
          }
          if (spannerType.startsWith("ARRAY<")) {
            // Substring "ARRAY<xxx> or ARRAY<xxx>(vector_length)"

            // Handle vector_length annotation
            Matcher m = EMBEDDING_VECTOR_PATTERN.matcher(spannerType);
            if (m.find()) {
              String spannerArrayType = m.group(1);
              Integer arrayLength = Integer.parseInt(m.group(2));
              SizedType itemType = parseSpannerType(spannerArrayType, dialect);
              return t(Type.array(itemType.type), itemType.size, arrayLength);
            }

            String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.array(itemType.type), itemType.size);
          }
          if (spannerType.startsWith("STRUCT<")) {
            // Substring "STRUCT<xxx>"
            String spannerStructType = spannerType.substring(7, spannerType.length() - 1);
            ImmutableList.Builder<Type.StructField> fields = ImmutableList.builder();
            int current = 0;
            // Parse each struct field. These type names are coming from information schema and are
            // expected to be correctly formatted. Fields are specified as NAME TYPE and separated
            // with commas. Since TYPE can be another struct we cannot simply split on commas, but
            // instead we will count opening braces and ignore any commas that are part of field
            // type specification.
            while (current < spannerStructType.length()) {
              int i = current;
              // Skip whitespace.
              for (; isWhitespace(spannerStructType.charAt(i)); ++i) {}
              current = i;
              // Read the name.
              for (; !isWhitespace(spannerStructType.charAt(i)); ++i) {}
              String fieldName = spannerStructType.substring(current, i);
              // Skip whitespace.
              for (; isWhitespace(spannerStructType.charAt(i)); ++i) {}
              current = i;
              // Find the end of the type.
              int bracketCount = 0;
              for (; i < spannerStructType.length(); ++i) {
                char c = spannerStructType.charAt(i);
                if (c == '<') {
                  ++bracketCount;
                } else if (c == '>') {
                  if (--bracketCount < 0) {
                    break;
                  }
                } else if (c == ',') {
                  if (bracketCount == 0) {
                    break;
                  }
                }
              }
              if (bracketCount != 0) {
                throw new IllegalArgumentException("Unknown spanner type " + spannerType);
              }
              // Read the type.
              SizedType fieldType =
                  parseSpannerType(spannerStructType.substring(current, i), dialect);
              fields.add(Type.StructField.of(fieldName, fieldType.type));
              current = i + 1;
            }
            return t(Type.struct(fields.build()), null);
          }
          break;
        }
      case POSTGRESQL:
        {
          // Handle vector_length annotation
          Matcher m = PG_EMBEDDING_VECTOR_PATTERN.matcher(spannerType);
          if (m.find()) {
            // Substring "xxx[] vector length yyy"
            String spannerArrayType = m.group(1);
            Integer arrayLength = Integer.parseInt(m.group(2));
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.pgArray(itemType.type), itemType.size, arrayLength);
          }
          if (spannerType.endsWith("[]")) {
            // Substring "xxx[]"
            // Must check array type first
            String spannerArrayType = spannerType.substring(0, spannerType.length() - 2);
            SizedType itemType = parseSpannerType(spannerArrayType, dialect);
            return t(Type.pgArray(itemType.type), itemType.size);
          }
          if (spannerType.equals("boolean")) {
            return t(Type.pgBool(), null);
          }
          if (spannerType.equals("bigint")) {
            return t(Type.pgInt8(), null);
          }
          if (spannerType.equals("double precision")) {
            return t(Type.pgFloat8(), null);
          }
          if (spannerType.equals("text")) {
            return t(Type.pgText(), -1);
          }
          if (spannerType.startsWith("character varying")) {
            int size = -1;
            if (spannerType.length() > 18) {
              String sizeStr = spannerType.substring(18, spannerType.length() - 1);
              size = Integer.parseInt(sizeStr);
            }
            return t(Type.pgVarchar(), size);
          }
          if (spannerType.equals("bytea")) {
            return t(Type.pgBytea(), -1);
          }
          if (spannerType.equals("timestamp with time zone")) {
            return t(Type.pgTimestamptz(), null);
          }
          if (spannerType.equals("numeric")) {
            return t(Type.pgNumeric(), null);
          }
          if (spannerType.equals("jsonb")) {
            return t(Type.pgJsonb(), null);
          }
          if (spannerType.equals("date")) {
            return t(Type.pgDate(), null);
          }
          if (spannerType.equals("spanner.commit_timestamp")) {
            return t(Type.pgSpannerCommitTimestamp(), null);
          }
          break;
        }
      default:
        break;
    }
    throw new IllegalArgumentException("Unknown spanner type " + spannerType);
  }
}
