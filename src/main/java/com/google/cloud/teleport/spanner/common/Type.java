/*
 * Copyright (C) 2017 Google LLC
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

import com.google.cloud.spanner.Dialect;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Describes a type in the Cloud Spanner type system. Types can either be primitive (for example,
 * {@code INT64} and {@code STRING}) or composite (for example, {@code ARRAY<INT64>} or {@code
 * STRUCT<INT64,STRING>}).
 *
 * <p>{@code Type} instances are immutable.
 */
@Immutable
public final class Type implements Serializable {
  private static final Type TYPE_BOOL = new Type(Code.BOOL, null, null);
  private static final Type TYPE_INT64 = new Type(Code.INT64, null, null);
  private static final Type TYPE_FLOAT64 = new Type(Code.FLOAT64, null, null);
  private static final Type TYPE_NUMERIC = new Type(Code.NUMERIC, null, null);
  private static final Type TYPE_STRING = new Type(Code.STRING, null, null);
  private static final Type TYPE_JSON = new Type(Code.JSON, null, null);
  private static final Type TYPE_BYTES = new Type(Code.BYTES, null, null);
  private static final Type TYPE_TIMESTAMP = new Type(Code.TIMESTAMP, null, null);
  private static final Type TYPE_DATE = new Type(Code.DATE, null, null);
  private static final Type TYPE_ARRAY_BOOL = new Type(Code.ARRAY, TYPE_BOOL, null);
  private static final Type TYPE_ARRAY_INT64 = new Type(Code.ARRAY, TYPE_INT64, null);
  private static final Type TYPE_ARRAY_FLOAT64 = new Type(Code.ARRAY, TYPE_FLOAT64, null);
  private static final Type TYPE_ARRAY_NUMERIC = new Type(Code.ARRAY, TYPE_NUMERIC, null);
  private static final Type TYPE_ARRAY_STRING = new Type(Code.ARRAY, TYPE_STRING, null);
  private static final Type TYPE_ARRAY_JSON = new Type(Code.ARRAY, TYPE_JSON, null);
  private static final Type TYPE_ARRAY_BYTES = new Type(Code.ARRAY, TYPE_BYTES, null);
  private static final Type TYPE_ARRAY_TIMESTAMP = new Type(Code.ARRAY, TYPE_TIMESTAMP, null);
  private static final Type TYPE_ARRAY_DATE = new Type(Code.ARRAY, TYPE_DATE, null);

  private static final Type TYPE_PG_BOOL = new Type(Code.PG_BOOL, null, null);
  private static final Type TYPE_PG_INT8 = new Type(Code.PG_INT8, null, null);
  private static final Type TYPE_PG_FLOAT8 = new Type(Code.PG_FLOAT8, null, null);
  private static final Type TYPE_PG_VARCHAR = new Type(Code.PG_VARCHAR, null, null);
  private static final Type TYPE_PG_TEXT = new Type(Code.PG_TEXT, null, null);
  private static final Type TYPE_PG_NUMERIC = new Type(Code.PG_NUMERIC, null, null);
  private static final Type TYPE_PG_BYTEA = new Type(Code.PG_BYTEA, null, null);
  private static final Type TYPE_PG_TIMESTAMPTZ = new Type(Code.PG_TIMESTAMPTZ, null, null);
  private static final Type TYPE_PG_DATE = new Type(Code.PG_DATE, null, null);
  private static final Type TYPE_PG_ARRAY_BOOL = new Type(Code.PG_ARRAY, TYPE_PG_BOOL, null);
  private static final Type TYPE_PG_ARRAY_INT8 = new Type(Code.PG_ARRAY, TYPE_PG_INT8, null);
  private static final Type TYPE_PG_ARRAY_FLOAT8 = new Type(Code.PG_ARRAY, TYPE_PG_FLOAT8, null);
  private static final Type TYPE_PG_ARRAY_VARCHAR = new Type(Code.PG_ARRAY, TYPE_PG_VARCHAR, null);
  private static final Type TYPE_PG_ARRAY_TEXT = new Type(Code.PG_ARRAY, TYPE_PG_TEXT, null);
  private static final Type TYPE_PG_ARRAY_NUMERIC = new Type(Code.PG_ARRAY, TYPE_PG_NUMERIC, null);
  private static final Type TYPE_PG_ARRAY_BYTEA = new Type(Code.PG_ARRAY, TYPE_PG_BYTEA, null);
  private static final Type TYPE_PG_ARRAY_TIMESTAMPTZ =
      new Type(Code.PG_ARRAY, TYPE_PG_TIMESTAMPTZ, null);
  private static final Type TYPE_PG_ARRAY_DATE = new Type(Code.PG_ARRAY, TYPE_PG_DATE, null);

  private static final int AMBIGUOUS_FIELD = -1;
  private static final long serialVersionUID = -3076152125004114582L;

  /** Returns the descriptor for the {@code BOOL type}. */
  public static Type bool() {
    return TYPE_BOOL;
  }

  /**
   * Returns the descriptor for the {@code INT64} type: an integral type with the same value domain
   * as a Java {@code long}.
   */
  public static Type int64() {
    return TYPE_INT64;
  }

  /**
   * Returns the descriptor for the {@code FLOAT64} type: a floating point type with the same value
   * domain as a Java {code double}.
   */
  public static Type float64() {
    return TYPE_FLOAT64;
  }

  /** Returns the descriptor for the {@code NUMERIC} type. */
  public static Type numeric() {
    return TYPE_NUMERIC;
  }

  /**
   * Returns the descriptor for the {@code STRING} type: a variable-length Unicode character string.
   */
  public static Type string() {
    return TYPE_STRING;
  }

  /** Returns the descriptor for the {@code JSON} type. */
  public static Type json() {
    return TYPE_JSON;
  }

  /** Returns the descriptor for the {@code BYTES} type: a variable-length byte string. */
  public static Type bytes() {
    return TYPE_BYTES;
  }

  /**
   * Returns the descriptor for the {@code TIMESTAMP} type: a nano precision timestamp in the range
   * [0000-01-01 00:00:00, 9999-12-31 23:59:59.999999999 UTC].
   */
  public static Type timestamp() {
    return TYPE_TIMESTAMP;
  }

  /**
   * Returns the descriptor for the {@code DATE} type: a timezone independent date in the range
   * [1678-01-01, 2262-01-01).
   */
  public static Type date() {
    return TYPE_DATE;
  }

  public static Type pgBool() {
    return TYPE_PG_BOOL;
  }

  public static Type pgInt8() {
    return TYPE_PG_INT8;
  }

  public static Type pgFloat8() {
    return TYPE_PG_FLOAT8;
  }

  public static Type pgText() {
    return TYPE_PG_TEXT;
  }

  public static Type pgVarchar() {
    return TYPE_PG_VARCHAR;
  }

  public static Type pgNumeric() {
    return TYPE_PG_NUMERIC;
  }

  public static Type pgBytea() {
    return TYPE_PG_BYTEA;
  }

  public static Type pgTimestamptz() {
    return TYPE_PG_TIMESTAMPTZ;
  }

  public static Type pgDate() {
    return TYPE_PG_DATE;
  }

  /** Returns a descriptor for an array of {@code elementType}. */
  public static Type array(Type elementType) {
    Preconditions.checkNotNull(elementType);
    switch (elementType.getCode()) {
      case BOOL:
        return TYPE_ARRAY_BOOL;
      case INT64:
        return TYPE_ARRAY_INT64;
      case FLOAT64:
        return TYPE_ARRAY_FLOAT64;
      case NUMERIC:
        return TYPE_ARRAY_NUMERIC;
      case STRING:
        return TYPE_ARRAY_STRING;
      case JSON:
        return TYPE_ARRAY_JSON;
      case BYTES:
        return TYPE_ARRAY_BYTES;
      case TIMESTAMP:
        return TYPE_ARRAY_TIMESTAMP;
      case DATE:
        return TYPE_ARRAY_DATE;
      default:
        return new Type(Code.ARRAY, elementType, null);
    }
  }

  /** Returns a descriptor for an array of PG {@code elementType}. */
  public static Type pgArray(Type elementType) {
    Preconditions.checkNotNull(elementType);
    switch (elementType.getCode()) {
      case PG_BOOL:
        return TYPE_PG_ARRAY_BOOL;
      case PG_INT8:
        return TYPE_PG_ARRAY_INT8;
      case PG_FLOAT8:
        return TYPE_PG_ARRAY_FLOAT8;
      case PG_NUMERIC:
        return TYPE_PG_ARRAY_NUMERIC;
      case PG_VARCHAR:
        return TYPE_PG_ARRAY_VARCHAR;
      case PG_TEXT:
        return TYPE_PG_ARRAY_TEXT;
      case PG_BYTEA:
        return TYPE_PG_ARRAY_BYTEA;
      case PG_TIMESTAMPTZ:
        return TYPE_PG_ARRAY_TIMESTAMPTZ;
      case PG_DATE:
        return TYPE_PG_ARRAY_DATE;
      default:
        throw new IllegalArgumentException("Unknown Array type: Array of " + elementType);
    }
  }

  /**
   * Returns a descriptor for a {@code STRUCT} type: an ordered collection of named and typed
   * fields.
   */
  public static Type struct(Iterable<StructField> fields) {
    return new Type(Code.STRUCT, null, ImmutableList.copyOf(fields));
  }

  /**
   * Returns a descriptor for a {@code STRUCT} type: an ordered collection of named and typed
   * fields.
   */
  public static Type struct(StructField... fields) {
    return new Type(Code.STRUCT, null, ImmutableList.copyOf(fields));
  }

  private final Code code;
  private final Type arrayElementType;
  private final ImmutableList<StructField> structFields;

  /**
   * Map of field name to field index. Ambiguous names are indexed to {@link #AMBIGUOUS_FIELD}. The
   * map is lazily initialized with a benign race.
   */
  private Map<String, Integer> fieldsByName;

  private Type(
      Code code,
      @Nullable Type arrayElementType,
      @Nullable ImmutableList<StructField> structFields) {
    this.code = code;
    this.arrayElementType = arrayElementType;
    this.structFields = structFields;
  }

  /** Enumerates the categories of types. */
  public enum Code {
    BOOL("BOOL", Dialect.GOOGLE_STANDARD_SQL),
    INT64("INT64", Dialect.GOOGLE_STANDARD_SQL),
    NUMERIC("NUMERIC", Dialect.GOOGLE_STANDARD_SQL),
    FLOAT64("FLOAT64", Dialect.GOOGLE_STANDARD_SQL),
    STRING("STRING", Dialect.GOOGLE_STANDARD_SQL),
    JSON("JSON", Dialect.GOOGLE_STANDARD_SQL),
    BYTES("BYTES", Dialect.GOOGLE_STANDARD_SQL),
    TIMESTAMP("TIMESTAMP", Dialect.GOOGLE_STANDARD_SQL),
    DATE("DATE", Dialect.GOOGLE_STANDARD_SQL),
    ARRAY("ARRAY", Dialect.GOOGLE_STANDARD_SQL),
    STRUCT("STRUCT", Dialect.GOOGLE_STANDARD_SQL),
    PG_BOOL("boolean", Dialect.POSTGRESQL),
    PG_INT8("bigint", Dialect.POSTGRESQL),
    PG_FLOAT8("double precision", Dialect.POSTGRESQL),
    PG_TEXT("text", Dialect.POSTGRESQL),
    PG_VARCHAR("character varying", Dialect.POSTGRESQL),
    PG_NUMERIC("numeric", Dialect.POSTGRESQL),
    PG_BYTEA("bytea", Dialect.POSTGRESQL),
    PG_TIMESTAMPTZ("timestamp with time zone", Dialect.POSTGRESQL),
    PG_DATE("date", Dialect.POSTGRESQL),
    PG_ARRAY("array", Dialect.POSTGRESQL);

    private final String name;
    private final Dialect dialect;

    Code(String name, Dialect dialect) {
      this.name = name;
      this.dialect = dialect;
    }

    public String getName() {
      return name;
    }

    public Dialect getDialect() {
      return dialect;
    }
  }

  /** Describes an individual field in a {@code STRUCT type}. */
  public static final class StructField implements Serializable {
    private static final long serialVersionUID = 8640511292704408210L;

    private final String name;
    private final Type type;

    public static StructField of(String name, Type type) {
      return new StructField(name, type);
    }

    private StructField(String name, Type type) {
      this.name = Preconditions.checkNotNull(name);
      this.type = Preconditions.checkNotNull(type);
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StructField that = (StructField) o;
      return name.equals(that.name) && type.equals(that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }

  /** Returns the type code corresponding to this type. */
  public Code getCode() {
    return code;
  }

  /**
   * Returns the type descriptor for elements of this {@code ARRAY} type.
   *
   * @throws IllegalStateException if {@code code() != Code.ARRAY}
   */
  public Type getArrayElementType() {
    Preconditions.checkState(
        code == Code.ARRAY || code == Code.PG_ARRAY, "Illegal call for non-ARRAY type");
    return arrayElementType;
  }

  /**
   * Returns the fields of this {@code STRUCT} type.
   *
   * @return an immutable list of the fields
   * @throws IllegalStateException if {@code code() != Code.STRUCT}
   */
  public List<StructField> getStructFields() {
    Preconditions.checkState(code == Code.STRUCT, "Illegal call for non-STRUCT type");
    return structFields;
  }

  /**
   * Returns the index of the field named {@code fieldName} in this {@code STRUCT} type.
   *
   * @throws IllegalArgumentException if there is not exactly one element of {@link
   *     #getStructFields()} with {@link StructField#getName()} equal to {@code fieldName}
   * @throws IllegalStateException if {@code code() != Code.STRUCT}
   */
  public int getFieldIndex(String fieldName) {
    Preconditions.checkState(code == Code.STRUCT, "Illegal call for non-STRUCT type");

    if (fieldsByName == null) {
      Map<String, Integer> tmp = new TreeMap<>();
      for (int i = 0; i < getStructFields().size(); ++i) {
        Type.StructField field = getStructFields().get(i);
        if (tmp.put(field.getName(), i) != null) {
          // Column name appears more than once: mark as ambiguous.
          tmp.put(field.getName(), AMBIGUOUS_FIELD);
        }
      }
      // Benign race: Java's final field semantics mean that if we see a non-null "fieldsByName",
      // we are guaranteed to see it in a fully initialized state.  It is thus important that we
      // use an ImmutableMap here, which necessarily uses final fields or equivalent reasoning.
      // Since all computations of "fieldsByName" produce the same value, there is no risk of
      // inconsistency.
      fieldsByName = ImmutableMap.copyOf(tmp);
    }

    Integer index = fieldsByName.get(fieldName);
    if (index == null) {
      throw new IllegalArgumentException("Field not found: " + fieldName);
    }
    if (index == AMBIGUOUS_FIELD) {
      throw new IllegalArgumentException("Ambiguous field name: " + fieldName);
    }
    return index;
  }

  void toString(StringBuilder b) {
    if (code == Code.ARRAY) {
      b.append("ARRAY<");
      arrayElementType.toString(b);
      b.append('>');
    } else if (code == Code.PG_ARRAY) {
      arrayElementType.toString(b);
      b.append("[]");
    } else if (code == Code.STRUCT) {
      b.append("STRUCT<");
      for (int i = 0; i < structFields.size(); ++i) {
        if (i > 0) {
          b.append(", ");
        }
        StructField f = structFields.get(i);
        b.append(f.getName()).append(' ');
        f.getType().toString(b);
      }
      b.append('>');
    } else {
      b.append(code.toString());
    }
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    toString(b);
    return b.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Type that = (Type) o;
    return code == that.code
        && Objects.equals(arrayElementType, that.arrayElementType)
        && Objects.equals(structFields, that.structFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, arrayElementType, structFields);
  }
}
