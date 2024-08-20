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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.StreamSupport;

/** Provides functionality to interact with Struct values. */
public class StructHelper {

  private final Struct struct;

  public StructHelper(Struct struct) {
    this.struct = struct;
  }

  public static StructHelper of(Struct struct) {
    return new StructHelper(struct);
  }

  public Struct getStruct() {
    return struct;
  }

  /**
   * Creates a copy of the StructHelper where the Struct is a copy without the omitted column names.
   *
   * @param omittedColumnNames Column names to remove from the struct.
   * @return StructHelper with a Struct without the omitted columns.
   */
  public StructHelper omitColumNames(Iterable<String> omittedColumnNames) {
    return new StructHelper(copyAsBuilderInternal(omittedColumnNames).build());
  }

  /**
   * Gets a copy of the struct as a builder.
   *
   * <p>This allows adding and removing fields from the original struct by other applications.
   *
   * @return Struct as a Struct.Builder.
   */
  public Struct.Builder copyAsBuilder() {
    return copyAsBuilderInternal(null);
  }

  private Struct.Builder copyAsBuilderInternal(Iterable<String> omittedColumnNames) {
    Struct.Builder recordBuilder = Struct.newBuilder();
    ImmutableSet<String> omittedColumnNamesSet =
        omittedColumnNames == null ? ImmutableSet.of() : ImmutableSet.copyOf(omittedColumnNames);
    struct.getType().getStructFields().stream()
        .filter(field -> !omittedColumnNamesSet.contains(field.getName()))
        .forEach(field -> recordBuilder.set(field.getName()).to(struct.getValue(field.getName())));
    return recordBuilder;
  }

  public KeyMaker keyMaker(Iterable<String> primaryKeyColumnNames) {
    return new KeyMaker(primaryKeyColumnNames);
  }

  public KeyMaker keyMaker(
      Iterable<String> primaryKeyColumnNames, Iterable<String> omittedColumnNames) {
    return new KeyMaker(primaryKeyColumnNames, omittedColumnNames);
  }

  /** Creates Keys for Structs. */
  public class KeyMaker {

    private final Iterable<String> primaryKeyColumnNames;

    /**
     * Initializes KeyMaker with primary key column names, which will be added to the key.
     *
     * @param primaryKeyColumnNames List of primary key column names.
     */
    public KeyMaker(Iterable<String> primaryKeyColumnNames) {
      this(primaryKeyColumnNames, ImmutableList.of());
    }

    public KeyMaker(Iterable<String> primaryKeyColumnNames, Iterable<String> omittedColumnNames) {
      ImmutableSet<String> omittedColumns =
          omittedColumnNames == null ? ImmutableSet.of() : ImmutableSet.copyOf(omittedColumnNames);
      this.primaryKeyColumnNames =
          StreamSupport.stream(primaryKeyColumnNames.spliterator(), false)
              .filter(col -> !omittedColumns.contains(col))
              .collect(toImmutableList());
    }

    /**
     * Generates the Key for a given record and (primary) key column names.
     *
     * @return Primary Key for the record.
     */
    public Key createKey() {
      Key.Builder keyBuilder = createKeyBuilderWithPrimaryKeys();
      return keyBuilder.build();
    }

    /**
     * Creates a Key for the primary keys and appends additional Values.
     *
     * @param values Values that will be added to the key, in order.
     * @return Key for the record with additional fields added.
     */
    public Key createKeyWithExtraValues(Value... values) {
      Key.Builder keyBuilder = createKeyBuilderWithPrimaryKeys();
      Arrays.stream(values).forEach(value -> addValueToKeyBuilder(keyBuilder, value));
      return keyBuilder.build();
    }

    private Key.Builder createKeyBuilderWithPrimaryKeys() {
      Key.Builder keyBuilder = Key.newBuilder();
      addRecordFieldsToKeyBuilder(struct, primaryKeyColumnNames, keyBuilder);
      return keyBuilder;
    }

    /**
     * Creates the Key and casts to string format.
     *
     * <p>Useful in cases where there is need for a deterministic coder. Key does not provide this
     * guarantee.
     *
     * @return Record Key in string format.
     */
    public String createKeyString() {
      return createKey().toString();
    }

    /**
     * Adds struct values to the Key builder for the requested column names.
     *
     * <p>Used to generate Keys for records.
     *
     * @param record Row in Struct format for which to create Key.
     * @param columnNames to add to the Key.
     * @param keyBuilder Key Builder where key will be created.
     */
    private void addRecordFieldsToKeyBuilder(
        Struct record, Iterable<String> columnNames, Key.Builder keyBuilder) {
      HashMap<String, StructField> structFieldMap = new HashMap<>();
      record
          .getType()
          .getStructFields()
          .forEach(field -> structFieldMap.put(field.getName(), field));

      columnNames.forEach(
          columnName -> {
            StructField field = structFieldMap.get(columnName);
            if (field == null) {
              throw new RuntimeException(
                  String.format(
                      "Primary key name %s not found in record. Unable to create Key.",
                      columnName));
            }

            Value fieldValue = record.getValue(field.getName());
            addValueToKeyBuilder(keyBuilder, fieldValue);
          });
    }

    private void addValueToKeyBuilder(Key.Builder keyBuilder, Value fieldValue) {
      Type fieldType = fieldValue.getType();

      switch (fieldType.getCode()) {
        case BOOL:
          keyBuilder.append(ValueHelper.of(fieldValue).getBoolOrNull());
          break;
        case BYTES:
          keyBuilder.append(ValueHelper.of(fieldValue).getBytesOrNull());
          break;
        case DATE:
          keyBuilder.append(ValueHelper.of(fieldValue).getDateOrNull());
          break;
        case FLOAT32:
          keyBuilder.append(ValueHelper.of(fieldValue).getFloat32OrNull());
          break;
        case FLOAT64:
          keyBuilder.append(ValueHelper.of(fieldValue).getFloat64OrNull());
          break;
        case INT64:
          keyBuilder.append(ValueHelper.of(fieldValue).getInt64OrNull());
          break;
        case JSON:
          keyBuilder.append(ValueHelper.of(fieldValue).getJsonOrNull());
          break;
        case NUMERIC:
        case PG_NUMERIC:
          keyBuilder.append(ValueHelper.of(fieldValue).getNumericOrNull());
          break;
        case PG_JSONB:
          keyBuilder.append(ValueHelper.of(fieldValue).getPgJsonbOrNull());
          break;
        case STRING:
          keyBuilder.append(ValueHelper.of(fieldValue).getStringOrNull());
          break;
        case TIMESTAMP:
          keyBuilder.append(ValueHelper.of(fieldValue).getTimestampOrNull());
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported Spanner field type %s.", fieldType.getCode()));
      }
    }
  }

  /** Provides functionality to get and work with Values for Structs. */
  public static class ValueHelper {

    private final Value value;

    /**
     * Initializes ValueHelper with a Value.
     *
     * @param value Value for which to create ValueHelper.
     */
    public static ValueHelper of(Value value) {
      return new ValueHelper(value);
    }

    /**
     * Initializes ValueHelper with a Value.
     *
     * @param value Value for which to create ValueHelper.
     */
    public ValueHelper(Value value) {
      this.value = value;
    }

    /** Value types with null values. */
    public static class NullTypes {

      public static final Boolean NULL_BOOLEAN = null;
      public static final ByteArray NULL_BYTES = null;
      public static final Date NULL_DATE = null;
      public static final Float NULL_FLOAT32 = null;
      public static final Double NULL_FLOAT64 = null;

      public static final Integer NULL_INT32 = null;
      public static final Long NULL_INT64 = null;
      public static final String NULL_JSON = null;
      public static final BigDecimal NULL_NUMERIC = null;
      public static final String NULL_STRING = null;
      public static final Timestamp NULL_TIMESTAMP = null;
    }

    public Boolean getBoolOrNull() {
      return value.isNull() ? NullTypes.NULL_BOOLEAN : Boolean.valueOf(value.getBool());
    }

    public ByteArray getBytesOrNull() {
      return value.isNull() ? NullTypes.NULL_BYTES : value.getBytes();
    }

    public Date getDateOrNull() {
      return value.isNull() ? NullTypes.NULL_DATE : value.getDate();
    }

    public Float getFloat32OrNull() {
      return value.isNull() ? NullTypes.NULL_FLOAT32 : Float.valueOf(value.getFloat32());
    }

    public Double getFloat64OrNull() {
      return value.isNull() ? NullTypes.NULL_FLOAT64 : Double.valueOf(value.getFloat64());
    }

    public Long getInt64OrNull() {
      return value.isNull() ? NullTypes.NULL_INT64 : Long.valueOf(value.getInt64());
    }

    public String getJsonOrNull() {
      return value.isNull() ? NullTypes.NULL_JSON : value.getJson();
    }

    public BigDecimal getNumericOrNull() {
      return value.isNull() ? NullTypes.NULL_NUMERIC : value.getNumeric();
    }

    public String getPgJsonbOrNull() {
      return value.isNull() ? NullTypes.NULL_JSON : value.getPgJsonb();
    }

    public String getStringOrNull() {
      return value.isNull() ? NullTypes.NULL_STRING : value.getString();
    }

    public Timestamp getTimestampOrNull() {
      return value.isNull() ? NullTypes.NULL_TIMESTAMP : value.getTimestamp();
    }
  }
}
