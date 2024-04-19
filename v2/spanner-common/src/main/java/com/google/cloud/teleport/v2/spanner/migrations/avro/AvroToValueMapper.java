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
package com.google.cloud.teleport.v2.spanner.migrations.avro;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.type.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Provides a mapping from Spanner dialects and types to functions that convert Avro record values
 * to corresponding Spanner `Value` objects.
 *
 * <p>This class uses a nested map structure where the outer map keys are {@link Dialect} enums, and
 * the inner map keys are {@link Type} enums. The values in the inner maps are {@link
 * AvroToValueFunction} instances that perform the actual conversion.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Dialect dialect = Dialect.POSTGRESQL;
 * Type type = Type.pgInt8();
 * Object recordValue = ...; // Avro record value
 * Schema fieldSchema = ...; // Avro field schema
 *
 * Value value = AvroToValueMapper.convertorMap()
 *     .get(dialect)
 *     .get(type)
 *     .apply(recordValue, fieldSchema);
 * }</pre>
 */
public class AvroToValueMapper {

  interface AvroToValueFunction {
    Value apply(Object recordValue, Schema fieldSchema);
  }

  static final Map<Dialect, Map<Type, AvroToValueFunction>> CONVERTOR_MAP = initConvertorMap();

  public static Map<Dialect, Map<Type, AvroToValueFunction>> convertorMap() {
    return CONVERTOR_MAP;
  }

  static Map<Dialect, Map<Type, AvroToValueFunction>> initConvertorMap() {
    Map<Dialect, Map<Type, AvroToValueFunction>> convertorMap = new HashMap<>();
    convertorMap.put(Dialect.GOOGLE_STANDARD_SQL, getGsqlMap());
    convertorMap.put(Dialect.POSTGRESQL, getPgMap());
    return convertorMap;
  }

  static Map<Type, AvroToValueFunction> getGsqlMap() {
    Map<Type, AvroToValueFunction> gsqlFunctions = new HashMap<>();
    gsqlFunctions.put(
        Type.bool(),
        (recordValue, fieldSchema) ->
            Value.bool(GenericRecordTypeConvertor.avroFieldToBoolean(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.int64(),
        (recordValue, fieldSchema) ->
            Value.int64(GenericRecordTypeConvertor.avroFieldToLong(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.float64(),
        (recordValue, fieldSchema) ->
            Value.float64(GenericRecordTypeConvertor.avroFieldToDouble(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.string(), (recordValue, fieldSchema) -> Value.string(recordValue.toString()));
    gsqlFunctions.put(
        Type.json(), (recordValue, fieldSchema) -> Value.string(recordValue.toString()));
    gsqlFunctions.put(
        Type.numeric(),
        (recordValue, fieldSchema) ->
            Value.numeric(
                GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.bytes(),
        (recordValue, fieldSchema) ->
            Value.bytes(GenericRecordTypeConvertor.avroFieldToByteArray(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.timestamp(),
        (recordValue, fieldSchema) ->
            Value.timestamp(
                GenericRecordTypeConvertor.avroFieldToTimestamp(recordValue, fieldSchema)));
    gsqlFunctions.put(
        Type.date(),
        (recordValue, fieldSchema) ->
            Value.date(GenericRecordTypeConvertor.avroFieldToDate(recordValue, fieldSchema)));
    return gsqlFunctions;
  }

  static Map<Type, AvroToValueFunction> getPgMap() {
    Map<Type, AvroToValueFunction> pgFunctions = new HashMap<>();
    pgFunctions.put(
        Type.pgBool(),
        (recordValue, fieldSchema) ->
            Value.bool(GenericRecordTypeConvertor.avroFieldToBoolean(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgInt8(),
        (recordValue, fieldSchema) ->
            Value.int64(GenericRecordTypeConvertor.avroFieldToLong(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgFloat8(),
        (recordValue, fieldSchema) ->
            Value.float64(GenericRecordTypeConvertor.avroFieldToDouble(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgVarchar(), (recordValue, fieldSchema) -> Value.string(recordValue.toString()));
    pgFunctions.put(
        Type.pgText(), (recordValue, fieldSchema) -> Value.string(recordValue.toString()));
    pgFunctions.put(
        Type.pgJsonb(), (recordValue, fieldSchema) -> Value.string(recordValue.toString()));
    pgFunctions.put(
        Type.pgNumeric(),
        (recordValue, fieldSchema) ->
            Value.numeric(
                GenericRecordTypeConvertor.avroFieldToNumericBigDecimal(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgBytea(),
        (recordValue, fieldSchema) ->
            Value.bytes(GenericRecordTypeConvertor.avroFieldToByteArray(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgCommitTimestamp(),
        (recordValue, fieldSchema) ->
            Value.timestamp(
                GenericRecordTypeConvertor.avroFieldToTimestamp(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgTimestamptz(),
        (recordValue, fieldSchema) ->
            Value.timestamp(
                GenericRecordTypeConvertor.avroFieldToTimestamp(recordValue, fieldSchema)));
    pgFunctions.put(
        Type.pgDate(),
        (recordValue, fieldSchema) ->
            Value.date(GenericRecordTypeConvertor.avroFieldToDate(recordValue, fieldSchema)));
    return pgFunctions;
  }
}
