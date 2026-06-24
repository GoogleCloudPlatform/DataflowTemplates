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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.common.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** Generates a stream of random Cloud Spanner values of type {@link Value}. */
public class RandomValueGenerator {

  private final Random random;
  private final int nullThreshold;
  private final int arrayNullThreshold;

  public static RandomValueGenerator defaultInstance() {
    return new RandomValueGenerator(new Random(), 75, 75);
  }

  public RandomValueGenerator(Random random, int nullThreshold, int arrayNullThreshold) {
    this.random = random;
    this.nullThreshold = nullThreshold;
    this.arrayNullThreshold = arrayNullThreshold;
  }

  public Stream<Value> valueStream(Column column, boolean notNull) {
    return Stream.generate(
        () -> {
          int threshold = nullThreshold;
          if (notNull || column.notNull()) {
            threshold = -1;
          }
          if (random.nextInt(100) < threshold) {
            return generateNullOrNaNValue(column.type());
          }
          return generate(column);
        });
  }

  private Value generateNullOrNaNValue(Type type) {
    switch (type.getCode()) {
      case BOOL:
      case PG_BOOL:
        return Value.bool(null);
      case INT64:
      case PG_INT8:
        return Value.int64(null);
      case FLOAT32:
      case PG_FLOAT4:
        if (random.nextBoolean()) {
          return Value.float32(Float.NaN);
        }
        return Value.float32(null);
      case FLOAT64:
      case PG_FLOAT8:
        if (random.nextBoolean()) {
          return Value.float64(Double.NaN);
        }
        return Value.float64(null);
      case BYTES:
      case PG_BYTEA:
        return Value.bytes(null);
      case STRING:
      case PG_TEXT:
      case PG_VARCHAR:
        return Value.string(null);
      case DATE:
      case PG_DATE:
        return Value.date(null);
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
        return Value.timestamp(null);
      case NUMERIC:
        return Value.numeric(null);
      case PG_NUMERIC:
        if (random.nextBoolean()) {
          return Value.pgNumeric("NaN");
        }
        return Value.pgNumeric(null);
      case JSON:
        return Value.json((String) null);
      case PG_JSONB:
        return Value.pgJsonb((String) null);
      case UUID:
      case PG_UUID:
        return Value.string(null);
      case ARRAY:
      case PG_ARRAY:
        switch (type.getArrayElementType().getCode()) {
          case BOOL:
          case PG_BOOL:
            return Value.boolArray((boolean[]) null);
          case INT64:
          case PG_INT8:
            return Value.int64Array((long[]) null);
          case FLOAT32:
          case PG_FLOAT4:
            return Value.float32Array((float[]) null);
          case FLOAT64:
          case PG_FLOAT8:
            return Value.float64Array((double[]) null);
          case BYTES:
          case PG_BYTEA:
            return Value.bytesArray(null);
          case STRING:
          case PG_TEXT:
          case PG_VARCHAR:
            return Value.stringArray(null);
          case JSON:
            return Value.jsonArray(null);
          case PG_JSONB:
            return Value.pgJsonbArray(null);
          case UUID:
          case PG_UUID:
            return Value.uuidArray(null);
          case DATE:
          case PG_DATE:
            return Value.dateArray(null);
          case TIMESTAMP:
          case PG_TIMESTAMPTZ:
            return Value.timestampArray(null);
          case NUMERIC:
            return Value.numericArray(null);
          case PG_NUMERIC:
            return Value.pgNumericArray(null);
        }
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private Value generate(Column column) {
    Type type = column.type();

    if (type.getCode() != Type.Code.ARRAY && type.getCode() != Type.Code.PG_ARRAY) {
      return generateScalar(column);
    }

    switch (type.getArrayElementType().getCode()) {
      case BOOL:
      case PG_BOOL:
        return Value.boolArray(generateList(random::nextBoolean, column));
      case INT64:
      case PG_INT8:
        return Value.int64Array(generateList(random::nextLong, column));
      case FLOAT32:
      case PG_FLOAT4:
        return Value.float32Array(generateList(random::nextFloat, column));
      case FLOAT64:
      case PG_FLOAT8:
        return Value.float64Array(generateList(random::nextDouble, column));
      case BYTES:
      case PG_BYTEA:
        return Value.bytesArray(generateList(() -> randomByteArray(column.size()), column));
      case STRING:
      case PG_VARCHAR:
      case PG_TEXT:
        return Value.stringArray(generateList(() -> randomString(column.size()), column));
      case JSON:
        return Value.jsonArray(generateList(() -> "{\"key\":\"value\"}", column));
      case PG_JSONB:
        return Value.pgJsonbArray(generateList(() -> "{\"key\":\"value\"}", column));
      case UUID:
      case PG_UUID:
        return Value.uuidArray(generateList(() -> java.util.UUID.randomUUID(), column));
      case DATE:
      case PG_DATE:
        return Value.dateArray(generateList(this::randomDate, column));
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
        return Value.timestampArray(generateList(this::randomTimestamp, column));
      case NUMERIC:
        return Value.numericArray(generateList(this::randomNumeric, column));
      case PG_NUMERIC:
        return Value.pgNumericArray(generateList(this::randomPgNumeric, column));
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private <T> List<T> generateList(Supplier<T> v, Column column) {
    int size = random.nextInt(10);
    boolean allowNull = true;
    if (column != null && column.arrayLength() != null) {
      size = column.arrayLength();
      allowNull = false;
    }
    List<T> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      T value = allowNull && random.nextInt(100) < arrayNullThreshold ? null : v.get();
      result.add(value);
    }
    return result;
  }

  private Value generateScalar(Column column) {
    Type type = column.type();
    switch (type.getCode()) {
      case BOOL:
      case PG_BOOL:
        return Value.bool(random.nextBoolean());
      case INT64:
      case PG_INT8:
        return Value.int64(random.nextLong());
      case FLOAT32:
      case PG_FLOAT4:
        return Value.float32(random.nextFloat());
      case FLOAT64:
      case PG_FLOAT8:
        return Value.float64(random.nextDouble());
      case BYTES:
      case PG_BYTEA:
        {
          return Value.bytes(randomByteArray(column.size()));
        }
      case STRING:
      case PG_VARCHAR:
      case PG_TEXT:
        {
          return Value.string(randomString(column.size()));
        }
      case DATE:
      case PG_DATE:
        {
          return Value.date(randomDate());
        }
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
        {
          return Value.timestamp(randomTimestamp());
        }
      case JSON:
        return Value.json("{\"key\":\"value\"}");
      case PG_JSONB:
        return Value.pgJsonb("{\"key\":\"value\"}");
      case UUID:
      case PG_UUID:
        return Value.uuid(java.util.UUID.randomUUID());
      case NUMERIC:
        return Value.numeric(randomNumeric());
      case PG_NUMERIC:
        return Value.pgNumeric(randomPgNumeric());
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private java.math.BigDecimal randomNumeric() {
    int leftSize = random.nextInt(NumericUtils.PRECISION - NumericUtils.SCALE) + 1;
    int rightSize = random.nextInt(NumericUtils.SCALE + 1);
    StringBuilder sb = new StringBuilder();
    if (leftSize == 1) {
      sb.append(0);
    } else {
      sb.append(random.nextInt(9) + 1);
    }
    for (int i = 1; i < leftSize; i++) {
      sb.append(random.nextInt(10));
    }
    if (rightSize > 0) {
      sb.append(".");
      for (int i = 0; i < rightSize; i++) {
        sb.append(random.nextInt(10));
      }
    }
    return new java.math.BigDecimal(sb.toString());
  }

  private String randomPgNumeric() {
    int leftSize = random.nextInt(NumericUtils.PG_MAX_PRECISION - NumericUtils.PG_MAX_SCALE) + 1;
    int rightSize = random.nextInt(NumericUtils.PG_MAX_SCALE + 1);
    StringBuilder sb = new StringBuilder();
    if (leftSize == 1) {
      sb.append(0);
    } else {
      sb.append(random.nextInt(9) + 1);
    }
    for (int i = 1; i < leftSize; i++) {
      sb.append(random.nextInt(10));
    }
    if (rightSize > 0) {
      sb.append(".");
      for (int i = 0; i < rightSize; i++) {
        sb.append(random.nextInt(10));
      }
    }
    return sb.toString();
  }

  private ByteArray randomByteArray(Integer size) {
    size = size == -1 ? 20 : size;
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return ByteArray.copyFrom(bytes);
  }

  private String randomString(Integer size) {
    size = size == -1 ? 20 : size;
    return RandomUtils.randomUtf8(size);
  }

  private Timestamp randomTimestamp() {
    long micros = 3919613394847L + random.nextInt(471179000);
    return Timestamp.ofTimeMicroseconds(micros);
  }

  private Date randomDate() {
    int year = 1980 + random.nextInt(40);
    int month = 1 + random.nextInt(11);
    int day = 1 + random.nextInt(27);
    return Date.fromYearMonthDay(year, month, day);
  }
}
