/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Generates a stream of random Cloud Spanner values of type {@link Value}.
 */
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

  public Stream<Value> valueStream(Column column) {
    return Stream.generate(() -> {
      int threshold = nullThreshold;
      if (column.notNull()) {
        threshold = -1;
      }
      if (random.nextInt(100) < threshold) {
        return generateNullValue(column.type());
      }
      return generate(column);
    });
  }

  private Value generateNullValue(Type type) {
    switch (type.getCode()) {
      case BOOL:
        return Value.bool(null);
      case INT64:
        return Value.int64(null);
      case FLOAT64:
        return Value.float64(null);
      case BYTES:
        return Value.bytes(null);
      case STRING:
        return Value.string(null);
      case DATE:
        return Value.date(null);
      case TIMESTAMP:
        return Value.timestamp(null);
      case ARRAY:
        switch (type.getArrayElementType().getCode()) {
          case BOOL:
            return Value.boolArray((boolean[]) null);
          case INT64:
            return Value.int64Array((long[]) null);
          case FLOAT64:
            return Value.float64Array((double[]) null);
          case BYTES:
            return Value.bytesArray(null);
          case STRING:
            return Value.stringArray(null);
          case DATE:
            return Value.dateArray(null);
          case TIMESTAMP:
            return Value.timestampArray(null);
        }
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private Value generate(Column column) {
    Type type = column.type();

    if (type.getCode() != Type.Code.ARRAY) {
      return generateScalar(column);
    }

    switch (type.getArrayElementType().getCode()) {
      case BOOL:
        return Value.boolArray(generateList(random::nextBoolean));
      case INT64:
        return Value.int64Array(generateList(random::nextLong));
      case FLOAT64:
        return Value.float64Array(generateList(random::nextDouble));
      case BYTES:
        return Value.bytesArray(generateList(() -> randomByteArray(column.size())));
      case STRING:
        return Value.stringArray(generateList(() -> randomString(column.size())));
      case DATE:
        return Value.dateArray(generateList(this::randomDate));
      case TIMESTAMP:
        return Value.timestampArray(generateList(this::randomTimestamp));
    }
    throw new IllegalArgumentException("Unexpected type " + type);
  }

  private <T> List<T> generateList(Supplier<T> v) {
    int size = random.nextInt(10);
    List<T> result = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      T value = random.nextInt(100) < arrayNullThreshold ? null : v.get();
      result.add(value);
    }
    return result;
  }

  private Value generateScalar(Column column) {
    Type type = column.type();
    switch (type.getCode()) {
      case BOOL:
        return Value.bool(random.nextBoolean());
      case INT64:
        return Value.int64(random.nextLong());
      case FLOAT64:
        return Value.float64(random.nextDouble());
      case BYTES: {
        return Value.bytes(randomByteArray(column.size()));
      }
      case STRING: {
        return Value.string(randomString(column.size()));
      }
      case DATE: {
        return Value.date(randomDate());
      }
      case TIMESTAMP: {
        return Value.timestamp(randomTimestamp());
      }
    }
    throw new IllegalArgumentException("Unexpected type " + type);
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
