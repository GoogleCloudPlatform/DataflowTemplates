/*
 * Copyright (C) 2024 Google Inc.
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
package org.example.pipeline.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.Function;
import java.util.function.Predicate;
import org.joda.time.DateTime;

/**
 * Contains utilities for extracting primitive values from JSON nodes.
 *
 * <p>Performs validation and rejects values which are out of bounds.
 */
public class JsonValueExtractors {

  public interface ValueExtractor<V> {
    String name();

    V extractValue(JsonNode value);

    boolean validate(JsonNode value);
  }

  /**
   * Extracts byte value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Byte> byteValueExtractor() {
    return ValidatingValueExtractor.<Byte>builder()
        .setName("byte")
        .setExtractor(jsonNode -> (byte) jsonNode.intValue())
        .setValidator(
            jsonNode ->
                jsonNode.isIntegralNumber()
                    && jsonNode.canConvertToInt()
                    && jsonNode.intValue() >= Byte.MIN_VALUE
                    && jsonNode.intValue() <= Byte.MAX_VALUE)
        .build();
  }

  /**
   * Extracts short value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Short> shortValueExtractor() {
    return ValidatingValueExtractor.<Short>builder()
        .setName("short")
        .setExtractor(jsonNode -> (short) jsonNode.intValue())
        .setValidator(
            jsonNode ->
                jsonNode.isNumber()
                    && jsonNode.canConvertToInt()
                    && jsonNode.intValue() >= Short.MIN_VALUE
                    && jsonNode.intValue() <= Short.MAX_VALUE)
        .build();
  }

  /**
   * Extracts int value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Integer> intValueExtractor() {
    return ValidatingValueExtractor.<Integer>builder()
        .setName("int")
        .setExtractor(JsonNode::intValue)
        .setValidator(jsonNode -> jsonNode.isNumber() && jsonNode.canConvertToInt())
        .build();
  }

  /**
   * Extracts long value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Long> longValueExtractor() {
    return ValidatingValueExtractor.<Long>builder()
        .setName("long")
        .setExtractor(JsonNode::longValue)
        .setValidator(jsonNode -> jsonNode.isNumber() && jsonNode.canConvertToLong())
        .build();
  }

  /**
   * Extracts float value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Float> floatValueExtractor() {
    return ValidatingValueExtractor.<Float>builder()
        .setName("float")
        .setExtractor(JsonNode::floatValue)
        .setValidator(
            jsonNode ->
                jsonNode.isFloat()

                    // Either floating number which allows lossless conversion to float
                    || (jsonNode.isFloatingPointNumber()
                        && jsonNode.doubleValue() == (double) (float) jsonNode.doubleValue())

                    // Or an integer number which allows lossless conversion to float
                    || (jsonNode.isIntegralNumber()
                        && jsonNode.canConvertToInt()
                        && jsonNode.asInt() == (int) (float) jsonNode.asInt()))
        .build();
  }

  /**
   * Extracts double value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Double> doubleValueExtractor() {
    return ValidatingValueExtractor.<Double>builder()
        .setName("double")
        .setExtractor(JsonNode::doubleValue)
        .setValidator(
            jsonNode ->
                jsonNode.isDouble()

                    // Either a long number which allows lossless conversion to float
                    || (jsonNode.isIntegralNumber()
                        && jsonNode.canConvertToLong()
                        && jsonNode.asLong() == (long) (double) jsonNode.asInt())

                    // Or a decimal number which allows lossless conversion to float
                    || (jsonNode.isFloatingPointNumber()
                        && jsonNode
                            .decimalValue()
                            .equals(BigDecimal.valueOf(jsonNode.doubleValue()))))
        .build();
  }

  /**
   * Extracts boolean value from JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<Boolean> booleanValueExtractor() {
    return ValidatingValueExtractor.<Boolean>builder()
        .setName("boolean")
        .setExtractor(JsonNode::booleanValue)
        .setValidator(JsonNode::isBoolean)
        .build();
  }

  /**
   * Extracts string value from the JsonNode if it is within bounds. We assume all values can be
   * converted to strings, even those which are objects values.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<String> stringValueExtractor() {
    return ValidatingValueExtractor.<String>builder()
        .setName("string")
        .setExtractor(json -> json.isTextual() ? json.textValue() : json.toString())
        .setValidator(json -> true)
        .build();
  }

  /**
   * Extracts BigDecimal from the JsonNode if it is within bounds.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<BigDecimal> decimalValueExtractor() {
    return ValidatingValueExtractor.<BigDecimal>builder()
        .setName("decimal")
        .setExtractor(JsonNode::decimalValue)
        .setValidator(jsonNode -> jsonNode.isNumber())
        .build();
  }

  /**
   * Extracts DateTime from the JsonNode (ISO 8601 format string) if it is valid.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<DateTime> datetimeValueExtractor() {
    return ValidatingValueExtractor.<DateTime>builder()
        .setName("datetime")
        .setExtractor(jsonNode -> DateTime.parse(jsonNode.textValue()))
        .setValidator(JsonNode::isTextual)
        .build();
  }

  /**
   * Extracts LocalDate from the JsonNode (ISO 8601 format string) if it is valid.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<LocalDate> dateValueExtractor() {
    return ValidatingValueExtractor.<LocalDate>builder()
        .setName("date")
        .setExtractor(jsonNode -> LocalDate.parse(jsonNode.textValue()))
        .setValidator(JsonNode::isTextual)
        .build();
  }

  /**
   * Extracts LocalTime from the JsonNode (ISO 8601 format string) if it is valid.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<LocalTime> timeValueExtractor() {
    return ValidatingValueExtractor.<LocalTime>builder()
        .setName("time")
        .setExtractor(jsonNode -> LocalTime.parse(jsonNode.textValue()))
        .setValidator(JsonNode::isTextual)
        .build();
  }

  /**
   * Extracts LocalDateTime from the JsonNode (ISO 8601 format string) if it is valid.
   *
   * <p>Throws {@link UnsupportedJsonExtractionException} if value is out of bounds.
   */
  public static ValueExtractor<LocalDateTime> localDatetimeValueExtractor() {
    return ValidatingValueExtractor.<LocalDateTime>builder()
        .setName("")
        .setExtractor(jsonNode -> LocalDateTime.parse(jsonNode.textValue()))
        .setValidator(JsonNode::isTextual)
        .build();
  }

  public static ValueExtractor<Void> nullValueExtractor() {
    return ValidatingValueExtractor.<Void>builder()
        .setName("")
        .setExtractor(jsonNode -> null)
        .setValidator(JsonNode::isNull)
        .build();
  }

  @AutoValue
  public abstract static class ValidatingValueExtractor<W> implements ValueExtractor<W> {

    abstract Predicate<JsonNode> validator();

    abstract Function<JsonNode, W> extractor();

    static <T> Builder<T> builder() {
      return new AutoValue_JsonValueExtractors_ValidatingValueExtractor.Builder<>();
    }

    @Override
    public W extractValue(JsonNode value) {
      if (!validator().test(value)) {
        throw new UnsupportedJsonExtractionException(
            String.format(
                "Value \"%s\" "
                    + "is out of range for the type of the field defined in the row schema (%s).",
                value.toString(), name()));
      }

      return extractor().apply(value);
    }

    @Override
    public boolean validate(JsonNode value) {
      return validator().test(value);
    }

    @AutoValue.Builder
    abstract static class Builder<W> {
      abstract Builder<W> setName(String name);

      abstract Builder<W> setValidator(Predicate<JsonNode> validator);

      abstract Builder<W> setExtractor(Function<JsonNode, W> extractor);

      abstract ValidatingValueExtractor<W> build();
    }
  }

  /** Gets thrown when Row parsing or serialization fails for any reason. */
  public static class UnsupportedJsonExtractionException extends RuntimeException {

    public UnsupportedJsonExtractionException(String message, Throwable reason) {
      super(message, reason);
    }

    public UnsupportedJsonExtractionException(String message) {
      super(message);
    }
  }
}
