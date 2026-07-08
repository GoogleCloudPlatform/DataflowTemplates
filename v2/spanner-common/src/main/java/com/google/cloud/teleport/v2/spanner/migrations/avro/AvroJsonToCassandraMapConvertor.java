/*
 * Copyright (C) 2025 Google LLC
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

import static java.lang.Math.multiplyExact;

import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraAnnotations;
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraType.Kind;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.AvroTypeConvertorException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Convert AvroJson To CassandraMap based on the Spanner Annotations.
 * Note that this mapping will not be needed when spanner supports Cassandra maps as interleaved tables.
 *
 */
public class AvroJsonToCassandraMapConvertor {
  private static final Logger LOG = LoggerFactory.getLogger(AvroJsonToCassandraMapConvertor.class);
  private static final String NULL_REPRESENTATION = "NULL";

  /**
   * Handle A cassandra map represented as Avro Json.
   *
   * @param recordValue - input
   * @param cassandraAnnotations - Annotation for Cassandra type, if not map, input is same as
   *     output.
   * @param fieldName - name of the field. Used for logging purpose only.
   * @param fieldSchema - avro schema of the filed. Used for logging purpose only.
   * @return
   */
  static String handleJsonToMap(
      Object recordValue,
      CassandraAnnotations cassandraAnnotations,
      String fieldName,
      Schema fieldSchema) {
    if (recordValue == null) {
      LOG.debug(
          "Handling Json as map fieldName {}, recordValue {}, fieldSchema {}, cassandraAnnotations {}, ret {}",
          fieldName,
          recordValue,
          fieldSchema,
          cassandraAnnotations,
          NULL_REPRESENTATION);
      return NULL_REPRESENTATION;
    }
    if (!cassandraAnnotations.cassandraType().getKind().equals(Kind.MAP)) {
      LOG.debug(
          "Handling Json as map fieldName {}, recordValue {}, fieldSchema {}, cassandraAnnotations {}, ret {}",
          fieldName,
          recordValue,
          fieldSchema,
          cassandraAnnotations,
          recordValue.toString());
      return recordValue.toString();
    }

    String keyType = cassandraAnnotations.cassandraType().map().keyType();
    String valueType = cassandraAnnotations.cassandraType().map().valueType();

    JsonObject element = JsonParser.parseString(recordValue.toString()).getAsJsonObject();
    JsonObject mappedJson = new JsonObject();

    element
        .asMap()
        .forEach(
            (k, v) -> {
              String mappedKey = mapKeyOrValue(k, keyType);
              String mappedValue = mapKeyOrValue(v, valueType);
              mappedJson.add(mappedKey, new JsonPrimitive(mappedValue));
            });

    LOG.debug(
        "Handling Json as map fieldName {}, recordValue {}, fieldSchema {}, cassandraAnnotations {}, ret {}",
        fieldName,
        recordValue,
        fieldSchema,
        cassandraAnnotations,
        mappedJson);
    return mappedJson.toString();
  }

  @VisibleForTesting
  static String mapKeyOrValue(Object input, String type) {
    if (input instanceof JsonPrimitive) {
      input = ((JsonPrimitive) input).getAsString();
    }
    if (input == null) {
      return NULL_REPRESENTATION;
    }
    MapValue mapper = CASSANDRA_ADAPTER_MAPPERS.getOrDefault(type, toString);
    return mapper.map(input);
  }

  private static final MapValue toString = (value) -> value.toString();
  private static final MapValue toStringLowerCase = (value) -> value.toString().toLowerCase();

  private static final MapValue hexStringToBase64 = (value) -> hexStringToBase64(value);

  private static MapValue daysSinceEpochToYYMMDD = (value) -> daysSinceEpochToYYMMDD(value);
  private static MapValue timestampMicrosecondsToISO8601UTC =
      (value) -> timestampMicrosecondsToISO8601UTC(value);
  private static MapValue durationToString = (value) -> durationToString(value);

  /**
   * Mappers for types that need special handling while representing as a map. Default for all other
   * types is to just stringify the input.
   */
  private static final ImmutableMap<String, MapValue> CASSANDRA_ADAPTER_MAPPERS =
      ImmutableMap.<String, MapValue>builder()
          .put("BOOLEAN", toStringLowerCase)
          .put("BLOB", hexStringToBase64)
          .put("DATE", daysSinceEpochToYYMMDD)
          .put("DURATION", durationToString)
          .put("TIMESTAMP", timestampMicrosecondsToISO8601UTC)
          .put("UUID", toStringLowerCase)
          .put("TIMEUUID", toStringLowerCase)
          .build();

  private interface MapValue {
    String map(Object input);
  }

  private static String durationToString(Object input) {
    JsonObject element = JsonParser.parseString(input.toString()).getAsJsonObject();
    Period period =
        Period.ZERO
            .plusYears(((Number) getOrDefault(element, "years", 0L)).longValue())
            .plusMonths(((Number) getOrDefault(element, "months", 0L)).longValue())
            .plusDays(((Number) getOrDefault(element, "days", 0L)).longValue());
    /*
     * Convert the period to a ISO-8601 period formatted String, such as P6Y3M1D.
     * A zero period will be represented as zero days, 'P0D'.
     * Refer to javadoc for Period#toString.
     */
    String periodIso8061 = period.toString();
    java.time.Duration duration =
        java.time.Duration.ZERO
            .plusHours(((Number) getOrDefault(element, "hours", 0L)).longValue())
            .plusMinutes(((Number) getOrDefault(element, "minutes", 0L)).longValue())
            .plusSeconds(((Number) getOrDefault(element, "seconds", 0L)).longValue())
            .plusNanos(((Number) getOrDefault(element, "nanos", 0L)).longValue());
    /*
     * Convert the duration to a ISO-8601 period formatted String, such as  PT8H6M12.345S
     * refer to javadoc for Duration#toString.
     */
    String durationIso8610 = duration.toString();
    // Convert to ISO-8601 period format.
    if (duration.isZero()) {
      return periodIso8061.toUpperCase();
    } else {
      return (periodIso8061 + StringUtils.removeStartIgnoreCase(durationIso8610, "P"))
          .toUpperCase();
    }
  }

  private static String hexStringToBase64(Object input) {
    // For string avro type, expect hex encoded string.
    String s = input.toString();
    if (s.length() % 2 == 1) {
      s = "0" + s;
    }
    try {
      return Base64.encodeBase64String(Hex.decodeHex(s));
    } catch (DecoderException e) {
      throw new AvroTypeConvertorException(
          "Unable to convert Json Value "
              + input
              + " to Bytes."
              + ", Exception: "
              + e.getMessage());
    }
  }

  private static String daysSinceEpochToYYMMDD(Object input) {
    long daysSinceEpoch = Long.parseLong(input.toString());
    // Calculate the number of seconds from days.
    long secondsSinceEpoch = multiplyExact(daysSinceEpoch, (24 * 60 * 60));

    // Create an Instant from the seconds.
    Instant instant = Instant.ofEpochSecond(secondsSinceEpoch);

    // Format the Instant into the desired date string in UTC.
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.from(ZoneOffset.UTC));
    return formatter.format(instant);
  }

  private static String timestampMicrosecondsToISO8601UTC(Object input) {
    long microseconds = Long.parseLong(input.toString());
    long seconds = microseconds / 1_000_000;
    long nanos = (microseconds % 1_000_000) * 1000; // Convert remaining microseconds to nanoseconds

    Instant instant = Instant.ofEpochSecond(seconds, nanos);
    return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC")).format(instant);
  }

  private static Long getOrDefault(JsonObject element, String name, Long def) {
    if (element.get(name).isJsonNull()) {
      return def;
    }
    return element.get(name).getAsLong();
  }

  private AvroJsonToCassandraMapConvertor() {}
}
