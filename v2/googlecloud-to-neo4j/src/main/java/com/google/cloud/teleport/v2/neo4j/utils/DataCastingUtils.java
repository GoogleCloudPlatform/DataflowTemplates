/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.ReadableInstant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility functions for casting rows between and to Neo4j properties. */
public class DataCastingUtils {

  /*
   * Cypher type/Java type
   * =========================
   * String/String
   * Integer/Long
   * Float/Double
   * Boolean/Boolean
   * Point/org.neo4j.graphdb.spatial.Point
   * Date/java.time.LocalDate
   * Time/java.time.OffsetTime
   * LocalTime/java.time.LocalTime
   * DateTime/java.time.ZonedDateTime
   * LocalDateTime/java.time.LocalDateTime
   * Duration/java.time.temporal.TemporalAmount
   * Node/org.neo4j.graphdb.Node
   * Relationship/org.neo4j.graphdb.Relationship
   * Path/org.neo4j.graphdb.Path
   */
  private static final Logger LOG = LoggerFactory.getLogger(DataCastingUtils.class);

  private static final DateTimeFormatter jsDateTimeFormatter =
      DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ssZ");
  private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-dd");

  public static List<Object> sourceTextToTargetObjects(Row row, Target target) {
    Schema targetSchema = BeamUtils.toBeamSchema(target);
    List<Mapping> targetMappings = target.getMappings();
    List<Object> castVals = new ArrayList<>();

    List<String> missingFields = new ArrayList<>();

    for (Schema.Field field : targetSchema.getFields()) {
      String fieldName = field.getName();
      Schema.FieldType type = field.getType();
      Object objVal = null;

      try {
        objVal = row.getValue(fieldName);
      } catch (Exception e) {
        LOG.warn("Error getting value for field '{}'", fieldName, e);
      }

      if (objVal == null) {
        String constant = findConstantValue(targetMappings, fieldName);
        if (constant != null) {
          castVals.add(StringUtils.trim(constant));
        } else {
          missingFields.add(fieldName);
          castVals.add(null);
        }
        continue;
      }

      try {
        TypeName typeName = type.getTypeName();
        switch (typeName) {
          case BYTE:
          case INT16:
          case INT32:
          case INT64:
            castVals.add(asLong(objVal));
            break;
          case DECIMAL:
          case FLOAT:
          case DOUBLE:
            castVals.add(asDouble(objVal));
            break;
          case STRING:
            castVals.add(asString(objVal));
            break;
          case DATETIME:
            castVals.add(asDateTime(objVal, ZonedDateTime::from, OffsetDateTime::from));
            break;
          case BOOLEAN:
            castVals.add(asBoolean(objVal));
            break;
          case BYTES:
            castVals.add(asByteArray(objVal));
            break;
          case ARRAY:
          case ITERABLE:
          case MAP:
          case ROW:
            {
              var message =
                  String.format("Mapping '%s' types from text sources is not supported.", typeName);
              LOG.warn(message);
              castVals.add(null);
              break;
            }
          case LOGICAL_TYPE:
            {
              switch (type.getLogicalType().getIdentifier()) {
                case NanosDuration.IDENTIFIER:
                  castVals.add(asDuration(objVal));
                  break;
                case org.apache.beam.sdk.schemas.logicaltypes.Date.IDENTIFIER:
                  castVals.add(asDate(objVal));
                  break;
                case org.apache.beam.sdk.schemas.logicaltypes.DateTime.IDENTIFIER:
                  castVals.add(asDateTime(objVal, LocalDateTime::from));
                  break;
                case org.apache.beam.sdk.schemas.logicaltypes.NanosInstant.IDENTIFIER:
                  try {
                    castVals.add(
                        Instant.from(
                            asDateTime(objVal, ZonedDateTime::from, OffsetDateTime::from)));
                  } catch (DateTimeException e) {
                    var local = (LocalDateTime) asDateTime(objVal, LocalDateTime::from);
                    castVals.add(local.toInstant(ZoneOffset.UTC));
                  }
                  break;
                case Time.IDENTIFIER:
                  castVals.add(asTime(objVal));
                  break;
                default:
                  {
                    var message =
                        String.format(
                            "Mapping '%s' types from text sources is not supported.", typeName);
                    LOG.warn(message);
                    castVals.add(null);
                    break;
                  }
              }

              break;
            }
        }
      } catch (Throwable t) {
        LOG.warn(
            "Invalid value '{}' for type '{}{}'",
            objVal,
            type.getTypeName(),
            type.getTypeName().isLogicalType()
                ? String.format(" (%s)", type.getLogicalType().getIdentifier())
                : "",
            t);
        castVals.add(null);
      }
    }

    if (!missingFields.isEmpty()) {
      LOG.warn("Value for fields {} were not found.", missingFields);
    }

    return castVals;
  }

  private static String findConstantValue(List<Mapping> targetMappings, String fieldName) {
    for (Mapping m : targetMappings) {
      // lookup data type
      if (StringUtils.isNotEmpty(m.getConstant())) {
        if (m.getName().equals(fieldName) || m.getConstant().equals(fieldName)) {
          return m.getConstant();
        }
      }
    }
    return null;
  }

  public static Map<String, Object> rowToNeo4jDataMap(Row row, Target target) {
    Map<String, Object> map = new HashMap<>();

    Schema dataSchema = row.getSchema();
    for (Schema.Field field : dataSchema.getFields()) {
      String fieldName = field.getName();

      map.put(fieldName, fromBeamType(fieldName, field.getType(), row.getValue(fieldName)));
    }

    for (Mapping m : target.getMappings()) {
      // if row is empty continue
      if (listFullOfNulls(row.getValues())) {
        continue;
      }
      // lookup data type
      if (StringUtils.isNotEmpty(m.getConstant())) {
        if (StringUtils.isNotEmpty(m.getName())) {
          map.put(m.getName(), m.getConstant());
        } else {
          map.put(m.getConstant(), m.getConstant());
        }
      }
    }

    return map;
  }

  static Object fromBeamType(String name, Schema.FieldType type, Object value) {
    switch (type.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
        {
          return value;
        }
      case DATETIME:
        {
          ReadableInstant typedValue = castValue(value, ReadableInstant.class);
          if (Objects.isNull(typedValue)) {
            return null;
          }

          return java.time.Instant.ofEpochMilli(typedValue.getMillis()).atOffset(ZoneOffset.UTC);
        }
      case DECIMAL:
        {
          BigDecimal typedValue = castValue(value, BigDecimal.class);
          if (Objects.isNull(typedValue)) {
            return null;
          }

          LOG.warn(
              "Type '{}' is not supported in Neo4j, converting field '{}' to Float64 instead.",
              type.getTypeName(),
              name);
          return typedValue.doubleValue();
        }
      case ARRAY:
      case ITERABLE:
        {
          Collection<?> typedValue = castValue(value, Collection.class);
          if (Objects.isNull(typedValue)) {
            return null;
          }

          return typedValue.stream()
              .map(element -> fromBeamType(name, type.getCollectionElementType(), element))
              .collect(Collectors.toList());
        }
      case MAP:
        {
          Schema.FieldType keyType = type.getMapKeyType();
          Schema.FieldType valueType = type.getMapValueType();
          if (!keyType.getTypeName().isStringType()) {
            var message =
                String.format(
                    "Only strings are supported as MAP key values, found '%s' in field '%s'",
                    keyType.getTypeName(), name);
            LOG.error(message);
            throw new RuntimeException(message);
          }

          Map<?, ?> typedValue = castValue(value, Map.class);
          if (Objects.isNull(typedValue)) {
            return null;
          }

          Map<String, Object> result = new HashMap<>(typedValue.size());
          for (var element : typedValue.entrySet()) {
            result.put(
                castValue(fromBeamType(name, keyType, element.getKey()), String.class),
                fromBeamType(name, valueType, element.getValue()));
          }
          return result;
        }
      case ROW:
        {
          Row typedValue = castValue(value, Row.class);
          if (Objects.isNull(typedValue)) {
            return null;
          }

          Map<String, Object> result = new HashMap<>(typedValue.getFieldCount());
          for (var field : typedValue.getSchema().getFields()) {
            var fieldName = field.getName();
            result.put(
                fieldName,
                fromBeamType(fieldName, field.getType(), typedValue.getValue(fieldName)));
          }
          return result;
        }
      case LOGICAL_TYPE:
        {
          if (Objects.isNull(value)) {
            return null;
          } else if (value instanceof java.time.Instant) {
            return ((java.time.Instant) value).atOffset(ZoneOffset.UTC);
          } else if (value instanceof TemporalAccessor) {
            return value;
          } else if (value instanceof EnumerationType.Value) {
            return ((EnumerationType.Value) value).getValue();
          } else {
            var message =
                String.format(
                    "Field '%s' of type '%s' ('%s') is not supported.",
                    name, type.getTypeName().name(), type.getLogicalType().getIdentifier());
            LOG.error(message);
            throw new RuntimeException(message);
          }
        }
      default:
        {
          var message =
              String.format(
                  "Field '%s' of type '%s' is not supported.", name, type.getTypeName().name());
          LOG.error(message);
          throw new RuntimeException(message);
        }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T castValue(Object value, Class<T> clz) {
    if (Objects.isNull(value)) {
      return null;
    }

    if (clz.isInstance(value)) {
      return (T) value;
    }

    var message =
        String.format(
            "expected value to be of type '%s', but got '%s'",
            clz.getName(), value.getClass().getName());
    LOG.error(message);
    throw new RuntimeException(message);
  }

  static boolean listFullOfNulls(List<Object> entries) {
    if (entries == null) {
      return true;
    }

    for (Object key : entries) {
      if (key != null) {
        return false;
      }
    }
    return true;
  }

  static String mapToString(Map<String, ?> map) {
    if (map == null) {
      return null;
    }

    return map.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }

  static byte[] asByteArray(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return (byte[]) o;
    } else if (o instanceof String) {
      try {
        return Base64.getDecoder().decode((String) o);
      } catch (IllegalArgumentException e) {
        return ((String) o).getBytes(StandardCharsets.UTF_8);
      }
    }

    return o.toString().getBytes(StandardCharsets.UTF_8);
  }

  static java.time.Duration asDuration(Object o) {
    if (o == null) {
      return null;
    }

    try {
      if (o instanceof java.time.Duration) {
        return (java.time.Duration) o;
      }

      return java.time.Duration.parse(o instanceof String ? (String) o : o.toString());
    } catch (DateTimeParseException e) {
      throw new RuntimeException(String.format("Invalid Duration value '%s'.", o), e);
    }
  }

  private static final java.time.format.DateTimeFormatter DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .append(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE)
          .optionalStart()
          .appendLiteral('T')
          .append(java.time.format.DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalStart()
          .appendOffsetId()
          .optionalStart()
          .appendLiteral('[')
          .parseCaseSensitive()
          .appendZoneRegionId()
          .appendLiteral(']')
          .toFormatter();

  static LocalDate asDate(Object o) {
    if (o == null) {
      return null;
    }

    try {
      if (o instanceof TemporalAccessor) {
        return LocalDate.from((TemporalAccessor) o);
      }

      return DATE_FORMATTER.parse(o instanceof String ? (String) o : o.toString(), LocalDate::from);
    } catch (DateTimeParseException e) {
      throw new RuntimeException(String.format("Invalid LocalDate value '%s'.", o), e);
    }
  }

  private static final java.time.format.DateTimeFormatter TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .optionalStart()
          .append(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral('T')
          .optionalEnd()
          .append(java.time.format.DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalStart()
          .appendOffsetId()
          .toFormatter();

  static TemporalAccessor asTime(Object o) {
    if (o == null) {
      return null;
    }

    try {
      if (o instanceof LocalTime) {
        return (LocalTime) o;
      } else if (o instanceof OffsetTime) {
        return (OffsetTime) o;
      } else if (o instanceof TemporalAccessor) {
        var accessor = (TemporalAccessor) o;

        if (accessor.isSupported(ChronoField.OFFSET_SECONDS)) {
          return OffsetTime.from((TemporalAccessor) o);
        } else {
          return LocalTime.from((TemporalAccessor) o);
        }
      }

      return TIME_FORMATTER.parseBest(
          o instanceof String ? (String) o : o.toString(), OffsetTime::from, LocalTime::from);
    } catch (DateTimeParseException e) {
      throw new RuntimeException(String.format("Invalid Time value '%s'.", o), e);
    }
  }

  static TemporalAccessor asDateTime(Object o, TemporalQuery<?>... queries) {
    if (o == null) {
      return null;
    }

    try {
      if (o instanceof java.time.Instant) {
        return ((java.time.Instant) o).atOffset(ZoneOffset.UTC);
      } else if (o instanceof TemporalAccessor) {
        var accessor = (TemporalAccessor) o;

        try {
          // first check if we have time zone information
          var zone = TemporalQueries.zone().queryFrom(accessor);
          if (zone != null) {
            // is it a zone id or a zone offset
            if (!(zone instanceof ZoneOffset)) {
              return ZonedDateTime.from(accessor);
            } else {
              return OffsetDateTime.from(accessor);
            }
          }
        } catch (DateTimeException e) {
          // falling back to LocalDateTime
        }

        return LocalDateTime.from((TemporalAccessor) o);
      }

      if (queries.length > 1) {
        return asDateTime(
            java.time.format.DateTimeFormatter.ISO_DATE_TIME.parseBest(
                o instanceof String ? (String) o : o.toString(), queries));
      }

      return asDateTime(
          java.time.format.DateTimeFormatter.ISO_DATE_TIME.parse(
              o instanceof String ? (String) o : o.toString()));
    } catch (DateTimeException e) {
      throw new RuntimeException(String.format("Invalid DateTime value '%s'.", o), e);
    }
  }

  static Double asDouble(Object o) {
    if (o == null) {
      return null;
    }

    double val;
    if (o instanceof Number) {
      val = ((Number) o).doubleValue();
    } else {
      val = Double.parseDouble(asString(o));
    }

    return val;
  }

  static BigDecimal asBigDecimal(Object o) {
    if (o == null) {
      return null;
    }

    BigDecimal val;
    if (o instanceof Number) {
      val = BigDecimal.valueOf(((Number) o).doubleValue());
    } else {
      val = new BigDecimal(asString(o));
    }

    return val;
  }

  static Float asFloat(Object o) {
    if (o == null) {
      return null;
    }

    float val;
    if (o instanceof Number) {
      val = ((Number) o).floatValue();
    } else {
      val = Float.parseFloat(asString(o));
    }
    return val;
  }

  static Long asLong(Object o) {
    if (o == null) {
      return null;
    }
    long val;
    if (o instanceof Number) {
      val = ((Number) o).longValue();
    } else {
      val = Long.parseLong(asString(o));
    }
    return val;
  }

  static Boolean asBoolean(Object o) {
    if (o == null) {
      return null;
    }
    boolean val;
    if (o instanceof Boolean) {
      val = (Boolean) o;
    } else {
      val = Boolean.parseBoolean(asString(o));
    }
    return val;
  }

  static String asString(Object o) {
    if (o == null) {
      return null;
    }
    String val;
    if (o instanceof String) {
      val = ((String) o);
    } else {
      val = o.toString();
    }
    return StringUtils.trim(val);
  }
}
