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
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
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
        // LOG.warn("Error getting value: "+fieldName);
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

      TypeName typeName = type.getTypeName();
      try {
        if (typeName.isNumericType()) {
          if (typeName == TypeName.INT16 || typeName == TypeName.INT32) {
            castVals.add(asInteger(objVal));
          } else if (typeName == TypeName.DECIMAL) {
            castVals.add(asBigDecimal(objVal));
          } else if (typeName == TypeName.FLOAT) {
            castVals.add(asFloat(objVal));
          } else if (typeName == TypeName.DOUBLE) {
            castVals.add(asDouble(objVal));
          } else {
            castVals.add(asLong(objVal));
          }
        } else if (typeName == TypeName.BOOLEAN) {
          castVals.add(asBoolean(objVal));
        } else if (typeName.isDateType()) {
          castVals.add(toZonedDateTime(asDateTime(objVal)));
        } else {
          castVals.add(objVal);
        }
      } catch (Exception e) {
        castVals.add(null);
        LOG.warn("Exception casting {} ({}): {}", fieldName, typeName.toString(), objVal);
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
      if (row.getValue(fieldName) == null) {
        map.put(fieldName, null);
        continue;
      }

      Schema.FieldType type = field.getType();

      try {
        // BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE, STRING, DATETIME, BOOLEAN, BYTES,
        // ARRAY, ITERABLE, MAP, ROW, LOGICAL_TYPE;
        // NUMERIC_TYPES; STRING_TYPES; DATE_TYPES; COLLECTION_TYPES; MAP_TYPES; COMPOSITE_TYPES;
        if (type.getTypeName().isNumericType()) {
          if (type.getTypeName() == TypeName.INT16 || type.getTypeName() == TypeName.INT32) {
            map.put(fieldName, asInteger(row.getValue(fieldName)));
          } else if (type.getTypeName() == TypeName.DECIMAL) {
            map.put(fieldName, asDouble(row.getDecimal(fieldName)));
          } else if (type.getTypeName() == TypeName.FLOAT) {
            map.put(fieldName, asDouble(row.getFloat(fieldName)));
          } else if (type.getTypeName() == TypeName.DOUBLE) {
            map.put(fieldName, row.getDouble(fieldName));
          } else {
            map.put(fieldName, asLong(row.getValue(fieldName)));
          }
          // TODO: this is an upstream error.  Dates are coming across as LOGICAL_TYPE.  Logical
          // type identifier does include ":date:"
        } else if (type.getLogicalType() != null
            && type.getLogicalType().getIdentifier().contains(":date:")) {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
          Date parsedDate = sdf.parse(row.getValue(fieldName));
          ZonedDateTime zdt = ZonedDateTime.from(parsedDate.toInstant());
          map.put(fieldName, zdt);
        } else if (type.getTypeName().isDateType()) {
          ReadableDateTime dt = row.getDateTime(fieldName);
          map.put(fieldName, toZonedDateTime(dt));
        } else if (type.typesEqual(Schema.FieldType.BOOLEAN)) {
          map.put(fieldName, asBoolean(row.getBoolean(fieldName)));
        } else {
          map.put(fieldName, row.getValue(fieldName));
        }
      } catch (Exception e) {
        LOG.error(
            "Error casting {}, {}, {}: {}",
            type.getTypeName().name(),
            type.getLogicalType(),
            fieldName,
            row.getValue(fieldName),
            e);
        map.put(fieldName, row.getValue(fieldName));
      }
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

  private static ZonedDateTime toZonedDateTime(ReadableDateTime dt) {
    return ZonedDateTime.ofLocal(
        LocalDateTime.of(
            dt.getYear(),
            dt.getMonthOfYear(),
            dt.getDayOfMonth(),
            dt.getHourOfDay(),
            dt.getMinuteOfHour(),
            dt.getSecondOfMinute(),
            dt.getMillisOfSecond() * 1_000_000),
        ZoneId.of(dt.getZone().getID(), ZoneId.SHORT_IDS),
        ZoneOffset.ofTotalSeconds(dt.getZone().getOffset(dt) / 1000));
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

  static DateTime asDateTime(Object o) {
    if (o == null) {
      return null;
    }
    DateTime val;
    if (o instanceof DateTime) {
      val = ((DateTime) o);
    } else if (o instanceof Instant) {
      val = ((Instant) o).toDateTime();
    } else if (o instanceof java.time.Instant) {
      val = new DateTime(((java.time.Instant) o).toEpochMilli());
    } else if (o instanceof Long) {
      val = new DateTime(o);
    } else {
      String strEl = asString(o);
      if (strEl.indexOf(':') > 0) {
        val = DateTime.parse(strEl, jsDateTimeFormatter);
      } else {
        val = DateTime.parse(strEl, jsDateFormatter);
      }
    }
    return val.withZone(DateTimeZone.UTC);
  }

  static LocalDateTime toLocalDateTime(DateTime dateTime) {
    return LocalDate.of(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth())
        .atTime(dateTime.getHourOfDay(), dateTime.getMinuteOfHour(), dateTime.getSecondOfMinute());
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
      val = new BigDecimal(((Number) o).doubleValue());
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

  static Integer asInteger(Object o) {
    if (o == null) {
      return null;
    }
    int val;
    if (o instanceof Number) {
      val = ((Number) o).intValue();
    } else {
      val = Integer.parseInt(asString(o));
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
