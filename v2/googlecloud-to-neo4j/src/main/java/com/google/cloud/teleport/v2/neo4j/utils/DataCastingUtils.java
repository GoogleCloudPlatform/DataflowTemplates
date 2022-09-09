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

import com.google.cloud.teleport.v2.neo4j.model.enums.PropertyType;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Target;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
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
      DateTimeFormat.forPattern("YYYY-MM-DD HH:MM:SSZ");
  private static final DateTimeFormatter jsDateFormatter = DateTimeFormat.forPattern("YYYY-MM-DD");
  private static final SimpleDateFormat jsTimeFormatter = new SimpleDateFormat("HH:MM:SS");

  public static List<Object> sourceTextToTargetObjects(Row rowOfStrings, Target target) {
    Schema targetSchema = BeamUtils.toBeamSchema(target);
    List<Mapping> targetMappings = target.getMappings();
    List<Object> castVals = new ArrayList<>();

    List<String> missingFields = new ArrayList<>();

    for (Schema.Field field : targetSchema.getFields()) {
      String fieldName = field.getName();
      Schema.FieldType type = field.getType();
      Object objVal = null;

      try {
        objVal = StringUtils.trim(rowOfStrings.getValue(fieldName));
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

      try {
        String strEl = String.valueOf(objVal);
        if (type.getTypeName().isNumericType()) {
          if (type.getTypeName() == Schema.TypeName.DECIMAL) {
            castVals.add(Double.parseDouble(strEl));
          } else if (type.getTypeName() == Schema.TypeName.FLOAT) {
            castVals.add(Float.parseFloat(strEl));
          } else if (type.getTypeName() == Schema.TypeName.DOUBLE) {
            castVals.add(Double.parseDouble(strEl));
          } else {
            castVals.add(Long.parseLong(strEl));
          }
        } else if (type.getTypeName().isLogicalType()) {
          castVals.add(Boolean.parseBoolean(strEl));
        } else if (type.getTypeName().isDateType()) {
          if (strEl.indexOf(':') > 0) {
            DateTime dt = DateTime.parse(strEl, jsDateTimeFormatter);
            LocalDate ldt = LocalDate.of(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
            ldt.atTime(dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute());
            castVals.add(ldt);
          } else {
            DateTime dt = DateTime.parse(strEl, jsDateFormatter);
            LocalDate ldt = LocalDate.of(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
            ldt.atTime(dt.getHourOfDay(), dt.getMinuteOfHour(), dt.getSecondOfMinute());
            castVals.add(ldt);
          }
        } else {
          castVals.add(objVal);
        }
      } catch (Exception e) {
        castVals.add(null);
        LOG.warn("Exception casting {} ({}): {}", fieldName, type.getTypeName().toString(), objVal);
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
          if (type.getTypeName() == Schema.TypeName.DECIMAL) {
            map.put(fieldName, row.getDecimal(fieldName).doubleValue());
          } else if (type.getTypeName() == Schema.TypeName.FLOAT) {
            map.put(fieldName, row.getFloat(fieldName).doubleValue());
          } else if (type.getTypeName() == Schema.TypeName.DOUBLE) {
            map.put(fieldName, row.getDouble(fieldName));
          } else {
            map.put(fieldName, Long.parseLong(String.valueOf(row.getValue(fieldName))));
          }
          // TODO: this is an upstream error.  Dates are coming across as LOGICAL_TYPE.  Logical
          // type identifier does include ":date:"
        } else if ((type.getLogicalType() == null ? "" : type.getLogicalType().getIdentifier())
            .contains(":date:")) {
          // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
          Date parsedDate = sdf.parse(row.getValue(fieldName));
          ZonedDateTime zdt = ZonedDateTime.from(parsedDate.toInstant());
          map.put(fieldName, zdt);
        } else if (type.getTypeName().isDateType()) {
          ReadableDateTime dt = row.getDateTime(fieldName);
          ZonedDateTime zdt =
              ZonedDateTime.ofLocal(
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
          map.put(fieldName, zdt);
        } else if (type.typesEqual(Schema.FieldType.BOOLEAN)) {
          map.put(fieldName, Boolean.parseBoolean(String.valueOf(row.getBoolean(fieldName))));
        } else {
          map.put(fieldName, row.getValue(fieldName));
        }
      } catch (Exception e) {
        LOG.error(
            "Error casting {}, {}, {}: {} [{}]",
            type.getTypeName().name(),
            type.getLogicalType(),
            fieldName,
            row.getValue(fieldName),
            e.getMessage());
        map.put(fieldName, row.getValue(fieldName));
      }
    }
    for (Mapping m : target.getMappings()) {
      // if row is empty continue
      if (listFullOfNulls(row.getValues())) {
        continue;
      }
      String fieldName = m.getField();
      PropertyType targetMappingType = m.getType();
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

  private static boolean listFullOfNulls(List<Object> entries) {
    for (Object key : entries) {
      if (key != null) {
        return false;
      }
    }
    return true;
  }

  private static String mapToString(Map<String, ?> map) {
    return map.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static byte[] asBytes(Object obj) throws IOException {
    try (ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut)) {
      oos.writeObject(obj);
      oos.flush();
      byte[] bytes = bytesOut.toByteArray();
      return bytes;
    }
  }

  private static DateTime asDateTime(Object o) {
    if (o == null) {
      return null;
    }
    DateTime val = null;
    if (o instanceof DateTime) {
      val = ((DateTime) o).toDateTime();
    }
    return val;
  }

  private static Double asDouble(Object o) {
    if (o == null) {
      return null;
    }
    Double val = null;
    if (o instanceof Number) {
      val = ((Number) o).doubleValue();
    }
    return val;
  }

  private static Float asFloat(Object o) {
    if (o == null) {
      return null;
    }
    Float val = null;
    if (o instanceof Number) {
      val = ((Number) o).floatValue();
    }
    return val;
  }

  private static Integer asInteger(Object o) {
    if (o == null) {
      return null;
    }
    Integer val = null;
    if (o instanceof Number) {
      val = ((Number) o).intValue();
    }
    return val;
  }

  private static Boolean asBoolean(Object o) {
    if (o == null) {
      return null;
    }
    Boolean val = null;
    if (o instanceof Boolean) {
      val = ((Boolean) o).booleanValue();
    }
    return val;
  }

  private static String asString(Object o) {
    if (o == null) {
      return null;
    }
    String val = null;
    if (o instanceof String) {
      val = ((String) o);
    }
    return val;
  }
}
