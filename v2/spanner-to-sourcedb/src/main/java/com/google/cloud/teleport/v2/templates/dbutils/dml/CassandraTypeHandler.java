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
package com.google.cloud.teleport.v2.templates.dbutils.dml;
import org.json.JSONObject;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

class CassandraTypeHandler {

    @FunctionalInterface
    public interface TypeParser<T> {
        T parse(Object value);
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Boolean} object containing the value represented in cassandra type.
     */
    public static Boolean handleCassandraBoolType(String colName, JSONObject valuesJson) {
        return valuesJson.getBoolean(colName);
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Float} object containing the value represented in cassandra type.
     */
    public static Float handleCassandraFloatType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).floatValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Double} object containing the value represented in cassandra type.
     */
    public static Double handleCassandraDoubleType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigDecimal(colName).doubleValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
     */
    public static ByteBuffer handleCassandraBlobType(String colName, JSONObject valuesJson) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }
        return parseBlobType(colValue);

    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colValue - contains all the key value for current incoming stream.
     *
     * @return a {@link ByteBuffer} object containing the value represented in cassandra type.
     */
    public static ByteBuffer parseBlobType(Object colValue) {
        byte[] byteArray;
        if (colValue instanceof byte[]) {
            byteArray = (byte[]) colValue;
        } else if (colValue instanceof String) {
            byteArray = java.util.Base64.getDecoder().decode((String) colValue);
        } else {
            throw new IllegalArgumentException("Unsupported type for column");
        }
        return ByteBuffer.wrap(byteArray);
    }

    /**
     * Generates a {@link LocalDate} based on the provided {@link CassandraTypeHandler}.
     * <p>
     * This method processes the given JSON object to extract a date value using the specified
     * column name and formatter. It specifically handles the "Cassandra Date" format (yyyy-MM-dd).
     * The resulting {@link LocalDate} represents the date value associated with the column.
     * </p>
     *
     * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
     * @param valuesJson - the JSON object containing all key-value pairs for the current incoming data stream.
     *
     * @return a {@link LocalDate} object containing the date value represented in Cassandra type format.
     * If the column is missing or contains an invalid value, this will return {@code null}.
     */
    public static LocalDate handleCassandraDateType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd");
    }

    /**
     * Generates a {@link LocalDate} based on the provided {@link CassandraTypeHandler}.
     * <p>
     * This method processes the given JSON object to extract a timestamp value using the specified
     * column name and formatter. It specifically handles the "Cassandra Timestamp" format (yyyy-MM-dd'T'HH:mm:ss.SSSZ).
     * The resulting {@link LocalDate} represents the date part of the timestamp value associated with the column.
     * </p>
     *
     * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
     * @param valuesJson - the JSON object containing all key-value pairs for the current incoming data stream.
     *
     * @return a {@link LocalDate} object containing the date value extracted from the timestamp in Cassandra type format.
     * If the column is missing or contains an invalid value, this will return {@code null}.
     */
    public static LocalDate handleCassandraTimestampType(String colName, JSONObject valuesJson) {
        return handleCassandraGenericDateType(colName, valuesJson, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }

    /**
     * A helper method that handles the conversion of a given column value to a {@link LocalDate} based on
     * the specified date format (formatter).
     * <p>
     * This method extracts the value for the given column name from the provided JSON object and parses it
     * into a {@link LocalDate} based on the provided date format. If the value is in an unsupported type or format,
     * an exception is thrown.
     * </p>
     *
     * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
     * @param valuesJson - the JSON object containing all key-value pairs for the current incoming data stream.
     * @param formatter - the date format pattern used to parse the value (e.g., "yyyy-MM-dd").
     *
     * @return a {@link LocalDate} object containing the parsed date value. If the column is missing or invalid,
     *         this method returns {@code null}.
     */
    private static LocalDate handleCassandraGenericDateType(String colName, JSONObject valuesJson, String formatter) {
        Object colValue = valuesJson.opt(colName);
        if (colValue == null) {
            return null;
        }

        if (formatter == null) {
            formatter = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        }

        return parseDate(colName, colValue, formatter);
    }

    /**
     * Parses a column value (String, {@link java.util.Date}, or {@code Long}) into a {@link LocalDate}
     * using the specified date format.
     * <p>
     * This method handles different data types (String, Date, Long) by converting them into a {@link LocalDate}.
     * The provided formatter is used to parse date strings, while other types are converted based on their
     * corresponding representations.
     * </p>
     *
     * @param colName - the key used to fetch the value from the provided {@link JSONObject}.
     * @param colValue - the value to be parsed into a {@link LocalDate}.
     * @param formatter - the date format pattern used to parse date strings.
     *
     * @return a {@link LocalDate} object parsed from the given value.
     * @throws IllegalArgumentException if the value cannot be parsed or is of an unsupported type.
     */
    private static LocalDate parseDate(String colName, Object colValue, String formatter) {
        LocalDate localDate;
        if (colValue instanceof String) {
            try {
                DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(formatter);
                localDate = LocalDate.parse((String) colValue, dateFormatter);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid date format for column " + colName, e);
            }
        } else if (colValue instanceof java.util.Date) {
            localDate = ((java.util.Date) colValue).toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        } else if (colValue instanceof Long) {
            localDate = java.time.Instant.ofEpochMilli((Long) colValue).atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        } else {
            throw new IllegalArgumentException("Unsupported type for column " + colName);
        }
        return localDate;
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link String} object containing String as value represented in cassandra type.
     */
    public static String handleCassandraTextType(String colName, JSONObject valuesJson) {
        return valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link UUID} object containing UUID as value represented in cassandra type.
     */
    public static UUID handleCassandraUuidType(String colName, JSONObject valuesJson) {
        String uuidString = valuesJson.optString(colName, null); // Get the value or null if the key is not found or the value is null

        if (uuidString == null) {
            return null;
        }

        return UUID.fromString(uuidString);

    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Long} object containing Long as value represented in cassandra type.
     */
    public static Long handleCassandraBigintType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).longValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Integer} object containing Integer as value represented in cassandra type.
     */
    public static Integer handleCassandraIntType(String colName, JSONObject valuesJson) {
        return valuesJson.getBigInteger(colName).intValue();
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Long>} object containing List<Long> as value represented in cassandra type.
     */
    public static List<Long> handleInt64ArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, obj -> {
            if (obj instanceof Number) {
                return ((Number) obj).longValue();
            } else if (obj instanceof String) {
                try {
                    return Long.getLong((String) obj);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format for column " + colName, e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported type for column " + colName);
            }
        });
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Long>} object containing Set<Long> as value represented in cassandra type.
     */
    public static Set<Long> handleInt64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleInt64ArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Integer>} object containing Set<Long> as value represented in cassandra type.
     */
    public static List<Integer> handleInt64ArrayAsInt32Array(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Integer>} object containing Set<Integer> as value represented in cassandra type.
     */
    public static Set<Integer> handleInt64ArrayAsInt32Set(String colName, JSONObject valuesJson) {
        return handleInt64ArrayType(colName, valuesJson).stream().map(Long::intValue).collect(Collectors.toSet());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<String>} object containing Set<String> as value represented in cassandra type.
     */
    public static Set<String> handleStringSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleStringArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<String>} object containing List<String> as value represented in cassandra type.
     */
    public static List<String> handleStringArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, String::valueOf);
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Boolean>} object containing List<Boolean> as value represented in cassandra type.
     */
    public static List<Boolean> handleBoolArrayType(String colName, JSONObject valuesJson) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(obj -> obj instanceof String && Boolean.parseBoolean((String) obj))
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Boolean>} object containing Set<Boolean> as value represented in cassandra type.
     */
    public static Set<Boolean> handleBoolSetTypeString(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleBoolArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Double>} object containing List<Double> as value represented in cassandra type.
     */
    public static List<Double> handleFloat64ArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, obj -> {
            if (obj instanceof Number) {
                return ((Number) obj).doubleValue();
            } else if (obj instanceof String) {
                try {
                    return Double.valueOf((String) obj);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format for column " + colName, e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported type for column " + colName);
            }
        });
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Double>} object containing Set<Double> as value represented in cassandra type.
     */
    public static Set<Double> handleFloat64SetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleFloat64ArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Float>} object containing List<Float> as value represented in cassandra type.
     */
    public static List<Float> handleFloatArrayType(String colName, JSONObject valuesJson) {
        return handleFloat64ArrayType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Float>} object containing Set<Float> as value represented in cassandra type.
     */
    public static Set<Float> handleFloatSetType(String colName, JSONObject valuesJson) {
        return handleFloat64SetType(colName, valuesJson).stream().map(Double::floatValue).collect(Collectors.toSet());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<LocalDate>} object containing List<LocalDate> as value represented in cassandra type.
     */
    public static List<LocalDate> handleDateArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, obj -> parseDate(colName, obj, "yyyy-MM-dd"));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<LocalDate>} object containing Set<LocalDate> as value represented in cassandra type.
     */
    public static Set<LocalDate> handleDateSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleDateArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<Timestamp>} object containing List<Timestamp> as value represented in cassandra type.
     */
    public static List<Timestamp> handleTimestampArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, value -> Timestamp.valueOf(parseDate(colName, value, "yyyy-MM-dd'T'HH:mm:ss.SSSX").atStartOfDay()));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<Timestamp>} object containing Set<Timestamp> as value represented in cassandra type.
     */
    public static Set<Timestamp> handleTimestampSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleTimestampArrayType(colName, valuesJson));
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<ByteBuffer>} object containing List<ByteBuffer> as value represented in cassandra type.
     */
    public static List<ByteBuffer> handleByteArrayType(String colName, JSONObject valuesJson) {
        return handleArrayType(colName, valuesJson, CassandraTypeHandler::parseBlobType);
    }
    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link List<T>} object containing List of Type T as value represented in cassandra type which will be assigned runtime.
     */
    public static <T> List<T> handleArrayType(String colName, JSONObject valuesJson, TypeParser<T> parser) {
        return valuesJson.getJSONArray(colName).toList().stream()
                .map(parser::parse)
                .collect(Collectors.toList());
    }

    /**
     * Generates a Type based on the provided {@link CassandraTypeHandler}.
     *
     * @param colName - which is used to fetch Key from valueJSON.
     * @param valuesJson - contains all the key value for current incoming stream.
     *
     * @return a {@link Set<ByteBuffer>} object containing Set<ByteBuffer> as value represented in cassandra type.
     */
    public static Set<ByteBuffer> handleByteSetType(String colName, JSONObject valuesJson) {
        return new HashSet<>(handleByteArrayType(colName, valuesJson));
    }
}
