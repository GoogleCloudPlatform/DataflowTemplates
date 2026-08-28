/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.common.io.Resources;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * A centralized helper class designed to reduce boilerplate when generating Avro test data for
 * integration tests. This class abstracts away the loading of standard Avro schemas and constructs
 * complex nested Avro records via a fluent builder.
 */
public class GCSSpannerDVAvroSetupHelper {

  /**
   * Helper function to load an Avro Schema from a resource file.
   *
   * @param resourceName The path to the Avro schema file relative to the resources directory
   * @return The parsed Avro Schema
   */
  public static Schema getSchemaFromAvscFile(String resourceName) {
    try (InputStream is = Resources.getResource(resourceName).openStream()) {
      return new Schema.Parser().parse(is);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load Avro schema from resource: " + resourceName, e);
    }
  }

  /**
   * Defines standard table schemas that are universally used across multiple integration tests.
   * Centralizing these definitions prevents schema drift across tests and minimizes setup code.
   */
  public static class TableDef {
    public static final TableDef USERS =
        new TableDef(
            getSchemaFromAvscFile("GCSSpannerDVAvroSetupHelper/users.avsc"),
            "Users",
            Arrays.asList("user_id", "event_id"));
    public static final TableDef ACCOUNT_ROLES =
        new TableDef(
            getSchemaFromAvscFile("GCSSpannerDVAvroSetupHelper/account_roles.avsc"),
            "AccountRoles",
            Arrays.asList("role_id"));

    public final Schema schema;
    public final String tableName;
    public final List<String> primaryKeys;

    public TableDef(Schema schema, String tableName, List<String> primaryKeys) {
      this.schema = schema;
      this.tableName = tableName;
      this.primaryKeys = primaryKeys;
    }
  }

  /**
   * A fluent builder for constructing Avro {@link GenericRecord}s that conform to the nested schema
   * required by the Spanner Data Validation pipeline.
   *
   * <p>This builder encapsulates two underlying Avro builders: an outer record for metadata
   * (tableName, shardId, primaryKeys) and an inner record for the table's specific column payload.
   */
  public static class RecordBuilder {
    private final GenericRecordBuilder outerBuilder;
    private final GenericRecordBuilder payloadBuilder;
    private final Schema payloadSchema;
    private final String tableName;

    public RecordBuilder(TableDef table, String shardId) {
      this.tableName = table.tableName;
      this.outerBuilder = new GenericRecordBuilder(table.schema);
      this.outerBuilder.set("tableName", table.tableName);
      this.outerBuilder.set("shardId", shardId);
      this.outerBuilder.set("primaryKeys", table.primaryKeys);

      this.payloadSchema = table.schema.getField("payload").schema();
      this.payloadBuilder = new GenericRecordBuilder(payloadSchema);
    }

    public RecordBuilder set(String columnName, Object value) {
      if (payloadSchema.getField(columnName) == null) {
        throw new IllegalArgumentException(
            String.format(
                "Column '%s' does not exist in schema '%s'. Please check for typos.",
                columnName, tableName));
      }
      payloadBuilder.set(columnName, convertToAvroFormat(value));
      return this;
    }

    public GenericRecord build() {
      outerBuilder.set("payload", payloadBuilder.build());
      return outerBuilder.build();
    }
  }

  /**
   * Converts standard Java types into the primitive formats required by Avro logical types.
   *
   * <p>Because this helper uses a generic {@code Map<String, Object>} to dynamically build records,
   * standard Java objects (like {@link Instant}, {@link java.math.BigDecimal}, {@code byte[]}) must
   * be manually translated into Avro's expected underlying primitives (like {@code Long} for
   * timestamp-micros, {@link java.nio.ByteBuffer} for decimals with scale 9 or raw bytes) before
   * serialization.
   *
   * <p><b>NOTE:</b> {@link java.math.BigDecimal} is currently normalized to scale 9 for Avro
   * decimal serialization.
   *
   * @param value The standard Java object provided in the test map.
   * @return The Avro-compatible primitive value ready for serialization.
   */
  private static Object convertToAvroFormat(Object value) {
    if (value == null) {
      return null;
    }
    // TIMESTAMP(fsp) -> timestamp-micros (long)
    if (value instanceof Instant) {
      Instant t = (Instant) value;
      return (t.getEpochSecond() * 1_000_000L) + (t.getNano() / 1000L);
    }

    if (value instanceof java.math.BigDecimal) {
      java.math.BigDecimal bd = (java.math.BigDecimal) value;
      return java.nio.ByteBuffer.wrap(
          bd.setScale(9, java.math.RoundingMode.HALF_UP).unscaledValue().toByteArray());
    }

    if (value instanceof byte[]) {
      return java.nio.ByteBuffer.wrap((byte[]) value);
    }

    // Default fallback (String, Integer, Long, Double, Float)
    return value;
  }
}
