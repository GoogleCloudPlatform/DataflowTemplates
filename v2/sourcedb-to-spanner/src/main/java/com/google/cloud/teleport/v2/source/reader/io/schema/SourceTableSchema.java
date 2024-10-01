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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.RecordDefault;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Value class that encloses both the source table schema as read from the source database's system
 * tables and the avroSchema for the same.
 *
 * <p><b>Note:</b> More details about the schema like foreign key references for example can be
 * added here as and when required.
 */
@AutoValue
public abstract class SourceTableSchema implements Serializable {
  private static final String AVRO_SCHEMA_RECORD_DEFAULT_NAME = "BulkReaderRecord";
  public static final String PAYLOAD_FIELD_NAME = "payload";
  public static final String READ_TIME_STAMP_FIELD_NAME = "read_timestamp";

  public abstract String tableSchemaUUID();

  public abstract String tableName();

  // Source Schema from metadata tables. SourceColumnType is similar to
  // com.google.cloud.teleport.v2.spanner.migrations.schema
  /* TODO(vardhanvthigle):
   * Simplify the builder by integrating with common type mapping converters.
   * The consumer of the builder does not need to build avro schemas once there's a defined mapping between `SourceColumnType` and Avro Schema.
   * The todo is completely internal to reader and does not in any way change the contract that reader exposes to outside.
   */
  public abstract ImmutableMap<String, SourceColumnType> sourceColumnNameToSourceColumnType();

  // Mapped Avro Schema (to unified types) that each row will carry.
  public abstract Schema avroSchema();

  public Schema getAvroPayload() {
    return avroSchema().getField(PAYLOAD_FIELD_NAME).schema();
  }

  /* TODO(vardhanvthigle@): Add more information as needed by other modules, like:
   * primary keys, indexing information, foreign key constraints etc.
   */

  public static Builder builder(SQLDialect dialect) {
    if (dialect == SQLDialect.POSTGRESQL) {
      return builder(MapperType.POSTGRESQL);
    }
    return builder(MapperType.MYSQL);
  }

  public static Builder builder(MapperType mapperType) {
    var builder = new AutoValue_SourceTableSchema.Builder().initialize(mapperType);
    builder.setTableSchemaUUID(UUID.randomUUID().toString());
    return builder;
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableSchemaUUID(String value);

    public abstract Builder setTableName(String value);

    @VisibleForTesting protected UnifiedTypeMapper.MapperType mapperType;

    abstract ImmutableMap.Builder<String, SourceColumnType>
        sourceColumnNameToSourceColumnTypeBuilder();

    private FieldAssembler<RecordDefault<Schema>> payloadFieldAssembler;

    public final Builder addSourceColumnNameToSourceColumnType(
        String sourceColumnName, SourceColumnType sourceColumnType) {
      this.sourceColumnNameToSourceColumnTypeBuilder().put(sourceColumnName, sourceColumnType);
      this.payloadFieldAssembler =
          this.payloadFieldAssembler
              .name(sourceColumnName)
              .type(new UnifiedTypeMapper(this.mapperType).getSchema(sourceColumnType))
              .noDefault();
      return this;
    }

    public Builder() {
      /* Note: DataStream `read_timestamp` is encoded as `timeMillis`. We use `timeMirco` since:
       * 1. To achieve 1 GBPs performance (potentially on a single table), we will be writing 1 to 10 million records per second.
       * (Assuming around 100 to 1000 bytes per record)
       * 2. That level of throughput will make milliseconds less precise than what we need.
       * 3. For example, for writing 1 million records per second, if we are batching 100 records, by Little's law,
       *    we get a mean latency of around 100 micro seconds.
       * 4. A precision finer than micro second granularity will be severely affected by clock skew across machines.
       */
      Schema timeMicroType = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
      this.payloadFieldAssembler =
          org.apache.avro.SchemaBuilder.record(AVRO_SCHEMA_RECORD_DEFAULT_NAME)
              .fields()
              .name(READ_TIME_STAMP_FIELD_NAME)
              .type(timeMicroType)
              .noDefault()
              .name(PAYLOAD_FIELD_NAME)
              .type()
              .record(PAYLOAD_FIELD_NAME)
              .fields();
    }

    abstract Builder setAvroSchema(Schema value);

    abstract SourceTableSchema autoBuild();

    public Builder initialize(UnifiedTypeMapper.MapperType mapperType) {
      this.mapperType = mapperType;
      return this;
    }

    public SourceTableSchema build() {
      this.setAvroSchema(this.payloadFieldAssembler.endRecord().noDefault().endRecord());
      SourceTableSchema sourceTableSchema = autoBuild();
      Preconditions.checkState(
          !sourceTableSchema.sourceColumnNameToSourceColumnType().isEmpty(),
          "SourceSchema must have fields in the payload");
      return sourceTableSchema;
    }
  }
}
