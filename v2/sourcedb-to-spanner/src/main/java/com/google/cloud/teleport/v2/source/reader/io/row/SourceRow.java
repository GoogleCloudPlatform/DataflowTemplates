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
package com.google.cloud.teleport.v2.source.reader.io.row;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/**
 * Encapsulates the SourceRow that is generated as a part of reading the source database tables.
 * Provides convenience methods to get the {@code readTime} and the {@code payload}
 */
@AutoValue
public abstract class SourceRow implements Serializable {

  /**
   * Get the tableSchemaUUID for easy lookup for the schemaMappingInterface.
   *
   * @return tableSchemaUUID
   */
  public abstract String tableSchemaUUID();

  /**
   * Get the readTime epoch in microseconds.
   *
   * @return readTime
   */
  public long getReadTimeMicros() {
    return (long) this.record().getRecord().get(SourceTableSchema.READ_TIME_STAMP_FIELD_NAME);
  }

  /**
   * Get Payload which has the actual fields of the source Database.
   *
   * @return payload as GenericRecord.
   */
  public GenericRecord getPayload() {
    return (GenericRecord) this.record().getRecord().get(SourceTableSchema.PAYLOAD_FIELD_NAME);
  }

  abstract SerializableGenericRecord record();

  /**
   * returns an initialized builder for SourceRow.
   *
   * <p><b>Note:</b> The caller has to provide only the payload schema, readTime and just add the
   * fields. The caller does not need to worry about how the SourceRow is represented or encoded.
   *
   * <p><b>Example</b>
   *
   * <pre>
   *     SourceTableSchema.builder(scientistsTablePayloadSchema, readTime)
   *     .setField("firstName", "Albert")
   *     .setField("lastName", "Einstein")
   *     .setField("NobelPrize", true)
   *     .build();
   *    </pre>
   *
   * @param schema schema of the source table.
   * @param readTimeMicros read time.
   * @return builder.
   */
  public static Builder builder(SourceTableSchema schema, long readTimeMicros) {
    var builder = new AutoValue_SourceRow.Builder();
    builder.initialize(schema, readTimeMicros);
    return builder;
  }

  @AutoValue.Builder
  public abstract static class Builder {
    @SuppressWarnings("CheckReturnValue")
    abstract Builder setTableSchemaUUID(String value);

    @SuppressWarnings("CheckReturnValue")
    abstract Builder setRecord(SerializableGenericRecord value);

    private GenericRecordBuilder recordBuilder = null;
    private GenericRecordBuilder payloadBuilder = null;

    abstract SourceRow autoBuild();

    public SourceRow build() {
      this.recordBuilder.set(SourceTableSchema.PAYLOAD_FIELD_NAME, payloadBuilder.build());
      this.setRecord(new SerializableGenericRecord(recordBuilder.build()));
      return autoBuild();
    }

    // Note: AutoValue requires a no-args constructor.

    protected void initialize(SourceTableSchema schema, long readTimeMicros) {
      this.setTableSchemaUUID(schema.tableSchemaUUID());
      this.recordBuilder = new GenericRecordBuilder(schema.avroSchema());
      this.recordBuilder.set(SourceTableSchema.READ_TIME_STAMP_FIELD_NAME, readTimeMicros);
      this.payloadBuilder =
          new GenericRecordBuilder(
              schema.avroSchema().getField(SourceTableSchema.PAYLOAD_FIELD_NAME).schema());
    }

    public Builder setField(String fieldName, Object value) {
      this.payloadBuilder.set(fieldName, value);
      return this;
    }
  }
}
