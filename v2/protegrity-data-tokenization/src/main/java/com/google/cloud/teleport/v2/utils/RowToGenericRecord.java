package com.google.cloud.teleport.v2.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;

public class RowToGenericRecord {

  private final Schema schema;

  public RowToGenericRecord(Schema avroSchema) {
    this.schema = avroSchema;
  }

  public GenericRecord getRecordFromRow(Row row) {
    GenericRecord genericRecord = AvroUtils.toGenericRecord(row, schema);
    return genericRecord;
//    return Create.of(genericRecord).withCoder(AvroCoder.of(schema));
  }
}
