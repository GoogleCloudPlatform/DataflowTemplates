package com.google.cloud.teleport.v2.dofn;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A {@link SerializableFunction} that returns the input {@link GenericRecord} as is.
 *
 * <p>This is used to allow {@link org.apache.beam.sdk.extensions.avro.io.AvroIO} to parse GenericRecords
 * while deferring the coder inference/setting to a later stage (e.g. explicitly setting {@link
 * com.google.cloud.teleport.v2.coders.GenericRecordCoder}).
 */
public class IdentityGenericRecordFn implements SerializableFunction<GenericRecord, GenericRecord> {
  @Override
  public GenericRecord apply(GenericRecord input) {
    return input;
  }
}
