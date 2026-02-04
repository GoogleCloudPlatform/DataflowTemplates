package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link DoFn} that converts {@link GenericRecord} to {@link ComparisonRecord}.
 */
public class SourceHashFn extends DoFn<GenericRecord, ComparisonRecord> {
  @ProcessElement
  public void processElement(ProcessContext c) {
    c.output(ComparisonRecord.fromAvroRecord(Objects.requireNonNull(c.element())));
  }
}
