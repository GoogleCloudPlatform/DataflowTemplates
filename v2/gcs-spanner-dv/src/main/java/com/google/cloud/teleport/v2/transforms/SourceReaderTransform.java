package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceReaderTransform extends PTransform<@NotNull PBegin, @NotNull PCollection<ComparisonRecord>> {

  private static final Logger LOG = LoggerFactory.getLogger(SourceReaderTransform.class);

  private final String gcsInputDirectory;

  public SourceReaderTransform(String gcsInputDirectory) {
    this.gcsInputDirectory = gcsInputDirectory;
  }

  @Override
  public @NotNull PCollection<ComparisonRecord> expand(PBegin input) {
    try {
      return input.apply("ReadSourceAvroRecords",
          AvroIO.parseGenericRecords(new ParseAvroFn())
              .from(createAvroFilePattern(gcsInputDirectory))
              //AvroIO is not able to automatically infer the coder for ComparisonRecord so be use
              //coder registry to explicitly set it
              .withCoder(input.getPipeline().getSchemaRegistry().getSchemaCoder(ComparisonRecord.class))
              .withHintMatchesManyFiles());
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  private static String createAvroFilePattern(String inputPath) {
    //clean up trailing "/" if entered by the user mistakenly
    String cleanPath =
        inputPath.endsWith("/") ? inputPath.substring(0, inputPath.length() - 1) : inputPath;
    return cleanPath + "/**/*.avro";
  }

  private static class ParseAvroFn implements SerializableFunction<GenericRecord, ComparisonRecord> {

    @Override
    public ComparisonRecord apply(GenericRecord input) {
      ComparisonRecord comparisonRecord = ComparisonRecord.fromAvroRecord(input);
      LOG.info("source comparison record: {}", comparisonRecord.toString());
      return comparisonRecord;
    }
  }
}
