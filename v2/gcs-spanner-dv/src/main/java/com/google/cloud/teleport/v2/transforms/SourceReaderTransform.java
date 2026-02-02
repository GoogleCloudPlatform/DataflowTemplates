package com.google.cloud.teleport.v2.transforms;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceReaderTransform extends PTransform<@NotNull PBegin, @NotNull PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(SourceReaderTransform.class);

  private final String gcsInputDirectory;

  public SourceReaderTransform(String gcsInputDirectory) {
    this.gcsInputDirectory = gcsInputDirectory;
  }

  @Override
  public @NotNull PCollection<String> expand(PBegin input) {
    return input.apply("ReadSourceAvroRecords",
        AvroIO.parseGenericRecords(new ParseAvroFn())
            .from(createAvroFilePattern(gcsInputDirectory))
            .withHintMatchesManyFiles());
  }

  private static String createAvroFilePattern(String inputPath) {
    //clean up trailing "/" if entered by the user mistakenly
    String cleanPath =
        inputPath.endsWith("/") ? inputPath.substring(0, inputPath.length() - 1) : inputPath;
    return cleanPath + "/**/*.avro";
  }

  private static class ParseAvroFn implements SerializableFunction<GenericRecord, String> {

    @Override
    public String apply(GenericRecord input) {
      LOG.info("Avro record: {}", input.toString());
      return input.toString();
    }
  }
}
