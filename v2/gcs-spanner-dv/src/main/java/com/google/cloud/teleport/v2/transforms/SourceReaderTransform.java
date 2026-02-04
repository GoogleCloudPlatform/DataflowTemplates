package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.dofn.IdentityGenericRecordFn;
import com.google.cloud.teleport.v2.dofn.SourceHashFn;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
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
    return input
        .apply(
            "ReadSourceAvroRecords",
            AvroIO.parseGenericRecords(new IdentityGenericRecordFn())
                .from(createAvroFilePattern(gcsInputDirectory))
                .withCoder(GenericRecordCoder.of())
                .withHintMatchesManyFiles())
        .apply("CalculateSourceRecordHash", ParDo.of(new SourceHashFn()));
  }

  private static String createAvroFilePattern(String inputPath) {
    // clean up trailing "/" if entered by the user mistakenly
    String cleanPath =
        inputPath.endsWith("/") ? inputPath.substring(0, inputPath.length() - 1) : inputPath;
    return cleanPath + "/**/*.avro";
  }
}
