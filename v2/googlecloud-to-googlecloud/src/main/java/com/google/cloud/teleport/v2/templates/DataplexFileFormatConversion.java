/*
 * Copyright (C) 2021 Google LLC
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

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormatCsvOptions;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DefaultDataplexClient;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.JsonConverters;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.values.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The {@link DataplexFileFormatConversion} pipeline converts file format of the files from the
 * given asset or the list of entities to, the new converted files are stored in the bucket
 * referenced by the provided output asset.
 */
public class DataplexFileFormatConversion {

  /**
   * The {@link FileFormatConversionOptions} provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface FileFormatConversionOptions extends PipelineOptions {

    @Description("Input asset.")
    String getInputAsset();

    void setInputAsset(String inputAsset);

    @Description("Input entities list.")
    List<String> getInputEntities();

    void setInputEntities(List<String> inputEntities);

    @Description("Output file format. Format: PARQUET, AVRO, or ORC. Default: none.")
    @Required
    OutputFileFormat getOutputFileFormat();

    void setOutputFileFormat(OutputFileFormat outputFileFormat);

    @Description(
        "Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. Default:"
            + " SNAPPY. BZIP2 not supported for PARQUET files.")
    @Default.Enum("SNAPPY")
    DataplexCompression getOutputFileCompression();

    void setOutputFileCompression(DataplexCompression outputFileCompression);

    @Description("Output asset.")
    @Required
    String getOutputAsset();

    void setOutputAsset(String outputAsset);
  }

  /** Supported input file formats. */
  public enum InputFileFormat {
    CSV,
    JSON
  }

  /** Supported output file formats. */
  public enum OutputFileFormat {
    PARQUET(".parquet"),
    AVRO(".avro"),
    ORC(".orc");

    private final String fileSuffix;

    OutputFileFormat(String fileSuffix) {
      this.fileSuffix = fileSuffix;
    }
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) throws IOException {
    FileFormatConversionOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(FileFormatConversionOptions.class);

    run(
        Pipeline.create(options),
        options,
        DefaultDataplexClient.withDefaultClient(),
        DataplexFileFormatConversion::gcsOutputPathFrom);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @return The pipeline result.
   */
  public static PipelineResult run(
      Pipeline pipeline,
      FileFormatConversionOptions options,
      DataplexClient dataplex,
      OutputPathProvider outputPathProvider)
      throws IOException {
    if ((options.getInputAsset() == null) == (options.getInputEntities() == null)) {
      throw new IllegalArgumentException(
          "Either input asset or input entities list must be provided");
    }

    GoogleCloudDataplexV1Asset outputAsset = dataplex.getAsset(options.getOutputAsset());
    if (outputAsset == null
        || outputAsset.getResourceSpec() == null
        || !DataplexAssetResourceSpec.STORAGE_BUCKET
            .name()
            .equals(outputAsset.getResourceSpec().getType())
        || outputAsset.getResourceSpec().getName() == null) {
      throw new IllegalArgumentException(
          "Output asset must be an existing asset with resource spec name being a GCS bucket and"
              + " resource spec type of "
              + DataplexAssetResourceSpec.STORAGE_BUCKET.name());
    }
    String outputBucket = outputAsset.getResourceSpec().getName();

    ImmutableList<GoogleCloudDataplexV1Entity> entities =
        options.getInputAsset() != null
            ? dataplex.getCloudStorageEntities(options.getInputAsset())
            : dataplex.getEntities(options.getInputEntities());

    for (GoogleCloudDataplexV1Entity entity : entities) {
      ImmutableList<GoogleCloudDataplexV1Partition> partitions =
          dataplex.getPartitions(entity.getName());
      if (partitions.isEmpty()) {
        pipeline.apply(
            "Convert entity " + entity.getName(),
            new ConvertFiles(
                entity,
                entityToFileSpec(entity),
                options.getOutputFileFormat(),
                options.getOutputFileCompression(),
                outputPathProvider.outputPathFrom(entity.getDataPath(), outputBucket)));
      } else {
        for (GoogleCloudDataplexV1Partition partition : partitions) {
          pipeline.apply(
              "Convert partition " + partition.getName(),
              new ConvertFiles(
                  entity,
                  partitionToFileSpec(partition),
                  options.getOutputFileFormat(),
                  options.getOutputFileCompression(),
                  outputPathProvider.outputPathFrom(partition.getLocation(), outputBucket)));
        }
      }
    }

    return pipeline.run();
  }

  private static String entityToFileSpec(GoogleCloudDataplexV1Entity entity) {
    if (!Strings.isNullOrEmpty(entity.getDataPathPattern())) {
      return entity.getDataPathPattern();
    }
    return addWildCard(entity.getDataPath());
  }

  private static String partitionToFileSpec(GoogleCloudDataplexV1Partition partition) {
    return addWildCard(partition.getLocation());
  }

  /** Return the output path that is similar to the input, but with a new bucket. */
  private static String gcsOutputPathFrom(String inputPath, String outputBucket) {
    return String.format("gs://%s/%s", outputBucket, GcsPath.fromUri(inputPath).getObject());
  }

  private static String addWildCard(String path) {
    return path.endsWith("/") ? path + "**" : path + "/**";
  }

  private static String ensurePathEndsWithSlash(String path) {
    return path.endsWith("/") ? path : path + '/';
  }

  /** Convert the input file path to a new output file path. */
  @FunctionalInterface
  interface OutputPathProvider {
    String outputPathFrom(String inputPath, String outputBucket);
  }

  private static class ConvertFiles extends PTransform<PBegin, PDone> {
    /** The tag for the headers of the CSV if required. */
    private static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

    /** The tag for the lines of the CSV. */
    private static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

    private final GoogleCloudDataplexV1Entity entity;
    private final String inputFileSpec;
    private final OutputFileFormat outputFileFormat;
    private final DataplexCompression outputFileCompression;
    private final String outputPath;

    protected ConvertFiles(
        GoogleCloudDataplexV1Entity entity,
        String inputFileSpec,
        OutputFileFormat outputFileFormat,
        DataplexCompression outputFileCompression,
        String outputPath) {
      super();
      this.entity = entity;
      this.outputFileFormat = outputFileFormat;
      this.inputFileSpec = inputFileSpec;
      this.outputFileCompression = outputFileCompression;
      this.outputPath = outputPath;
    }

    @Override
    public PDone expand(PBegin input) {
      Schema schema = Schemas.dataplexSchemaToAvro(entity.getSchema());
      String serializedSchema = Schemas.serialize(schema);
      PCollection<GenericRecord> records;
      switch (InputFileFormat.valueOf(entity.getFormat().getFormat())) {
        case CSV:
          records =
              input
                  .apply("ReadCsvFiles", readCsvTransform(entity, inputFileSpec))
                  .get(CSV_LINES)
                  .apply(
                      "ConvertToGenericRecord",
                      ParDo.of(csvToGenericRecordFn(entity, serializedSchema)))
                  .setCoder(AvroCoder.of(GenericRecord.class, schema));
          break;
        case JSON:
          records =
              input
                  .apply("ReadJsonFiles", readJsonTransform(inputFileSpec))
                  .apply(
                      "ConvertToGenericRecord", ParDo.of(jsonToGenericRecordFn(serializedSchema)))
                  .setCoder(AvroCoder.of(GenericRecord.class, schema));
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected input file format: " + entity.getFormat().getFormat());
      }

      Sink<GenericRecord> sink;
      switch (outputFileFormat) {
        case PARQUET:
          sink =
              ParquetIO.sink(schema).withCompressionCodec(outputFileCompression.getParquetCodec());
          break;
        case AVRO:
          sink = AvroIO.<GenericRecord>sink(schema).withCodec(outputFileCompression.getAvroCodec());
          break;
        default:
          throw new UnsupportedOperationException(
              "Output format is not implemented: " + outputFileFormat);
      }

      records.apply(
          "WriteFile(s)",
          FileIO.<GenericRecord>write()
              .via(sink)
              .to(ensurePathEndsWithSlash(outputPath))
              .withPrefix(entityToOutputFilePrefix(entity))
              .withSuffix(outputFileFormat.fileSuffix)
              .withNumShards(1)); // Must be 1 as we can only have 1 file per Dataplex partition.

      return PDone.in(input.getPipeline());
    }

    private static CsvConverters.ReadCsv readCsvTransform(
        GoogleCloudDataplexV1Entity entity, String inputFileSpec) {
      CsvConverters.ReadCsv.Builder builder =
          CsvConverters.ReadCsv.newBuilder()
              .setCsvFormat("Default")
              .setHeaderTag(CSV_HEADERS)
              .setLineTag(CSV_LINES);
      GoogleCloudDataplexV1StorageFormatCsvOptions csvOptions = entity.getFormat().getCsv();
      if (csvOptions == null) {
        return builder
            .setInputFileSpec(inputFileSpec)
            .setFileEncoding("UTF-8")
            .setHasHeaders(false)
            .setDelimiter(",")
            .build();
      }
      if (csvOptions.getHeaderRows() != null && csvOptions.getHeaderRows() > 1) {
        // TODO(olegsa): consider updating CsvConverters.ReadCsv to support multiple headers rows
        //  (probably rare case)
        throw new UnsupportedOperationException(
            "CSV conversion currently doesn't support files with multiple headers.");
      }
      // TODO(olegsa): consider updating CsvConverters.ReadCsv to support
      //  GoogleCloudDataplexV1StorageFormatCsvOptions.getQuote (probably rare case)
      return CsvConverters.ReadCsv.newBuilder()
          .setInputFileSpec(inputFileSpec)
          .setFileEncoding(csvOptions.getEncoding() != null ? csvOptions.getEncoding() : "UTF-8")
          .setHasHeaders(csvOptions.getHeaderRows() != null && csvOptions.getHeaderRows() > 0)
          .setDelimiter(csvOptions.getDelimiter() != null ? csvOptions.getDelimiter() : ",")
          .build();
    }

    private static CsvConverters.StringToGenericRecordFn csvToGenericRecordFn(
        GoogleCloudDataplexV1Entity entity, String serializedSchema) {
      GoogleCloudDataplexV1StorageFormatCsvOptions csvOptions = entity.getFormat().getCsv();
      return new CsvConverters.StringToGenericRecordFn(
              csvOptions != null && csvOptions.getDelimiter() != null
                  ? csvOptions.getDelimiter()
                  : ",")
          .withSerializedSchema(serializedSchema);
    }

    private static JsonConverters.ReadJson readJsonTransform(String inputFileSpec) {
      return JsonConverters.ReadJson.newBuilder().setInputFileSpec(inputFileSpec).build();
    }

    private static JsonConverters.StringToGenericRecordFn jsonToGenericRecordFn(
        String serializedSchema) {
      return new JsonConverters.StringToGenericRecordFn(serializedSchema);
    }

    private static String entityToOutputFilePrefix(GoogleCloudDataplexV1Entity entity) {
      return entity.getName().substring(entity.getName().lastIndexOf('/') + 1);
    }
  }
}
