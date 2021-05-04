package com.google.cloud.teleport.v2.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.CsvOptions;
import com.google.cloud.teleport.v2.options.GcsCommonOptions;
import com.google.cloud.teleport.v2.options.GcsCommonOptions.ReadOptions;
import com.google.cloud.teleport.v2.options.GcsCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.CsvConverters.RowToCsv;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.utils.FailsafeElementToStringCsvSerializableFunction;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * The {@link GoogleCloudStorageIO} class to read and write data in separete formats from GCS. that
 * reads from one or more Avro files and returns a {@link PCollection} of {@link GenericRecord >}.
 */
public class GoogleCloudStorageIO {


  public static Write write(WriteOptions outputOptions) {
    checkNotNull(outputOptions, "outputOptions is required.");
    return new AutoValue_GoogleCloudStorageIO_Write.Builder().setOutputOptions(outputOptions)
        .build();
  }

  public static Read read(ReadOptions inputOptions, Schema beamSchema) {
    checkNotNull(inputOptions, "inputOptions is required.");
    checkNotNull(beamSchema, "beamSchema is required.");
    return new AutoValue_GoogleCloudStorageIO_Read.Builder().setInputOptions(inputOptions)
        .setBeamSchema(beamSchema)
        .build();
  }

  /**
   * Implementation of {@link #write}.
   */
  @AutoValue
  public abstract static class Write {

    public abstract GcsCommonOptions.WriteOptions getOutputOptions();

    abstract Write.Builder toBuilder();

    public WriteCsv csv(CsvOptions csvOptions) {
      return new AutoValue_GoogleCloudStorageIO_WriteCsv.Builder()
          .setCsvOptions(csvOptions).setOutputOptions(getOutputOptions()).build();

    }

    public WriteJson json() {
      return new AutoValue_GoogleCloudStorageIO_WriteJson.Builder()
          .setOutputOptions(getOutputOptions()).build();

    }

    public WriteAvro avro(Schema beamSchema) {
      return new AutoValue_GoogleCloudStorageIO_WriteAvro.Builder()
          .setBeamSchema(beamSchema)
          .setOutputOptions(getOutputOptions()).build();

    }

    @AutoValue.Builder
    abstract static class Builder {


      abstract GoogleCloudStorageIO.Write.Builder setOutputOptions(
          GcsCommonOptions.WriteOptions inputOptions);

      abstract Write autoBuild();

      public Write build() {
        return autoBuild();
      }
    }
  }

  /**
   * Implementation of {@link #read}.
   */
  @AutoValue
  public abstract static class Read {

    abstract GcsCommonOptions.ReadOptions getInputOptions();


    abstract Schema getBeamSchema();


    public ReadCsv csv(CsvOptions csvOptions) {
      return new AutoValue_GoogleCloudStorageIO_ReadCsv.Builder().
          setInputOptions(getInputOptions()).setCsvOptions(csvOptions)
          .setBeamSchema(getBeamSchema()).build();

    }

    public ReadJson json() {
      return new AutoValue_GoogleCloudStorageIO_ReadJson.Builder().
          setInputOptions(getInputOptions()).setBeamSchema(getBeamSchema()).build();

    }

    public ReadAvro avro() {
      return new AutoValue_GoogleCloudStorageIO_ReadAvro.Builder().
          setInputOptions(getInputOptions()).setBeamSchema(getBeamSchema()).build();

    }

    @AutoValue.Builder
    abstract static class Builder {


      abstract GoogleCloudStorageIO.Read.Builder setInputOptions(
          GcsCommonOptions.ReadOptions inputOptions);

      abstract Read autoBuild();

      public Read build() {
        return autoBuild();
      }

      public abstract Builder setBeamSchema(Schema beamSchema);
    }
  }

  /**
   * A {@link PTransform} that takes PCollection of Row and writes them to GCS in CSV format.
   */
  @AutoValue
  public abstract static class WriteCsv extends PTransform<PCollection<Row>, POutput> {

    abstract GcsCommonOptions.WriteOptions getOutputOptions();

    abstract CsvOptions getCsvOptions();

    abstract WriteCsv.Builder toBuilder();

    @Nullable
    abstract Iterable<String> getFieldNames();

    public WriteCsv withFieldNames(Iterable<String> fieldsNames) {
      return toBuilder().setFieldNames(fieldsNames).build();
    }

    @Override
    public POutput expand(PCollection<Row> input) {
      String header = null;
      if (getFieldNames() != null) {
        header = String.join(getCsvOptions().getCsvDelimiter(), getFieldNames());
      }

      String csvDelimiter = getCsvOptions().getCsvDelimiter();
      PCollection<String> csvs = input
          .apply(
              "ConvertToCSV",
              ParDo.of(new RowToCsv(csvDelimiter))
          );

      if (csvs.isBounded() == IsBounded.BOUNDED) {
        return csvs
            .apply(
                "WriteToGCS",
                TextIO.write().to(getOutputOptions().getOutputGcsDirectory()).withHeader(header));
      } else {
        return csvs
            .apply(
                "WriteToGCS",
                TextIO.write().withWindowedWrites().withNumShards(1)
                    .to(getOutputOptions().getOutputGcsDirectory()).withHeader(header));
      }

    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract GoogleCloudStorageIO.WriteCsv.Builder setCsvOptions(CsvOptions csvOptions);

      public abstract GoogleCloudStorageIO.WriteCsv.Builder setFieldNames(
          @Nullable Iterable<String> fieldsNames);

      abstract GoogleCloudStorageIO.WriteCsv.Builder setOutputOptions(
          GcsCommonOptions.WriteOptions inputOptions);


      abstract CsvOptions getCsvOptions();

      abstract WriteCsv autoBuild();

      public WriteCsv build() {
        checkNotNull(getCsvOptions(), "csvOptions is required.");
        return autoBuild();
      }


    }
  }

  /**
   * A {@link PTransform} that takes PCollection of Row and writes them to GCS in JSON format.
   */
  @AutoValue
  public abstract static class WriteJson extends PTransform<PCollection<Row>, POutput> {

    abstract GcsCommonOptions.WriteOptions getOutputOptions();

    abstract WriteJson.Builder toBuilder();

    @Override
    public POutput expand(PCollection<Row> input) {
      PCollection<String> jsons = input.apply("RowsToJSON", ToJson.of());

      if (jsons.isBounded() == IsBounded.BOUNDED) {
        return jsons
            .apply(
                "WriteToGCS",
                TextIO.write().to(getOutputOptions().getOutputGcsDirectory()));
      } else {
        return jsons
            .apply(
                "WriteToGCS",
                TextIO.write().withWindowedWrites().withNumShards(1)
                    .to(getOutputOptions().getOutputGcsDirectory()));
      }
    }

    @AutoValue.Builder
    abstract static class Builder {


      abstract GoogleCloudStorageIO.WriteJson.Builder setOutputOptions(
          GcsCommonOptions.WriteOptions inputOptions);


      abstract WriteJson autoBuild();

      public WriteJson build() {
        return autoBuild();
      }


    }
  }

  /**
   * A {@link PTransform} that takes PCollection of Row and writes them to GCS in AVRO format.
   */
  @AutoValue
  public abstract static class WriteAvro extends PTransform<PCollection<Row>, POutput> {

    abstract GcsCommonOptions.WriteOptions getOutputOptions();

    abstract Schema getBeamSchema();

    abstract WriteAvro.Builder toBuilder();

    public WriteAvro withBeamSchema(Schema beamSchema) {
      return toBuilder().setBeamSchema(beamSchema).build();
    }

    @Override
    public POutput expand(PCollection<Row> input) {
      org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(getBeamSchema());
      return input
          .apply(
              "RowToGenericRecord",
              MapElements.into(TypeDescriptor.of(GenericRecord.class))
                  .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
          .setCoder(AvroCoder.of(GenericRecord.class, avroSchema))
          .apply(
              "WriteToAvro",
              AvroIO.writeGenericRecords(avroSchema)
                  .to(getOutputOptions().getOutputGcsDirectory())
                  .withSuffix(".avro"));
    }

    @AutoValue.Builder
    abstract static class Builder {


      abstract GoogleCloudStorageIO.WriteAvro.Builder setOutputOptions(
          GcsCommonOptions.WriteOptions inputOptions);

      public abstract WriteAvro.Builder setBeamSchema(Schema beamSchema);


      abstract WriteAvro autoBuild();

      public WriteAvro build() {
        return autoBuild();
      }
    }
  }

  /**
   * A {@link PTransform} that reads CSV files from GCS and returns PCollection of Row.
   */
  @AutoValue
  public abstract static class ReadCsv extends PTransform<PBegin, PCollection<Row>> {

    /**
     * The tag for the headers of the CSV if required.
     */
    static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {
    };

    /**
     * The tag for the lines of the CSV.
     */
    static final TupleTag<String> CSV_LINES = new TupleTag<String>() {
    };

    /**
     * The tag for the dead-letter output.
     */
    static final TupleTag<FailsafeElement<String, String>> PROCESSING_DEADLETTER_OUT =
        new TupleTag<FailsafeElement<String, String>>() {
        };

    /**
     * The tag for the main output.
     */
    static final TupleTag<FailsafeElement<String, String>> PROCESSING_OUT =
        new TupleTag<FailsafeElement<String, String>>() {
        };

    abstract GcsCommonOptions.ReadOptions getInputOptions();

    abstract CsvOptions getCsvOptions();

    abstract ReadCsv.Builder toBuilder();

    abstract Schema getBeamSchema();

    @Nullable
    abstract String getDeadLetterFilePattern();

    /**
     *
     */
    public ReadCsv withParseDeadLetterFilePattern(String deadLetterFilePattern) {
      return toBuilder().setDeadLetterFilePattern(deadLetterFilePattern).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      /*
       * Step 1: Read CSV file(s) from Cloud Storage using {@link CsvConverters.ReadCsv}.
       */
      PCollectionTuple csvLines = input
          .apply(
              "ReadCsvFromGcsFiles",
              CsvConverters.ReadCsv.newBuilder()
                  .setCsvFormat(getCsvOptions().getCsvFormat())
                  .setDelimiter(getCsvOptions().getCsvDelimiter())
                  .setHasHeaders(getCsvOptions().getCsvContainsHeaders())
                  .setInputFileSpec(getInputOptions().getInputGcsFilePattern())
                  .setHeaderTag(CSV_HEADERS)
                  .setLineTag(CSV_LINES)
                  .build());


      /*
       * Step 2: Convert lines to Json.
       */
      PCollectionTuple jsons = csvLines.apply(
          "LineToJson",
          CsvConverters.LineToFailsafeJson.newBuilder()
              .setDelimiter(getCsvOptions().getCsvDelimiter())
              .setJsonSchema(BigQueryHelpers
                  .toJsonString(BigQueryUtils.toTableSchema(getBeamSchema()).getFields()))
              .setHeaderTag(CSV_HEADERS)
              .setLineTag(CSV_LINES)
              .setUdfOutputTag(PROCESSING_OUT)
              .setUdfDeadletterTag(PROCESSING_DEADLETTER_OUT)
              .build());

      if (getDeadLetterFilePattern() != null) {
        /*
         * Step 3: Write jsons to dead-letter gcs that were not successfully processed.
         */
        jsons.get(PROCESSING_DEADLETTER_OUT)
            .apply(
                "WriteCsvConversionErrorsToGcs",
                ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                    .setErrorWritePath(getDeadLetterFilePattern())
                    .setTranslateFunction(
                        new FailsafeElementToStringCsvSerializableFunction<>())
                    .build());
      }

      /*
       * Step 4: Get jsons that were successfully processed.
       */
      return jsons.get(PROCESSING_OUT)
          .apply(
              "GetJson",
              MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
          .apply(
              "TransformToBeamRow",
              new JsonToBeamRow(getBeamSchema()));
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract GoogleCloudStorageIO.ReadCsv.Builder setCsvOptions(CsvOptions csvOptions);

      abstract GoogleCloudStorageIO.ReadCsv.Builder setInputOptions(
          GcsCommonOptions.ReadOptions inputOptions);

      public abstract ReadCsv.Builder setBeamSchema(Schema beamSchema);

      public abstract GoogleCloudStorageIO.ReadCsv.Builder setDeadLetterFilePattern(
          @Nullable String deadLetterFilePattern);


      abstract ReadCsv autoBuild();

      public ReadCsv build() {
        return autoBuild();
      }


    }
  }

  /**
   * A {@link PTransform} that reads JSON files from GCS and returns PCollection of Row.
   */
  @AutoValue
  public abstract static class ReadJson extends PTransform<PBegin, PCollection<Row>> {


    abstract GcsCommonOptions.ReadOptions getInputOptions();


    abstract ReadJson.Builder toBuilder();

    abstract Schema getBeamSchema();

    @Nullable
    abstract String getDeadLetterFilePattern();

    public ReadJson withParseDeadLetterFilePattern(String deadLetterFilePattern) {
      return toBuilder().setDeadLetterFilePattern(deadLetterFilePattern).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {

      return input.apply("ReadJsonFromGCSFiles",
          TextIO.read().from(getInputOptions().getInputGcsFilePattern()))
          .apply(
              "TransformToBeamRow",
              new JsonToBeamRow(getBeamSchema()));


    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract GoogleCloudStorageIO.ReadJson.Builder setInputOptions(
          GcsCommonOptions.ReadOptions inputOptions);

      public abstract ReadJson.Builder setBeamSchema(Schema beamSchema);


      public abstract GoogleCloudStorageIO.ReadJson.Builder setDeadLetterFilePattern(
          @Nullable String deadLetterFilePattern);


      abstract ReadJson autoBuild();

      public ReadJson build() {
        return autoBuild();
      }


    }
  }


  /**
   * A {@link PTransform} that reads AVRO files from GCS and returns PCollection of Row.
   */
  @AutoValue
  abstract static class ReadAvro extends PTransform<PBegin, PCollection<Row>> {


    abstract GcsCommonOptions.ReadOptions getInputOptions();


    abstract ReadAvro.Builder toBuilder();

    abstract Schema getBeamSchema();


    @Override
    public PCollection<Row> expand(PBegin input) {

      org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(getBeamSchema());

      return input
          .apply(
              "ReadAvroFiles",
              AvroIO.readGenericRecords(avroSchema)
                  .from(getInputOptions().getInputGcsFilePattern()))
          .apply(
              "GenericRecordToRow",
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via(AvroUtils.getGenericRecordToRowFunction(getBeamSchema())))
          .setCoder(RowCoder.of(getBeamSchema()));


    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract GoogleCloudStorageIO.ReadAvro.Builder setInputOptions(
          GcsCommonOptions.ReadOptions inputOptions);

      public abstract ReadAvro.Builder setBeamSchema(Schema beamSchema);


      abstract ReadAvro autoBuild();

      public ReadAvro build() {
        return autoBuild();
      }


    }
  }

  /**
   * The {@link JsonToBeamRow} converts jsons string to beam rows. TODO @MikhailMedvedevAkvelon
   * remove it after refactoring
   */
  public static class JsonToBeamRow extends PTransform<PCollection<String>, PCollection<Row>> {

    /**
     * String/String Coder for FailsafeElement.
     */
    public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
        FailsafeElementCoder.of(
            NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

    private final Schema schema;

    public JsonToBeamRow(Schema schema) {
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PCollection<String> jsons) {
      JsonToRow.ParseResult rows = jsons
          .apply("JsonToRow",
              JsonToRow.withExceptionReporting(schema).withExtendedErrorInfo());

      return rows.getResults().setRowSchema(schema);
    }
  }

}



