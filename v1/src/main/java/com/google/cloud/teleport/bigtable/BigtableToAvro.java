/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.bigtable;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.TimestampRange;
import com.google.cloud.teleport.bigtable.BigtableToAvro.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import com.google.protobuf.ByteOutput;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to Avro files in GCS. Currently,
 * filtering on Cloud Bigtable table is not supported.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Bigtable_to_GCS_Avro.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_GCS_Avro",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to Avro Files in Cloud Storage",
    description =
        "The Bigtable to Cloud Storage Avro template is a pipeline that reads data from a Bigtable table and writes it to a Cloud Storage bucket in Avro format. "
            + "You can use the template to move data from Bigtable to Cloud Storage.",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-avro",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })



public class BigtableToAvro {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableToAvro.class);


  /** Options for the export pipeline. */


  public interface Options extends PipelineOptions {
    @TemplateParameter.ProjectId(
        order = 1,
        groupName = "Source",
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project that contains the Bigtable instance that you want to read data from.")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 2,
        groupName = "Source",
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Bigtable instance that contains the table.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @TemplateParameter.Text(
        order = 3,
        groupName = "Source",
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Bigtable table to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        groupName = "Target",
        description = "Output file directory in Cloud Storage",
        helpText = "The Cloud Storage path where data is written.",
        example = "gs://mybucket/somefolder")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        groupName = "Target",
        description = "Avro file prefix",
        helpText = "The prefix of the Avro filename. For example, `output-`.")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);

    @TemplateParameter.Text(
        order = 6,
        groupName = "Source",
        optional = true,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Application profile ID",
        helpText =
            "The ID of the Bigtable application profile to use for the export. If you don't specify an app profile, Bigtable uses the instance's default app profile: https://cloud.google.com/bigtable/docs/app-profiles#default-app-profile.")
    @Default.String("default")
    ValueProvider<String> getBigtableAppProfileId();

    @SuppressWarnings("unused")
    void setBigtableAppProfileId(ValueProvider<String> appProfileId);


    @TemplateParameter.Text(
            order = 7,
            groupName = "Source",
            description = "Start Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)",
            helpText = " Example UTC timestamp 2024-10-27T10:15:30.00Z"
    )
    ValueProvider<String> getStartTimestamp();
    @SuppressWarnings("unused")
    void setStartTimestamp(ValueProvider<String> startTimestamp);


    @TemplateParameter.Text(
            order = 8,
            groupName = "Source",
            description = "End Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)",
            helpText = " Example UTC timestamp 2024-10-27T10:15:30.00Z"
    )
    ValueProvider<String> getEndTimestamp();
    @SuppressWarnings("unused")
    void setEndTimestamp(ValueProvider<String> endTimestamp);

//    @TemplateParameter.Text(
//            order = 7,
//            groupName = "Source",
//            description = "Start Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)",
//            helpText = " Example UTC timestamp 2024-10-27T10:15:30.00Z"
//    )
//    @Description("Start Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)")
//    @Default.String("") // Provide a default value or leave it empty
//    String getStartTimestamp();
//
//    void setStartTimestamp(String startTimestamp1);
//
//
//    @TemplateParameter.Text(
//            order = 8,
//            groupName = "Source",
//            description = "End Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)",
//            helpText = " Example UTC timestamp 2024-10-27T10:15:30.00Z"
//    )
//    @Description("End Timestamp in UTC Format (YYYY-MM-DDTHH:MM:SSZ)")
//    @Default.String("")
//    String getEndTimestamp();
//
//    void setEndTimestamp(String endTimestamp1);

  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to Avro files in GCS.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    SdkHarnessOptions loggingOptions = options.as(SdkHarnessOptions.class);
    loggingOptions.setDefaultSdkHarnessLogLevel(SdkHarnessOptions.LogLevel.TRACE);

    PipelineResult result = run(options);


    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
  }

  public static long convertUtcToMicros(String utcTimestamp) {
    DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
    long microsTimestamp = parser.parseDateTime(utcTimestamp).getMillis() * 1000;
    LOG.debug("Converting UTC timestamp {} to microseconds: {}", utcTimestamp, microsTimestamp);
    return microsTimestamp;
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));

    // Create a function to build the RowFilter based on the timestamps
    SerializableFunction<String, Long> timestampConverter = utcTimestamp -> {
      if (utcTimestamp == null || utcTimestamp.isEmpty()) {
        return null;
      }
      DateTimeFormatter parser = ISODateTimeFormat.dateTimeParser();
      return parser.parseDateTime(utcTimestamp).getMillis() * 1000;
    };

    ValueProvider<RowFilter> filterProvider = new DualInputNestedValueProvider<>(
            options.getStartTimestamp(),
            options.getEndTimestamp(),
            (TranslatorInput<String, String> input) -> {
              Long startMicros = timestampConverter.apply(input.getX());
              Long endMicros = timestampConverter.apply(input.getY());

              if (startMicros == null && endMicros == null) {
                return null;
              }

              com.google.cloud.bigtable.data.v2.models.Filters.TimestampRangeFilter filterBuilder =
                      FILTERS.timestamp().range();

              if (startMicros != null) {
                filterBuilder.startClosed(startMicros);
              }
              if (endMicros != null) {
                filterBuilder.endOpen(endMicros);
              }

              return filterBuilder.toProto();
            });

    BigtableIO.Read read =
            BigtableIO.read()
                    .withProjectId(options.getBigtableProjectId())
                    .withInstanceId(options.getBigtableInstanceId())
                    .withAppProfileId(options.getBigtableAppProfileId())
                    .withTableId(options.getBigtableTableId());

    // Only add filter if timestamps are provided
    if (options.getStartTimestamp() != null || options.getEndTimestamp() != null) {
      read = read.withRowFilter(filterProvider);
    }

    // Do not validate input fields if it is running as a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
      read = read.withoutValidation();
    }
//    String startTimestamp = options.getStartTimestamp();
////    String endTimestamp = options.getEndTimestamp();
////
////    long startMicros = 0;
////    long endMicros = 0;
////
////    long epochMillis1 = Long.parseLong(startTimestamp);
////    long epochMillis2 = Long.parseLong(endTimestamp);
//
//
//    if (startTimestamp != null && !startTimestamp.isEmpty()) {
//      startMicros = convertUtcToMicros(startTimestamp);
//      LOG.info("Start timestamp microseconds: {}", startMicros);
//    }
//
//    if (endTimestamp != null && !endTimestamp.isEmpty()) {
//      endMicros = convertUtcToMicros(endTimestamp);
//      LOG.info("End timestamp microseconds: {}", endMicros);
//    }
//
//    RowFilter actualFilter =
//            FILTERS.timestamp().range().startClosed(epochMillis1).endOpen(epochMillis2).toProto();

//    long startMicros =0;
//    RowFilter.Builder filterBuilder = RowFilter.newBuilder();
//    TimestampRange.Builder timestampRangeBuilder = TimestampRange.newBuilder();
//    if (options.getStartTimestamp() != null && options.getStartTimestamp().isAccessible()) {
//      String startTimestamp = options.getStartTimestamp().get();
//      LOG.info("Processing start timestamp: {}", startTimestamp);
//      if (startTimestamp != null && !startTimestamp.isEmpty()) {
//         startMicros = convertUtcToMicros(startTimestamp);
//        timestampRangeBuilder.setStartTimestampMicros(startMicros);
//        LOG.info("Set start timestamp filter to {} microseconds", startMicros);
//      }
//    }
//
//    long endMicros=0;
//
//    if (options.getEndTimestamp() != null && options.getEndTimestamp().isAccessible()) {
//      String endTimestamp = options.getEndTimestamp().get();
//      LOG.info("Processing end timestamp: {}", endTimestamp);
//      if (endTimestamp != null && !endTimestamp.isEmpty()) {
//         endMicros = convertUtcToMicros(endTimestamp);
//        timestampRangeBuilder.setEndTimestampMicros(endMicros);
//        LOG.info("Set end timestamp filter to {} microseconds", endMicros);
//      }
//    }
//
//    TimestampRange timestampRange = timestampRangeBuilder.build();
//    RowFilter filter = filterBuilder
//            .setTimestampRangeFilter(timestampRange)
//            .build();
//
//    LOG.info("Created row filter with timestamp range: {}", filter);
//
//    RowFilter actualFilter =
//            FILTERS.timestamp().range().startClosed(startMicros).endOpen(endMicros).toProto();

//    BigtableIO.Read read =
//        BigtableIO.read()
//            .withProjectId(options.getBigtableProjectId())
//            .withInstanceId(options.getBigtableInstanceId())
//            .withAppProfileId(options.getBigtableAppProfileId())
//            .withTableId(options.getBigtableTableId())
//             .withRowFilter(actualFilter);



    // Do not validate input fields if it is running as a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
      read = read.withoutValidation();
    }

    ValueProvider<String> filePathPrefix =
        DualInputNestedValueProvider.of(
            options.getOutputDirectory(),
            options.getFilenamePrefix(),
            new SerializableFunction<TranslatorInput<String, String>, String>() {
              @Override
              public String apply(TranslatorInput<String, String> input) {
                return FileSystems.matchNewResource(input.getX(), true)
                    .resolve(input.getY(), StandardResolveOptions.RESOLVE_FILE)
                    .toString();
              }
            });

    pipeline
        .apply("Read from Bigtable", read)
        .apply("Transform to Avro", MapElements.via(new BigtableToAvroFn()))
        .apply(
            "Write to Avro in GCS",
            AvroIO.write(BigtableRow.class).to(filePathPrefix).withSuffix(".avro"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to Avro {@link BigtableRow}. */
  static class BigtableToAvroFn extends SimpleFunction<Row, BigtableRow> {
    @Override
    public BigtableRow apply(Row row) {
      ByteBuffer key = ByteBuffer.wrap(toByteArray(row.getKey()));
      List<BigtableCell> cells = new ArrayList<>();
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          ByteBuffer qualifier = ByteBuffer.wrap(toByteArray(column.getQualifier()));
          for (Cell cell : column.getCellsList()) {
            long timestamp = cell.getTimestampMicros();
            ByteBuffer value = ByteBuffer.wrap(toByteArray(cell.getValue()));
            cells.add(new BigtableCell(familyName, qualifier, timestamp, value));
          }
        }
      }
      return new BigtableRow(key, cells);
    }
  }

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   *
   * @param byteString A {@link ByteString} from which to extract the array.
   * @return an array of byte.
   */
  protected static byte[] toByteArray(final ByteString byteString) {
    try {
      ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
      UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
      return byteOutput.bytes;
    } catch (IOException e) {
      return byteString.toByteArray();
    }
  }

  private static final class ZeroCopyByteOutput extends ByteOutput {
    private byte[] bytes;

    @Override
    public void writeLazy(byte[] value, int offset, int length) {
      if (offset != 0 || length != value.length) {
        throw new UnsupportedOperationException();
      }
      bytes = value;
    }

    @Override
    public void write(byte value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) {
      throw new UnsupportedOperationException();
    }
  }
}
