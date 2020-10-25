/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.avro.AvroPubsubMessageRecord;
import com.google.cloud.teleport.avro.GenericAvroTransform;
import com.google.cloud.teleport.util.DurationUtils;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and outputs the raw data into
 * windowed Avro files at the specified output directory.
 *
 * <p>Files output will have the following schema:
 *
 * <pre>
 *   {
 *    "type": "record",
 *    "name": "AvroPubsubMessageRecord",
 *    "namespace": "com.google.cloud.teleport.avro",
 *    "fields": [
 *    {"name": "message", "type": {"type": "array", "items": "bytes"}},
 *    {"name": "attributes", "type": {"type": "map", "values": "string"}},
 *    {"name": "timestamp", "type": "long"}
 *    ]
 *   }
 * </pre>
 *
 * <p>Example Usage:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.teleport.templates.${PIPELINE_NAME} \
 *   -Dexec.cleanupDaemonThreads=false \
 *   -Dexec.args=" \
 *   --project=${PROJECT_ID} \
 *   --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 *   --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 *   --runner=DataflowRunner \
 *   --windowDuration=2m \
 *   --numShards=1 \
 *   --topic=projects/${PROJECT_ID}/topics/windowed-files \
 *   --outputDirectory=gs://${PROJECT_ID}/temp/ \
 *   --outputFilenamePrefix=windowed-file \
 *   --outputFilenameSuffix=.avro
 *   --avroTempDirectory=gs://${PROJECT_ID}/avro-temp-dir/"
 * </pre>
 */
public class PubsubToGenericAvro {

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    ValueProvider<String> getInputTopic();

    void setInputTopic(ValueProvider<String> value);

    @Description("The directory to output files to. Must end with a slash.")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description("The filename prefix of the files to write to.")
    @Default.String("output")
    ValueProvider<String> getOutputFilenamePrefix();

    void setOutputFilenamePrefix(ValueProvider<String> value);

    @Description("The suffix of the files to write.")
    @Default.String("")
    ValueProvider<String> getOutputFilenameSuffix();

    void setOutputFilenameSuffix(ValueProvider<String> value);

    @Description(
        "The shard template of the output file. Specified as repeating sequences "
            + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
            + "shard number, or number of shards respectively")
    @Default.String("W-P-SS-of-NN")
    ValueProvider<String> getOutputShardTemplate();

    void setOutputShardTemplate(ValueProvider<String> value);

    @Description("The maximum number of output shards produced when writing.")
    @Default.Integer(1)
    Integer getNumShards();

    void setNumShards(Integer value);

    @Description(
        "The window duration in which data will be written. Defaults to 5m. "
            + "Allowed formats are: "
            + "Ns (for seconds, example: 5s), "
            + "Nm (for minutes, example: 12m), "
            + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();

    void setWindowDuration(String value);

    @Description("The Avro Write Temporary Directory. Must end with /")
    @Required
    ValueProvider<String> getAvroTempDirectory();

    void setAvroTempDirectory(ValueProvider<String> value);

    @Description("The bucket directory, or REST endpoint, from gets AVRO schemas. " +
        "Internal operation concat the global-id at the end")
    @Required
    ValueProvider<String> getSchemaResgitryUri();

    void setSchemaResgitryUri(ValueProvider<String> value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    // Create PubSub to Avro schema manipulator
    GenericAvroTransform genericAvroTransform = new GenericAvroTransform(options.getSchemaResgitryUri());
    ValueProvider<String> outputFilenamePrefix = options.getOutputFilenamePrefix();
    ValueProvider<String> outputFilenameSuffix = options.getOutputFilenamePrefix();
    ValueProvider<String> outputShardTemplate = options.getOutputShardTemplate();
    /*
     * Steps:
     *   1) Read messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed data into Avro files, one per window by default.
     */
    pipeline
        .apply(
            "Read PubSub Events",
            PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()))
        .apply("Map to Archive", ParDo.of(new PubsubMessageToArchiveDoFn()))
        .apply(
            options.getWindowDuration() + " Window",
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration()))))

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)",
            FileIO.<Integer, AvroPubsubMessageRecord>writeDynamic()
                .by(genericAvroTransform::schemaRegistryId)
                .withDestinationCoder(VarIntCoder.of())
                .via(
                    Contextful.<AvroPubsubMessageRecord, GenericRecord>fn(genericAvroTransform::convert),
                    Contextful.<Integer, FileIO.Sink<GenericRecord>>fn(globalId ->
                        AvroIO.sink(genericAvroTransform.schemaFrom(globalId))))
                .withTempDirectory(options.getAvroTempDirectory())
                .withNumShards(options.getNumShards())
                .withNaming(globalId ->
                        new CustomAvroFileNaming(globalId, outputFilenamePrefix, outputFilenameSuffix, outputShardTemplate))
                .to(options.getOutputDirectory()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * Converts an incoming {@link PubsubMessage} to the {@link AvroPubsubMessageRecord} class by
   * copying it's fields and the timestamp of the message.
   */
  static class PubsubMessageToArchiveDoFn extends DoFn<PubsubMessage, AvroPubsubMessageRecord> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      context.output(
          new AvroPubsubMessageRecord(
              message.getPayload(), message.getAttributeMap(), context.timestamp().getMillis()));
    }
  }

  /**
   * Based on {@link org.apache.beam.sdk.io.DefaultFilenamePolicy}, but adapted to this case.
   */
  static class CustomAvroFileNaming implements FileIO.Write.FileNaming {

    /*
     * pattern for both windowed and non-windowed file names.
     * Copy from {@link org.apache.beam.sdk.io.DefaultFilenamePolicy}
     */
    private static final Pattern SHARD_FORMAT_RE = Pattern.compile("(S+|N+|W|P)");

    private final Integer globalId;
    private final ValueProvider<String> filePrefix;
    private final ValueProvider<String> fileSuffix;
    private final ValueProvider<String> shardTemplate;

    public CustomAvroFileNaming(Integer globalId, ValueProvider<String> filePrefix, ValueProvider<String> fileSuffix, ValueProvider<String> shardTemplate) {

      this.globalId = globalId;
      this.filePrefix = filePrefix;
      this.fileSuffix = fileSuffix;
      this.shardTemplate = shardTemplate;
    }


    /**
     * Generates the filename. MUST use each argument and return different values for each
     * combination of the arguments.
     *
     * @param window
     * @param pane
     * @param numShards
     * @param shardIndex
     * @param compression
     */
    @Override
    public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
      return constructName(window, pane, numShards, shardIndex);
    }

    /**
     * Based on
     * {@link org.apache.beam.sdk.io.DefaultFilenamePolicy#constructName(ResourceId, String, String, int, int, String, String)}
     * method.
     *
     * @param window
     * @param pane
     * @param numShards
     * @param shardIndex
     * @return
     */
    private String constructName(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex) {
      String paneStr = paneInfoToString(pane);
      String windowStr = windowToString(window);

      StringBuffer sb = new StringBuffer();
      sb.append(filePrefix.get());

      Matcher m = SHARD_FORMAT_RE.matcher(shardTemplate.get());

      while (m.find()) {
        boolean isCurrentShardNum = (m.group(1).charAt(0) == 'S');
        boolean isNumberOfShards = (m.group(1).charAt(0) == 'N');
        boolean isPane = (m.group(1).charAt(0) == 'P') && paneStr != null;
        boolean isWindow = (m.group(1).charAt(0) == 'W') && windowStr != null;

        char[] zeros = new char[m.end() - m.start()];
        Arrays.fill(zeros, '0');
        DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
        if (isCurrentShardNum) {
          String formatted = df.format(shardIndex);
          m.appendReplacement(sb, formatted);
        } else if (isNumberOfShards) {
          String formatted = df.format(numShards);
          m.appendReplacement(sb, formatted);
        } else if (isPane) {
          m.appendReplacement(sb, paneStr);
        } else if (isWindow) {
          m.appendReplacement(sb, windowStr);
        }
      }
      m.appendTail(sb);
      sb.append("-").append(globalId);
      sb.append(fileSuffix.get());
      return sb.toString();
    }

    /**
     * Copy from private method in {@link org.apache.beam.sdk.io.DefaultFileNamePolicy}.
     *
     * @return
     */
    private String paneInfoToString(PaneInfo paneInfo) {
      String paneString = String.format("pane-%d", paneInfo.getIndex());
      if (paneInfo.getTiming() == PaneInfo.Timing.LATE) {
        paneString = String.format("%s-late", paneString);
      }
      if (paneInfo.isLast()) {
        paneString = String.format("%s-last", paneString);
      }
      return paneString;
    }

    /**
     * Copy from private method in {@link org.apache.beam.sdk.io.DefaultFileNamePolicy}.
     *
     * @return
     */
    private String windowToString(BoundedWindow window) {
      if (window instanceof IntervalWindow) {
        IntervalWindow iw = (IntervalWindow) window;
        return String.format("%s-%s", iw.start().toString(), iw.end().toString());
      } else {
        return window.maxTimestamp().toString();
      }
    }
  }
}
