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
package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.common.base.Charsets;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentRequest.Builder;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.ReadableFileCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DLPTextToBigQueryStreaming} is a streaming pipeline that reads CSV files from a
 * storage location (e.g. Google Cloud Storage), uses Cloud DLP API to inspect, classify, and mask
 * sensitive information (e.g. PII Data like passport or SIN number) and at the end stores
 * obfuscated data in BigQuery (Dynamic Table Creation) to be used for various purposes. e.g. data
 * analytics, ML model. Cloud DLP inspection and masking can be configured by the user and can make
 * use of over 90 built in detectors and masking techniques like tokenization, secure hashing, date
 * shifting, partial masking, and more.
 *
 * <p><b>Pipeline Requirements</b>
 *
 * <ul>
 *   <li>DLP Templates exist (e.g. deidentifyTemplate, InspectTemplate)
 *   <li>The BigQuery Dataset exists
 * </ul>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * # Set the pipeline vars
 * PROJECT_ID=PROJECT ID HERE
 * BUCKET_NAME=BUCKET NAME HERE
 * PIPELINE_FOLDER=gs://${BUCKET_NAME}/dataflow/pipelines/dlp-text-to-bigquery
 *
 * # Set the runner
 * RUNNER=DataflowRunner
 *
 * # Build the template
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.DLPTextToBigQueryStreaming \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=${PIPELINE_FOLDER}/staging \
 * --tempLocation=${PIPELINE_FOLDER}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --runner=${RUNNER}"
 *
 * # Execute the template
 * JOB_NAME=dlp-text-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`
 *
 * gcloud dataflow jobs run ${JOB_NAME} \
 * --gcs-location=${PIPELINE_FOLDER}/template \
 * --zone=us-east1-d \
 * --parameters \
 * "inputFilePattern=gs://<bucketName>/<fileName>.csv, batchSize=15,datasetName=<BQDatasetId>,
 *  dlpProjectId=<projectId>,
 *  deidentifyTemplateName=projects/{projectId}/deidentifyTemplates/{deIdTemplateId}
 * </pre>
 */
public class DLPTextToBigQueryStreaming {

  public static final Logger LOG = LoggerFactory.getLogger(DLPTextToBigQueryStreaming.class);
  /** Default interval for polling files in GCS. */
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(30);
  /** Expected only CSV file in GCS bucket. */
  private static final String ALLOWED_FILE_EXTENSION = String.valueOf("csv");
  /** Regular expression that matches valid BQ table IDs. */
  private static final Pattern TABLE_REGEXP = Pattern.compile("[-\\w$@]{1,1024}");
  /** Default batch size if value not provided in execution. */
  private static final Integer DEFAULT_BATCH_SIZE = 100;
  /** Regular expression that matches valid BQ column name . */
  private static final Pattern COLUMN_NAME_REGEXP = Pattern.compile("^[A-Za-z_]+[A-Za-z_0-9]*$");
  /** Default window interval to create side inputs for header records. */
  private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(30);

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * DLPTextToBigQueryStreaming#run(TokenizePipelineOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    TokenizePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TokenizePipelineOptions.class);
    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(TokenizePipelineOptions options) {
    // Create the pipeline
    Pipeline p = Pipeline.create(options);
    /*
     * Steps:
     *   1) Read from the text source continuously based on default interval e.g. 30 seconds
     *       - Setup a window for 30 secs to capture the list of files emited.
     *       - Group by file name as key and ReadableFile as a value.
     *   2) Create a side input for the window containing list of headers par file.
     *   3) Output each readable file for content processing.
     *   4) Split file contents based on batch size for parallel processing.
     *   5) Process each split as a DLP table content request to invoke API.
     *   6) Convert DLP Table Rows to BQ Table Row.
     *   7) Create dynamic table and insert successfully converted records into BQ.
     */

    PCollection<KV<String, Iterable<ReadableFile>>> csvFiles =
        p
            /*
             * 1) Read from the text source continuously based on default interval e.g. 300 seconds
             *     - Setup a window for 30 secs to capture the list of files emited.
             *     - Group by file name as key and ReadableFile as a value.
             */
            .apply(
                "Poll Input Files",
                FileIO.match()
                    .filepattern(options.getInputFilePattern())
                    .continuously(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
            .apply("Find Pattern Match", FileIO.readMatches().withCompression(Compression.AUTO))
            .apply("Add File Name as Key", WithKeys.of(file -> getFileName(file)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), ReadableFileCoder.of()))
            .apply(
                "Fixed Window(30 Sec)",
                Window.<KV<String, ReadableFile>>into(FixedWindows.of(WINDOW_INTERVAL))
                    .triggering(
                        Repeatedly.forever(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(Duration.ZERO)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(GroupByKey.create());

    /*
     * Side input for the window to capture list of headers for each file emited so that it can be
     * used in the next transform.
     */
    final PCollectionView<List<KV<String, List<String>>>> headerMap =
        csvFiles

            // 2) Create a side input for the window containing list of headers par file.
            .apply(
                "Create Header Map",
                ParDo.of(
                    new DoFn<KV<String, Iterable<ReadableFile>>, KV<String, List<String>>>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String fileKey = c.element().getKey();
                        c.element()
                            .getValue()
                            .forEach(
                                file -> {
                                  try (BufferedReader br = getReader(file)) {
                                    c.output(KV.of(fileKey, getFileHeaders(br)));

                                  } catch (IOException e) {
                                    LOG.error("Failed to Read File {}", e.getMessage());
                                    throw new RuntimeException(e);
                                  }
                                });
                      }
                    }))
            .apply("View As List", View.asList());

    PCollection<KV<String, TableRow>> bqDataMap =
        csvFiles

            // 3) Output each readable file for content processing.
            .apply(
                "File Handler",
                ParDo.of(
                    new DoFn<KV<String, Iterable<ReadableFile>>, KV<String, ReadableFile>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String fileKey = c.element().getKey();
                        c.element()
                            .getValue()
                            .forEach(
                                file -> {
                                  c.output(KV.of(fileKey, file));
                                });
                      }
                    }))

            // 4) Split file contents based on batch size for parallel processing.
            .apply(
                "Process File Contents",
                ParDo.of(
                        new CSVReader(
                            NestedValueProvider.of(
                                options.getBatchSize(),
                                batchSize -> {
                                  if (batchSize != null) {
                                    return batchSize;
                                  } else {
                                    return DEFAULT_BATCH_SIZE;
                                  }
                                }),
                            headerMap))
                    .withSideInputs(headerMap))

            // 5) Create a DLP Table content request and invoke DLP API for each processsing
            .apply(
                "DLP-Tokenization",
                ParDo.of(
                    new DLPTokenizationDoFn(
                        options.getDlpProjectId(),
                        options.getDeidentifyTemplateName(),
                        options.getInspectTemplateName())))

            // 6) Convert DLP Table Rows to BQ Table Row
            .apply("Process Tokenized Data", ParDo.of(new TableRowProcessorDoFn()));

    // 7) Create dynamic table and insert successfully converted records into BQ.
    bqDataMap.apply(
        "Write To BQ",
        BigQueryIO.<KV<String, TableRow>>write()
            .to(new BQDestination(options.getDatasetName(), options.getDlpProjectId()))
            .withFormatFunction(
                element -> {
                  return element.getValue();
                })
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withoutValidation()
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));

    return p.run();
  }

  /**
   * The {@link TokenizePipelineOptions} interface provides the custom execution options passed by
   * the executor at the command-line.
   */
  public interface TokenizePipelineOptions extends DataflowPipelineOptions {

    @Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description(
        "DLP Deidentify Template to be used for API request "
            + "(e.g.projects/{project_id}/deidentifyTemplates/{deIdTemplateId}")
    @Required
    ValueProvider<String> getDeidentifyTemplateName();

    void setDeidentifyTemplateName(ValueProvider<String> value);

    @Description(
        "DLP Inspect Template to be used for API request "
            + "(e.g.projects/{project_id}/inspectTemplates/{inspectTemplateId}")
    ValueProvider<String> getInspectTemplateName();

    void setInspectTemplateName(ValueProvider<String> value);

    @Description(
        "DLP API has a limit for payload size of 524KB /api call. "
            + "That's why dataflow process will need to chunk it. User will have to decide "
            + "on how they would like to batch the request depending on number of rows "
            + "and how big each row is.")
    @Required
    ValueProvider<Integer> getBatchSize();

    void setBatchSize(ValueProvider<Integer> value);

    @Description("Big Query data set must exist before the pipeline runs (e.g. pii-dataset")
    ValueProvider<String> getDatasetName();

    void setDatasetName(ValueProvider<String> value);

    @Description("Project id to be used for DLP Tokenization")
    ValueProvider<String> getDlpProjectId();

    void setDlpProjectId(ValueProvider<String> value);
  }

  /**
   * The {@link CSVReader} class uses experimental Split DoFn to split each csv file contents in
   * chunks and process it in non-monolithic fashion. For example: if a CSV file has 100 rows and
   * batch size is set to 15, then initial restrictions for the SDF will be 1 to 7 and split
   * restriction will be {{1-2},{2-3}..{7-8}} for parallel executions.
   */
  static class CSVReader extends DoFn<KV<String, ReadableFile>, KV<String, Table>> {

    private ValueProvider<Integer> batchSize;
    private PCollectionView<List<KV<String, List<String>>>> headerMap;
    /** This counter is used to track number of lines processed against batch size. */
    private Integer lineCount;

    List<String> csvHeaders;

    public CSVReader(
        ValueProvider<Integer> batchSize,
        PCollectionView<List<KV<String, List<String>>>> headerMap) {
      lineCount = 1;
      this.batchSize = batchSize;
      this.headerMap = headerMap;
      this.csvHeaders = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker)
        throws IOException {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        String fileKey = c.element().getKey();
        try (BufferedReader br = getReader(c.element().getValue())) {

          csvHeaders = getHeaders(c.sideInput(headerMap), fileKey);
          if (csvHeaders != null) {
            List<FieldId> dlpTableHeaders =
                csvHeaders.stream()
                    .map(header -> FieldId.newBuilder().setName(header).build())
                    .collect(Collectors.toList());
            List<Table.Row> rows = new ArrayList<>();
            Table dlpTable = null;
            /** finding out EOL for this restriction so that we know the SOL */
            int endOfLine = (int) (i * batchSize.get().intValue());
            int startOfLine = (endOfLine - batchSize.get().intValue());
            /** skipping all the rows that's not part of this restriction */
            br.readLine();
            Iterator<CSVRecord> csvRows =
                CSVFormat.DEFAULT.withSkipHeaderRecord().parse(br).iterator();
            for (int line = 0; line < startOfLine; line++) {
              if (csvRows.hasNext()) {
                csvRows.next();
              }
            }
            /** looping through buffered reader and creating DLP Table Rows equals to batch */
            while (csvRows.hasNext() && lineCount <= batchSize.get()) {

              CSVRecord csvRow = csvRows.next();
              rows.add(convertCsvRowToTableRow(csvRow));
              lineCount += 1;
            }
            /** creating DLP table and output for next transformation */
            dlpTable = Table.newBuilder().addAllHeaders(dlpTableHeaders).addAllRows(rows).build();
            c.output(KV.of(fileKey, dlpTable));

            LOG.debug(
                "Current Restriction From: {}, Current Restriction To: {},"
                    + " StartofLine: {}, End Of Line {}, BatchData {}",
                tracker.currentRestriction().getFrom(),
                tracker.currentRestriction().getTo(),
                startOfLine,
                endOfLine,
                dlpTable.getRowsCount());

          } else {

            throw new RuntimeException("Header Values Can't be found For file Key " + fileKey);
          }
        }
      }
    }

    /**
     * SDF needs to define a @GetInitialRestriction method that can create a restriction describing
     * the complete work for a given element. For our case this would be the total number of rows
     * for each CSV file. We will calculate the number of split required based on total number of
     * rows and batch size provided.
     *
     * @throws IOException
     */
    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element KV<String, ReadableFile> csvFile)
        throws IOException {

      int rowCount = 0;
      int totalSplit = 0;
      try (BufferedReader br = getReader(csvFile.getValue())) {
        /** assume first row is header */
        int checkRowCount = (int) br.lines().count() - 1;
        rowCount = (checkRowCount < 1) ? 1 : checkRowCount;
        totalSplit = rowCount / batchSize.get().intValue();
        int remaining = rowCount % batchSize.get().intValue();
        /**
         * Adjusting the total number of split based on remaining rows. For example: batch size of
         * 15 for 100 rows will have total 7 splits. As it's a range last split will have offset
         * range {7,8}
         */
        if (remaining > 0) {
          totalSplit = totalSplit + 2;

        } else {
          totalSplit = totalSplit + 1;
        }
      }

      LOG.debug("Initial Restriction range from 1 to: {}", totalSplit);
      return new OffsetRange(1, totalSplit);
    }

    /**
     * SDF needs to define a @SplitRestriction method that can split the intital restricton to a
     * number of smaller restrictions. For example: a intital rewstriction of (x, N) as input and
     * produces pairs (x, 0), (x, 1), …, (x, N-1) as output.
     */
    @SplitRestriction
    public void splitRestriction(
        @Element KV<String, ReadableFile> csvFile,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> out) {
      /** split the initial restriction by 1 */
      for (final OffsetRange p : range.split(1, 1)) {
        out.output(p);
      }
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange range) {
      return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));
    }

    private Table.Row convertCsvRowToTableRow(CSVRecord csvRow) {
      /** convert from CSV row to DLP Table Row */
      Iterator<String> valueIterator = csvRow.iterator();
      Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
      while (valueIterator.hasNext()) {
        String value = valueIterator.next();
        if (value != null) {
          tableRowBuilder.addValues(Value.newBuilder().setStringValue(value.toString()).build());
        } else {
          tableRowBuilder.addValues(Value.newBuilder().setStringValue("").build());
        }
      }

      return tableRowBuilder.build();
    }

    private List<String> getHeaders(List<KV<String, List<String>>> headerMap, String fileKey) {
      return headerMap.stream()
          .filter(map -> map.getKey().equalsIgnoreCase(fileKey))
          .findFirst()
          .map(e -> e.getValue())
          .orElse(null);
    }
  }

  /**
   * The {@link DLPTokenizationDoFn} class executes tokenization request by calling DLP api. It uses
   * DLP table as a content item as CSV file contains fully structured data. DLP templates (e.g.
   * de-identify, inspect) need to exist before this pipeline runs. As response from the API is
   * received, this DoFn ouptputs KV of new table with table id as key.
   */
  static class DLPTokenizationDoFn extends DoFn<KV<String, Table>, KV<String, Table>> {
    private ValueProvider<String> dlpProjectId;
    private DlpServiceClient dlpServiceClient;
    private ValueProvider<String> deIdentifyTemplateName;
    private ValueProvider<String> inspectTemplateName;
    private boolean inspectTemplateExist;
    private Builder requestBuilder;
    private final Distribution numberOfRowsTokenized =
        Metrics.distribution(DLPTokenizationDoFn.class, "numberOfRowsTokenizedDistro");
    private final Distribution numberOfBytesTokenized =
        Metrics.distribution(DLPTokenizationDoFn.class, "numberOfBytesTokenizedDistro");

    public DLPTokenizationDoFn(
        ValueProvider<String> dlpProjectId,
        ValueProvider<String> deIdentifyTemplateName,
        ValueProvider<String> inspectTemplateName) {
      this.dlpProjectId = dlpProjectId;
      this.dlpServiceClient = null;
      this.deIdentifyTemplateName = deIdentifyTemplateName;
      this.inspectTemplateName = inspectTemplateName;
      this.inspectTemplateExist = false;
    }

    @Setup
    public void setup() {
      if (this.inspectTemplateName.isAccessible()) {
        if (this.inspectTemplateName.get() != null) {
          this.inspectTemplateExist = true;
        }
      }
      if (this.deIdentifyTemplateName.isAccessible()) {
        if (this.deIdentifyTemplateName.get() != null) {
          this.requestBuilder =
              DeidentifyContentRequest.newBuilder()
                  .setParent(ProjectName.of(this.dlpProjectId.get()).toString())
                  .setDeidentifyTemplateName(this.deIdentifyTemplateName.get());
          if (this.inspectTemplateExist) {
            this.requestBuilder.setInspectTemplateName(this.inspectTemplateName.get());
          }
        }
      }
    }

    @StartBundle
    public void startBundle() throws SQLException {

      try {
        this.dlpServiceClient = DlpServiceClient.create();

      } catch (IOException e) {
        LOG.error("Failed to create DLP Service Client", e.getMessage());
        throw new RuntimeException(e);
      }
    }

    @FinishBundle
    public void finishBundle() throws Exception {
      if (this.dlpServiceClient != null) {
        this.dlpServiceClient.close();
      }
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String key = c.element().getKey();
      Table nonEncryptedData = c.element().getValue();
      ContentItem tableItem = ContentItem.newBuilder().setTable(nonEncryptedData).build();
      this.requestBuilder.setItem(tableItem);
      DeidentifyContentResponse response =
          dlpServiceClient.deidentifyContent(this.requestBuilder.build());
      Table tokenizedData = response.getItem().getTable();
      numberOfRowsTokenized.update(tokenizedData.getRowsList().size());
      numberOfBytesTokenized.update(tokenizedData.toByteArray().length);
      c.output(KV.of(key, tokenizedData));
    }
  }

  /**
   * The {@link TableRowProcessorDoFn} class process tokenized DLP tables and convert them to
   * BigQuery Table Row.
   */
  public static class TableRowProcessorDoFn extends DoFn<KV<String, Table>, KV<String, TableRow>> {

    @ProcessElement
    public void processElement(ProcessContext c) {

      Table tokenizedData = c.element().getValue();
      List<String> headers =
          tokenizedData.getHeadersList().stream()
              .map(fid -> fid.getName())
              .collect(Collectors.toList());
      List<Table.Row> outputRows = tokenizedData.getRowsList();
      if (outputRows.size() > 0) {
        for (Table.Row outputRow : outputRows) {
          if (outputRow.getValuesCount() != headers.size()) {
            throw new IllegalArgumentException(
                "CSV file's header count must exactly match with data element count");
          }
          c.output(
              KV.of(
                  c.element().getKey(),
                  createBqRow(outputRow, headers.toArray(new String[headers.size()]))));
        }
      }
    }

    private static TableRow createBqRow(Table.Row tokenizedValue, String[] headers) {
      TableRow bqRow = new TableRow();
      AtomicInteger headerIndex = new AtomicInteger(0);
      List<TableCell> cells = new ArrayList<>();
      tokenizedValue
          .getValuesList()
          .forEach(
              value -> {
                String checkedHeaderName =
                    checkHeaderName(headers[headerIndex.getAndIncrement()].toString());
                bqRow.set(checkedHeaderName, value.getStringValue());
                cells.add(new TableCell().set(checkedHeaderName, value.getStringValue()));
              });
      bqRow.setF(cells);
      return bqRow;
    }
  }

  /**
   * The {@link BQDestination} class creates BigQuery table destination and table schema based on
   * the CSV file processed in earlier transformations. Table id is same as filename Table schema is
   * same as file header columns.
   */
  public static class BQDestination
      extends DynamicDestinations<KV<String, TableRow>, KV<String, TableRow>> {

    private ValueProvider<String> datasetName;
    private ValueProvider<String> projectId;

    public BQDestination(ValueProvider<String> datasetName, ValueProvider<String> projectId) {
      this.datasetName = datasetName;
      this.projectId = projectId;
    }

    @Override
    public KV<String, TableRow> getDestination(ValueInSingleWindow<KV<String, TableRow>> element) {
      String key = element.getValue().getKey();
      String tableName = String.format("%s:%s.%s", projectId.get(), datasetName.get(), key);
      LOG.debug("Table Name {}", tableName);
      return KV.of(tableName, element.getValue().getValue());
    }

    @Override
    public TableDestination getTable(KV<String, TableRow> destination) {
      TableDestination dest =
          new TableDestination(destination.getKey(), "pii-tokenized output data from dataflow");
      LOG.debug("Table Destination {}", dest.getTableSpec());
      return dest;
    }

    @Override
    public TableSchema getSchema(KV<String, TableRow> destination) {

      TableRow bqRow = destination.getValue();
      TableSchema schema = new TableSchema();
      List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
      List<TableCell> cells = bqRow.getF();
      for (int i = 0; i < cells.size(); i++) {
        Map<String, Object> object = cells.get(i);
        String header = object.keySet().iterator().next();
        /** currently all BQ data types are set to String */
        fields.add(new TableFieldSchema().setName(checkHeaderName(header)).setType("STRING"));
      }

      schema.setFields(fields);
      return schema;
    }
  }

  private static String getFileName(ReadableFile file) {
    String csvFileName = file.getMetadata().resourceId().getFilename().toString();
    /** taking out .csv extension from file name e.g fileName.csv->fileName */
    String[] fileKey = csvFileName.split("\\.", 2);

    if (!fileKey[1].equals(ALLOWED_FILE_EXTENSION) || !TABLE_REGEXP.matcher(fileKey[0]).matches()) {
      throw new RuntimeException(
          "[Filename must contain a CSV extension "
              + " BQ table name must contain only letters, numbers, or underscores ["
              + fileKey[1]
              + "], ["
              + fileKey[0]
              + "]");
    }
    /** returning file name without extension */
    return fileKey[0];
  }

  private static BufferedReader getReader(ReadableFile csvFile) {
    BufferedReader br = null;
    ReadableByteChannel channel = null;
    /** read the file and create buffered reader */
    try {
      channel = csvFile.openSeekable();

    } catch (IOException e) {
      LOG.error("Failed to Read File {}", e.getMessage());
      throw new RuntimeException(e);
    }

    if (channel != null) {

      br = new BufferedReader(Channels.newReader(channel, Charsets.UTF_8.name()));
    }

    return br;
  }

  private static List<String> getFileHeaders(BufferedReader reader) {
    List<String> headers = new ArrayList<>();
    try {
      CSVRecord csvHeader = CSVFormat.DEFAULT.parse(reader).getRecords().get(0);
      csvHeader.forEach(
          headerValue -> {
            headers.add(headerValue);
          });
    } catch (IOException e) {
      LOG.error("Failed to get csv header values}", e.getMessage());
      throw new RuntimeException(e);
    }
    return headers;
  }

  private static String checkHeaderName(String name) {
    /** some checks to make sure BQ column names don't fail e.g. special characters */
    String checkedHeader = name.replaceAll("\\s", "_");
    checkedHeader = checkedHeader.replaceAll("'", "");
    checkedHeader = checkedHeader.replaceAll("/", "");
    if (!COLUMN_NAME_REGEXP.matcher(checkedHeader).matches()) {
      throw new IllegalArgumentException("Column name can't be matched to a valid format " + name);
    }
    return checkedHeader;
  }
}
