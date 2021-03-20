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

package com.google.cloud.teleport.v2.cdc.sources;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.client.util.DateTime;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.FormatDatastreamJsonToJson;
import com.google.cloud.teleport.v2.transforms.FormatDatastreamRecordToJson;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.GcsUtilFactory;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class designed to inspect the directory tree produced by Cloud DataStream.
 *
 * This class can work in continuous streaming mode or in single-check batch mode.
 *
 * In streaming mode, every 100 seconds, this transform looks for new "objects", where each
 * object represents a table. 100 seconds was chosen to avoid performing this operation
 * too often, as it requires listing the full directory tree, which is very costly.
 *
 * An object represents a new directory at the base of the root path:
 * <ul>
 *   <li>`gs://BUCKET/`</li>
 *   <li>`gs://BUCKET/root/prefix/`</li>
 *   <li>`gs://BUCKET/root/prefix/HR_JOBS/` - This directory represents an "object"</li>
 *   <li>`gs://BUCKET/root/prefix/HR_SALARIES/` - This directory represents an "object"</li>
 * </ul>
 *
 * Every time a new "object" is discovered, a new key is created for it, and new directories
 * are monitored. Monitoring of directories is done periodically with the `matchPeriod`
 * parameter.
 *
 * <ul>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/` - This directory represents an "object"</li>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/2020/07/14/11/03/` - This directory is an example
 *  *    of the final output of this transform.</li>
 *    <li>`gs://BUCKET/root/prefix/HR_JOBS/2020/07/14/12/35/` - This directory is an example
 *  *    of the final output of this transform.</li>
 *    <li>`gs://BUCKET/root/prefix/HR_SALARIES/` - This directory represents an "object"</li>
 *  </ul>
 */
public class DataStreamIO extends PTransform<PBegin, PCollection<FailsafeElement<String, String>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamIO.class);
  private static final String AVRO_SUFFIX = "avro";
  private static final String JSON_SUFFIX = "json";

  private String streamName;
  private String inputFilePattern;
  private String fileType;
  private String gcsNotificationSubscription;
  private String rfcStartDateTime;
  private Integer fileReadConcurrency = 30;
  private Boolean lowercaseSourceColumns = false;
  PCollection<String> directories = null;

  public DataStreamIO() {}

  public DataStreamIO(
      String streamName, String inputFilePattern, String fileType,
      String gcsNotificationSubscription, String rfcStartDateTime) {
    this.streamName = streamName;
    this.inputFilePattern = inputFilePattern;
    this.gcsNotificationSubscription = gcsNotificationSubscription;
    this.rfcStartDateTime = rfcStartDateTime;
    this.fileType = fileType;

    if (!(fileType.equals(AVRO_SUFFIX) || fileType.equals(JSON_SUFFIX))){
      throw new IllegalArgumentException(
          "Input file format must be one of: avro or json - found " + fileType);
    }
  }

  public DataStreamIO withFileReadConcurrency(Integer fileReadConcurrency) {
    this.fileReadConcurrency = fileReadConcurrency;
    return this;
  }

  public DataStreamIO withLowercaseSourceColumns() {
    this.lowercaseSourceColumns = true;
    return this;
  }

  @Override
  public PCollection<FailsafeElement<String, String>> expand(PBegin input) {
    PCollection<ReadableFile> datastreamFiles =
        input.apply("Read Datastream Files", new DataStreamFileIO());
    PCollection<FailsafeElement<String, String>> datastreamJsonStrings =
        expandDataStreamJsonStrings(datastreamFiles);
    return datastreamJsonStrings;
  }

  public PCollection<FailsafeElement<String, String>>
      expandDataStreamJsonStrings(PCollection<ReadableFile> datastreamFiles) {
    PCollection<FailsafeElement<String, String>> datastreamJsonRecords;

    if (this.fileType.equals(JSON_SUFFIX)) {
        datastreamJsonRecords = datastreamFiles
          .apply("ReadFiles", TextIO.readFiles())
          .apply("ParseJsonRecords", ParDo.of(
              FormatDatastreamJsonToJson.create()
                  .withStreamName(this.streamName)
                  .withLowercaseSourceColumns(this.lowercaseSourceColumns)))
          .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
      } else {
        datastreamJsonRecords = datastreamFiles
          .apply("ParseAvroRecords",
              AvroIO.parseFilesGenericRecords(
                FormatDatastreamRecordToJson.create()
                    .withStreamName(this.streamName)
                    .withLowercaseSourceColumns(this.lowercaseSourceColumns))
                .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));
      }
    return datastreamJsonRecords.apply("Reshuffle", Reshuffle.viaRandomKey());
  }

  class DataStreamFileIO extends PTransform<PBegin, PCollection<ReadableFile>> {

    @Override
    public PCollection<ReadableFile> expand(PBegin input) {
      PCollection<ReadableFile> datastreamFiles;
      if (gcsNotificationSubscription != null) {
        datastreamFiles = expandGcsPubSubPipeline(input);
      } else if (inputFilePattern != null) {
        datastreamFiles = expandPollingPipeline(input);
      } else {
        throw new IllegalArgumentException(
            "DataStreamIO requires either a GCS stream  directory or Pub/Sub Subscription");
      }

      return datastreamFiles
          .apply("FileReadConcurrency",
              Reshuffle.<ReadableFile>viaRandomKey().withNumBuckets(fileReadConcurrency));
    }

    public PCollection<ReadableFile> expandGcsPubSubPipeline(PBegin input) {
        return input
          .apply(
            "ReadGcsPubSubSubscription",
            PubsubIO.readMessagesWithAttributes()
              .fromSubscription(gcsNotificationSubscription))
          .apply("ExtractGcsFilePath", ParDo.of(new ExtractGcsFile()))
          .apply("ReadFiles", FileIO.readMatches());
    }

    public PCollection<ReadableFile> expandPollingPipeline(PBegin input) {
      directories = input
          .apply("StartPipeline", Create.of(inputFilePattern))
          .apply("FindTableDirectory",
            Watch
              .growthOf(new DirectoryMatchPollFn(1, 1, null, "/"))
              .withPollInterval(Duration.standardSeconds(120)))
          .apply(Values.create())
          .apply("FindTablePerMinuteDirectory",
            Watch
              .growthOf(new DirectoryMatchPollFn(5, 5, rfcStartDateTime, null))
              .withPollInterval(Duration.standardSeconds(5)))
          .apply(Values.create());

      return directories
          .apply("GetDirectoryGlobs",
                  MapElements.into(TypeDescriptors.strings()).via(path -> path + "**"))
              .apply(
                  "MatchDatastreamAvroFiles",
                  FileIO.matchAll()
                      .continuously(
                          Duration.standardSeconds(5),
                          Growth.afterTimeSinceNewOutput(Duration.standardMinutes(10))))
              .apply("ReadFiles", FileIO.readMatches());
    }
  }

  static class ExtractGcsFile extends DoFn<PubsubMessage, Metadata> {
    @ProcessElement
    public void process(ProcessContext context) {
      PubsubMessage message = context.element();

      String eventType = message.getAttribute("eventType");
      String bucketId = message.getAttribute("bucketId");
      String objectId = message.getAttribute("objectId");

      if (eventType.equals("OBJECT_FINALIZE") && !objectId.endsWith("/")) {
        String fileName = "gs://" + bucketId + "/" + objectId;
        try {
          Metadata fileMetadata = FileSystems.matchSingleFileSpec(fileName);
          context.output(fileMetadata);
        } catch (IOException e) {
          LOG.error("GCS Failure retrieving {}: {}", fileName, e);
        }
      }
    }
  }

  static class DirectoryMatchPollFn extends PollFn<String, String> {
    private transient GcsUtil util;
    private final Integer minDepth;
    private final Integer maxDepth;
    private final DateTime startDateTime;
    private final String delimiter;

    DirectoryMatchPollFn(Integer minDepth, Integer maxDepth,
        String rfcStartDateTime, String delimiter) {
      this.maxDepth = maxDepth;
      this.minDepth = minDepth;
      // The delimiter parameter works for 1-depth elements.
      // See https://cloud.google.com/storage/docs/json_api/v1/objects/list for details.
      this.delimiter = delimiter;
      if (rfcStartDateTime != null) {
        this.startDateTime = DateTime.parseRfc3339(rfcStartDateTime);
      } else {
        this.startDateTime = DateTime.parseRfc3339("1970-01-01T00:00:00.00Z");
      }
    }

    private Integer getObjectDepth(String objectName) {
      int depthCount = 1;
      for (char i : objectName.toCharArray()) {
        if (i == '/') {
          depthCount += 1;
        }
      }
      return depthCount;
    }

    private GcsUtil getUtil() {
      if (util == null) {
        util = new GcsUtilFactory().create(PipelineOptionsFactory.create());
      }
      return util;
    }

    private boolean shouldFilterObject(StorageObject object) {
      DateTime updatedDateTime = object.getUpdated();
      if (updatedDateTime.getValue() < this.startDateTime.getValue()) {
        return true;
      }
      return false;
    }

    private List<TimestampedValue<String>> getMatchingObjects(GcsPath path) throws IOException {
      List<TimestampedValue<String>> result = new ArrayList<>();
      Integer baseDepth = getObjectDepth(path.getObject());
      GcsUtil util = getUtil();
      String pageToken = null;
      do {
        Objects objects = util.listObjects(
            path.getBucket(), path.getObject(), pageToken, delimiter);
        pageToken = objects.getNextPageToken();
        List<StorageObject> items = firstNonNull(
            objects.getItems(), Lists.newArrayList());
        if (objects.getPrefixes() != null) {
          for (String prefix : objects.getPrefixes()) {
            result.add(TimestampedValue.of(
                "gs://" + path.getBucket() + "/" + prefix,
                Instant.EPOCH));
          }
        }
        for (StorageObject object : items) {
          String fullName = "gs://" + object.getBucket() + "/" + object.getName();
          if (!object.getName().endsWith("/")) {
            // This object is not a directory, and should be ignored.
            continue;
          }
          if (object.getName().equals(path.getObject())) {
            // Output only direct children and not the directory itself.
            continue;
          }
          if (shouldFilterObject(object)) {
            // Skip file due to iinitial timestamp
            continue;
          }
          Integer newDepth = getObjectDepth(object.getName());
          if (baseDepth + minDepth <= newDepth && newDepth <= baseDepth + maxDepth) {
            Instant fileUpdatedInstant = Instant.ofEpochMilli(object.getUpdated().getValue());
            result.add(TimestampedValue.of(fullName, fileUpdatedInstant));
          }
        }
      } while (pageToken != null);
      return result;
    }

    @Override
    public Watch.Growth.PollResult<String> apply(String element, Context c)
        throws Exception {
      Instant now = Instant.now();
      GcsPath path = GcsPath.fromUri(element);
      return Watch.Growth.PollResult.incomplete(getMatchingObjects(path));
    }
  }
}
