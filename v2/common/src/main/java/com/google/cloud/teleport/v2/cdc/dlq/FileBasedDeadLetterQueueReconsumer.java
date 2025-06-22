/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.cdc.dlq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DeadLetterQueueReconsumer that works by periodically fetching files from a DLQ directory.
 *
 * <p>This transforms assumes that the DLQ files are stored in JSON Lines format.
 */
public class FileBasedDeadLetterQueueReconsumer extends PTransform<PBegin, PCollection<String>> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileBasedDeadLetterQueueReconsumer.class);

  public static final Duration DEFAULT_RECHECK_PERIOD = Duration.standardMinutes(5);

  private final String dlqDirectory;
  private final Duration recheckPeriod;

  public static FileBasedDeadLetterQueueReconsumer create(
      String dlqDirectory, Integer recheckPeriodMinutes) {
    return new FileBasedDeadLetterQueueReconsumer(
        dlqDirectory, Duration.standardMinutes(recheckPeriodMinutes));
  }

  public static FileBasedDeadLetterQueueReconsumer create(String dlqDirectory) {
    return new FileBasedDeadLetterQueueReconsumer(dlqDirectory, DEFAULT_RECHECK_PERIOD);
  }

  private FileBasedDeadLetterQueueReconsumer(String dlqDirectory, Duration recheckPeriod) {
    this.dlqDirectory = dlqDirectory;
    this.recheckPeriod = recheckPeriod;
  }

  public PCollection<String> expand(PBegin in) {
    // We want to match all the files in this directory (but not the directories).
    // TODO: Paths resolve converts "gs://bucket/.." to "gs:/bucket/.."
    // String filePattern = Paths.get(dlqDirectory).resolve("*").toString();

    String filePattern = dlqDirectory + "**";
    return in.getPipeline()
        .apply("TriggerConsumeDLQ", GenerateSequence.from(0).withRate(1, this.recheckPeriod))
        .apply("AsFilePattern", MapElements.into(TypeDescriptors.strings()).via(seq -> filePattern))
        .apply("MatchFiles", FileIO.matchAll())
        .apply("MoveAndConsumeMatches", moveAndConsumeMatches());
  }

  /**
   * Build a {@link PTransform} that consumes matched DLQ files.
   *
   * <p>This transform is not idempotent. It should only be used when the source is guaranteed to
   * provide each file only once.
   */
  static PTransform<PCollection<Metadata>, PCollection<String>> moveAndConsumeMatches() {
    return new PTransform<PCollection<Metadata>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Metadata> input) {
        TupleTag<String> fileContents = new TupleTag<String>();
        TupleTag<ResourceId> fileMetadata = new TupleTag<ResourceId>();

        PCollection<ResourceId> movedFiles =
            input
                .apply("MoveFiles", ParDo.of(new MoveFiles()))
                .setCoder(ResourceIdCoder.of())
                .apply("ReshuffleMovedFiles", Reshuffle.viaRandomKey());

        PCollectionTuple results =
            movedFiles.apply(
                "ConsumeFiles",
                ParDo.of(new MoveAndConsumeFn(fileContents, fileMetadata))
                    .withOutputTags(fileContents, TupleTagList.of(fileMetadata)));

        results
            .get(fileMetadata)
            .setCoder(ResourceIdCoder.of())
            .apply("ReshuffleFiles", Reshuffle.viaRandomKey())
            .apply("RemoveFiles", ParDo.of(new RemoveFiles()));

        return results
            .get(fileContents)
            .setCoder(StringUtf8Coder.of())
            .apply("ReshuffleContents", Reshuffle.viaRandomKey());
      }
    };
  }

  private static class MoveFiles extends DoFn<Metadata, ResourceId> {
    @ProcessElement
    public void process(@Element Metadata dlqFile, OutputReceiver<ResourceId> output)
        throws IOException {
      ResourceId tmpFile =
          dlqFile
              .resourceId()
              .getCurrentDirectory()
              .resolve(
                  "tmp-" + dlqFile.resourceId().getFilename(), StandardResolveOptions.RESOLVE_FILE);
      LOG.info("Moving DLQ File {} to {}", dlqFile.resourceId().toString(), tmpFile.toString());
      FileSystems.rename(
          Collections.singletonList(dlqFile.resourceId()), Collections.singletonList(tmpFile));
      output.output(tmpFile);
    }
  }

  private static class RemoveFiles extends DoFn<ResourceId, Void> {
    private final List<ResourceId> filesToRemove = new ArrayList<>();
    private final Counter failedDeletions =
        Metrics.counter(MoveAndConsumeFn.class, "failedDeletions");

    @ProcessElement
    public void process(@Element ResourceId dlqFile, MultiOutputReceiver outputs)
        throws IOException {
      this.filesToRemove.add(dlqFile);
    }

    @FinishBundle
    public void cleanupFiles() {
      for (ResourceId file : filesToRemove) {
        try {
          FileSystems.delete(Collections.singleton(file));
          LOG.info("Deleted file {}.", file);
        } catch (IOException e) {
          LOG.error("Unable to delete file {}. Exception: {}", file, e);
          failedDeletions.inc();
        }
      }
      filesToRemove.clear();
    }
  }

  private static class MoveAndConsumeFn extends DoFn<ResourceId, String> {

    private final Counter reconsumedElements =
        Metrics.counter(MoveAndConsumeFn.class, "elementsReconsumedFromDeadLetterQueue");

    private final TupleTag<ResourceId> filesTag;
    private final TupleTag<String> contentTag;

    MoveAndConsumeFn(TupleTag<String> contentTag, TupleTag<ResourceId> filesTag) {
      this.filesTag = filesTag;
      this.contentTag = contentTag;
    }

    /* Gets the total number of times the record has circulated through the retry
     * DeadLetterHeadQueue. If not found, it assumes the record has been through the
     * DeadLetterHeadQueue once. If found, increments the count by 1;
     */
    long getRetryCountForRecord(ObjectNode resultNode) {
      JsonNode errorCount = resultNode.get("_metadata_retry_count");
      if (errorCount == null) {
        return 1;
      }
      return errorCount.asLong() + 1;
    }

    @ProcessElement
    public void process(@Element ResourceId dlqFile, MultiOutputReceiver outputs)
        throws IOException {
      LOG.info("Found DLQ File: {}", dlqFile.toString());
      if (dlqFile.toString().contains("/tmp/.temp")) {
        return;
      }

      BufferedReader jsonReader;
      try {
        jsonReader = readFile(dlqFile);
      } catch (FileNotFoundException e) {
        // If the file does exist, it will be retried on the next trigger.
        LOG.warn("DLQ File Not Found: {}", dlqFile.toString());
        return;
      }

      // Assuming that files are JSONLines formatted.
      ObjectMapper mapper = new ObjectMapper();
      jsonReader
          .lines()
          .forEach(
              line -> {
                ObjectNode resultNode;
                // Each line is expecting this format: {"message": ROW, "error_message": ERROR}
                try {
                  JsonNode jsonDLQElement = mapper.readTree(line);
                  if (!jsonDLQElement.get("message").isObject()) {
                    throw new IOException("Unable to parse JSON record " + line);
                  }
                  resultNode = (ObjectNode) jsonDLQElement.get("message");
                  resultNode.put("_metadata_error", jsonDLQElement.get("error_message"));
                  // Populate the retried count.
                  long retryErrorCount = getRetryCountForRecord(resultNode);
                  resultNode.put("_metadata_retry_count", retryErrorCount);
                  outputs.get(contentTag).output(resultNode.toString());
                  reconsumedElements.inc();
                } catch (IOException e) {
                  LOG.error("Issue parsing JSON record {}. Unable to continue.", line, e);
                  throw new RuntimeException(e);
                }
              });
      outputs.get(filesTag).output(dlqFile);
    }
  }

  public static BufferedReader readFile(ResourceId resourceId)
      throws IOException, FileNotFoundException {
    InputStream jsonStream = Channels.newInputStream(FileSystems.open(resourceId));
    return new BufferedReader(new InputStreamReader(jsonStream, StandardCharsets.UTF_8));
  }
}
