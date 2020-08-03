/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.cdc.dlq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Watch.Growth;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DeadLetterQueueReconsumer that works by periodically fetching files from a DLQ directory.
 *
 * This transforms assumes that the DLQ files are stored in JSON Lines format.
 */
public class FileBasedDeadLetterQueueReconsumer extends PTransform<PBegin, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(
      FileBasedDeadLetterQueueReconsumer.class);

  public static final Duration DEFAULT_RECHECK_PERIOD = Duration.standardMinutes(2);
  public static final String TEMPORARY_HOLD_SUBDIRECTORY = "tmp";

  private final String dlqDirectory;
  private final Duration recheckPeriod;

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
    String filePattern = dlqDirectory + "*";
    return in.getPipeline()
        .apply(FileIO.match()
            .filepattern(filePattern)
            .continuously(recheckPeriod, Growth.never()))
        .apply(moveAndConsumeMatches());

  }

  /** Build a {@link PTransform} that consumes matched DLQ files. */
  static PTransform<PCollection<Metadata>, PCollection<String>> moveAndConsumeMatches() {
    return new PTransform<PCollection<Metadata>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Metadata> input) {
        // TODO(pabloem, dhercher): Use a Beam standard transform once possible
        // TODO(pabloem, dhercher): Add a _metadata attribute to track whether a row comes from DLQ.
        return input.apply(Reshuffle.viaRandomKey())
            .apply(ParDo.of(new MoveAndConsumeFn()));
      }
    };
  }

  // TODO(pabloen): Switch over to use FileIO after BEAM-10246
  private static class MoveAndConsumeFn extends DoFn<Metadata, String> {
    private final List<ResourceId> filesToRemove = new ArrayList<>();

    private final Counter failedDeletions =
        Metrics.counter(MoveAndConsumeFn.class, "failedDeletions");
    private final Counter reconsumedElements =
        Metrics.counter(MoveAndConsumeFn.class, "elementsReconsumedFromDeadLetterQueue");

    @ProcessElement
    public void process(
        @Element Metadata dlqFile,
        OutputReceiver<String> outputs) throws IOException {
      // First we move the file to a temporary location so it will not be picked up
      // by the DLQ picker again.
      ResourceId newFileLocation = dlqFile.resourceId()
          .getCurrentDirectory()
          .resolve(TEMPORARY_HOLD_SUBDIRECTORY, StandardResolveOptions.RESOLVE_DIRECTORY)
          .resolve(dlqFile.resourceId().getFilename(), StandardResolveOptions.RESOLVE_FILE);

      LOG.info("Moving DLQ file {} to {}", dlqFile.resourceId().getFilename(), newFileLocation);
      // If this move has a failure, this means that the file has already been moved, thus we
      // ignore it.
      FileSystems.copy(
          Collections.singletonList(dlqFile.resourceId()),
          Collections.singletonList(newFileLocation),
          StandardMoveOptions.IGNORE_MISSING_FILES);

      InputStream jsonStream = Channels.newInputStream(FileSystems.open(newFileLocation));
      BufferedReader jsonReader = new BufferedReader(new InputStreamReader(jsonStream));

      // Assuming that files are JSONLines formatted.
      jsonReader.lines().forEach(line -> {
        outputs.output(line);
        reconsumedElements.inc();
      });
      this.filesToRemove.add(dlqFile.resourceId());
      this.filesToRemove.add(newFileLocation);
    }

    @FinishBundle
    public void cleanupFiles() {
      for (ResourceId file : filesToRemove) {
        try {
          FileSystems.delete(Collections.singleton(file));
        } catch (IOException e) {
          LOG.error("Unable to delete file {}. Exception: {}", file, e);
          failedDeletions.inc();
        }
      }
      filesToRemove.clear();
    }
  }
}
