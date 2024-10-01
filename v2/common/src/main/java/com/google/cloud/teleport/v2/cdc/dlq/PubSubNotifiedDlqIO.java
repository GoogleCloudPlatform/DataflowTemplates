/*
 * Copyright (C) 2024 Google LLC
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a DLQ reconsumer where the DLQ files reside in the GCS and the file path comes as a
 * PubSub message. It also skips files under specific configured directories , such as temporary and
 * severe, so that even if there are misconfigrations done when setting the GCS notifier, the list
 * of directories to ignore can be configured.
 */
public class PubSubNotifiedDlqIO extends PTransform<PBegin, PCollection<Metadata>> {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubNotifiedDlqIO.class);

  private final String gcsNotificationSubscription;
  private final List<String> filePathsToIgnore;

  public PubSubNotifiedDlqIO(String gcsNotificationSubscription, List<String> filePathsToIgnore) {
    this.gcsNotificationSubscription = gcsNotificationSubscription;
    this.filePathsToIgnore = filePathsToIgnore;
  }

  @Override
  public PCollection<Metadata> expand(PBegin input) {
    return input
        .apply(
            "ReadGcsPubSubSubscription",
            PubsubIO.readMessagesWithAttributes().fromSubscription(gcsNotificationSubscription))
        .apply("ExtractGcsFilePath", ParDo.of(new ExtractGcsFileForRetry()));
  }

  class ExtractGcsFileForRetry extends DoFn<PubsubMessage, Metadata> {
    @ProcessElement
    public void process(ProcessContext context) throws IOException {
      PubsubMessage message = context.element();
      String eventType = message.getAttribute("eventType");
      String bucketId = message.getAttribute("bucketId");
      String objectId = message.getAttribute("objectId");
      if (eventType.equals("OBJECT_FINALIZE") && !objectId.endsWith("/")) {
        String fileName = "gs://" + bucketId + "/" + objectId;
        try {
          Metadata fileMetadata = FileSystems.matchSingleFileSpec(fileName);
          if (filePathsToIgnore != null) {
            for (String filePathToIgnore : filePathsToIgnore) {
              if (fileMetadata.resourceId().toString().contains(filePathToIgnore)) {
                LOG.info(
                    "Ignoring severe or temporary file during retry processing {} due to ignore"
                        + " path {} ",
                    fileName,
                    filePathToIgnore);
                return;
              }
            }
          }
          context.output(fileMetadata);
        } catch (FileNotFoundException e) {
          LOG.warn("Ignoring non-existent file {}", fileName, e);
        } catch (IOException e) {
          LOG.error("GCS Failure retrieving {}", fileName, e);
          throw e;
        }
      }
    }
  }
}
