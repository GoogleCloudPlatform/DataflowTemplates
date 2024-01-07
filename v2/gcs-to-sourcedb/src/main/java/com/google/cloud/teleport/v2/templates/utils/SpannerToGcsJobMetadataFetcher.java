/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.metadata.SpannerToGcsJobMetadata;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fetches the Spanner to GCS job Metadata. */
public class SpannerToGcsJobMetadataFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToGcsJobMetadataFetcher.class);

  public static SpannerToGcsJobMetadata getSpannerToGcsJobMetadata(
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String tableSuffix,
      String runId)
      throws InterruptedException {
    SpannerDao spannerDao =
        new SpannerDao(spannerProjectId, metadataInstance, metadataDatabase, tableSuffix);

    SpannerToGcsJobMetadata response = spannerDao.getSpannerToGcsJobMetadata(runId);
    while (response == null) {
      LOG.info("SpannerToGcsJobMetadata response is null, will retry");
      Thread.sleep(5000);
      response = spannerDao.getSpannerToGcsJobMetadata(runId);
    }
    spannerDao.close();
    return response;
  }
}
