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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToBigQueryOptions;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class OptionsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OptionsUtils.class);

  public static List<String> processBigQueryProjectAndDataset(
      SpannerChangeStreamsToBigQueryOptions options) {
    String bigqueryProjectId = options.getBigQueryProjectId();
    String bigqueryDataset = options.getBigQueryDataset();
    LOG.info("===bigqueryDataset: " + bigqueryDataset);
    int datasetStartPos = bigqueryDataset.indexOf(".");
    if (datasetStartPos != -1) {
      String inferredBigQueryProjectId = bigqueryDataset.substring(0, datasetStartPos);
      if (bigqueryProjectId.isEmpty()) {
        bigqueryProjectId = inferredBigQueryProjectId;
      } else if (!bigqueryProjectId.equals(inferredBigQueryProjectId)) {
        throw new IllegalArgumentException(
            "BigQuery dataset must be in the same project as BigQuery project ID: "
                + bigqueryProjectId);
      }
    }
    bigqueryProjectId = bigqueryProjectId.isEmpty() ? options.getProject() : bigqueryProjectId;
    bigqueryDataset = bigqueryDataset.substring(datasetStartPos + 1);

    List<String> results = Arrays.asList(bigqueryProjectId, bigqueryDataset);
    return results;
  }

  public static String getSpannerProjectId(SpannerChangeStreamsToBigQueryOptions options) {
    return options.getSpannerProjectId().isEmpty()
        ? options.getProject()
        : options.getSpannerProjectId();
  }

  private static String getBigQueryProjectId(SpannerChangeStreamsToBigQueryOptions options) {
    return options.getBigQueryProjectId().isEmpty()
        ? options.getProject()
        : options.getBigQueryProjectId();
  }
}
