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
package com.google.cloud.teleport.dfmetrics.output;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.teleport.dfmetrics.model.JobInfo;
import com.google.cloud.teleport.dfmetrics.utils.PrettyLogger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link BigqueryStore} encapsulates the functionality to write the job metrics to BigQuery.
 */
public class BigqueryStore implements IOutputStore {

  private static final Logger LOG = LoggerFactory.getLogger(BigqueryStore.class);

  private static final String PROJECT_ID_TAG = "projectId";
  private static final String DATASET_ID_TAG = "dataset";
  private static final String TABLE_ID_TAG = "table";

  private static final String PROJECT_PATTERN = "[a-zA-Z0-9\\.\\-\\:]+";
  private static final String DATASET_PATTERN = "[a-zA-Z_][a-zA-Z0-9\\_]+";
  private static final String TABLE_PATTERN = "[a-zA-Z0-9\\_]+";

  public static final String BQ_TABLE_PATTERN_ERROR =
      "Invalid bigquery tablespec format and should be " + "project.dataset.table";

  public static final String BQ_TABLE_PATTERN =
      String.format(
          "^(?<%s>%s)[:\\.](?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG,
          PROJECT_PATTERN,
          DATASET_ID_TAG,
          DATASET_PATTERN,
          TABLE_ID_TAG,
          TABLE_PATTERN);

  private final TableReference tableReference;

  public BigqueryStore(String tableSpec) {
    this.tableReference = getTableReference(tableSpec);
  }

  /**
   * Returns Bigquery table reference based on project, dataset and table values.
   *
   * @param tableSpec - Table specification
   * @return BigQuery table reference
   */
  public static TableReference getTableReference(String tableSpec) {
    Matcher tableIdMatcher = Pattern.compile(BQ_TABLE_PATTERN).matcher(tableSpec);
    if (tableIdMatcher.matches()) {
      return new TableReference()
          .setProjectId(tableIdMatcher.group(1))
          .setDatasetId(tableIdMatcher.group(2))
          .setTableId(tableIdMatcher.group(3));
    }
    throw new IllegalArgumentException(BQ_TABLE_PATTERN_ERROR);
  }

  /**
   * Validates if given table spec matches Bigquery table pattern.
   *
   * @param tableSpec - Table specification
   * @return Boolean value indicating if table spec is valid or not
   */
  private static boolean isValidTableId(String tableSpec) {
    return Pattern.compile(BQ_TABLE_PATTERN).matcher(tableSpec).matches();
  }

  /**
   * Writes record to BigQuery.
   *
   * @throws Exception
   */
  @Override
  public void load(JobInfo jobInfo, Map<String, Double> metrics) {
    LOG.debug("Loading metrics:\n{}", PrettyLogger.logMap(metrics));
    try {
      // Create a bq service instance
      BigQuery bigquery =
          BigQueryOptions.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .build()
              .getService();

      // Create a table row
      Map<String, Object> row = new HashMap<>();
      row.put("run_timestamp", Instant.now().toString());
      row.put("job_create_timestamp", jobInfo.createTime());
      row.put("sdk", jobInfo.sdk());
      row.put("version", jobInfo.sdkVersion());
      row.put("job_type", jobInfo.jobType());
      addIfValueExists(row, "template_name", jobInfo.templateName());
      addIfValueExists(row, "template_version", jobInfo.templateVersion());
      addIfValueExists(row, "template_type", jobInfo.templateType());
      row.put("pipeline_name", jobInfo.pipelineName());

      // Convert parameters map to list of table row since it's a repeated record
      List<TableRow> parameterRows = new ArrayList<>();
      if (jobInfo.parameters() != null) {
        for (Map.Entry<String, String> entry : jobInfo.parameters().entrySet()) {
          TableRow parameterRow =
              new TableRow().set("name", entry.getKey()).set("value", entry.getValue());
          parameterRows.add(parameterRow);
        }
      }
      row.put("parameters", parameterRows);

      // Convert metrics map to list of table row since it's a repeated record
      List<TableRow> metricRows = new ArrayList<>();
      for (Map.Entry<String, Double> entry : metrics.entrySet()) {
        TableRow metricRow =
            new TableRow().set("name", entry.getKey()).set("value", entry.getValue());
        metricRows.add(metricRow);
      }
      row.put("metrics", metricRows);

      // Create insertAll (streaming) request
      InsertAllRequest insertAllRequest =
          InsertAllRequest.newBuilder(tableReference.getDatasetId(), tableReference.getTableId())
              .addRow(row)
              .build();

      // Insert data into table
      InsertAllResponse response = bigquery.insertAll(insertAllRequest);

      // Check for errors and raise exception
      if (response.hasErrors()) {
        StringBuilder errors = new StringBuilder();
        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
          errors.append(
              System.out.printf(
                  "error in entry %d: %s \n", entry.getKey(), entry.getValue().toString()));
        }
        throw new RuntimeException("Errors inserting to BigQuery:" + errors);
      }
    } catch (IllegalStateException e) {
      LOG.error("Unable to export results to bigquery. ", e);
    }
  }
}
