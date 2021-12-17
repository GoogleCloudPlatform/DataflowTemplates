/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A set of helper functions and classes for BigQuery. */
public class BigQueryUtils {
  /**
   * This regex isn't exact and allows patterns that would be rejected by the service, but is
   * sufficient for basic sanity checks.
   */
  private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

  private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

  /**
   * Matches table specifications in the form {@code "projects/[project_id]/datasets/[dataset_id]".
   */
  private static final String DATASET_URN_REGEXP =
      String.format(
          "projects/(?<PROJECT>%s)/datasets/(?<DATASET>%s)", PROJECT_ID_REGEXP, DATASET_REGEXP);

  private static final Pattern DATASET_URN_SPEC = Pattern.compile(DATASET_URN_REGEXP);

  public static DatasetId parseDatasetUrn(String datasetUrn) {
    // This should be eventually moved to BigQueryHelpers/BigQueryIO classes in Apache Beam SDK,
    // but at the moment it only supports parsing a full Table reference, not a Dataset reference,
    // so we'll keep Dataset parsing utils here.

    Matcher match = DATASET_URN_SPEC.matcher(datasetUrn);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Dataset reference is not in projects/[project_id]/datasets/[dataset_id] "
              + "format: "
              + datasetUrn);
    }
    return DatasetId.of(match.group("PROJECT"), match.group("DATASET"));
  }

  /**
   * Creates ReadSession for schema extraction.
   *
   * @param client BigQueryStorage client used to create ReadSession.
   * @param datasetId ID of the dataset to read from.
   * @param tableName Name of the table in the dataset {@code datasetId} to read from.
   * @return session ReadSession object that contains the schema for the export.
   */
  public static ReadSession createReadSession(
      BigQueryStorageClient client,
      DatasetId datasetId,
      String tableName,
      TableReadOptions options) {

    String parentProjectId = "projects/" + datasetId.getProject();

    TableReferenceProto.TableReference storageTableRef =
        TableReferenceProto.TableReference.newBuilder()
            .setProjectId(datasetId.getProject())
            .setDatasetId(datasetId.getDataset())
            .setTableId(tableName)
            .build();

    CreateReadSessionRequest.Builder builder =
        CreateReadSessionRequest.newBuilder()
            .setParent(parentProjectId)
            .setReadOptions(options)
            .setTableReference(storageTableRef);

    return client.createReadSession(builder.build());
  }
}
