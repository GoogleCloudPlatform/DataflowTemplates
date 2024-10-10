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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles Spanner interaction. */
public class SpannerDao {

  private SpannerAccessor spannerAccessor;
  private SpannerConfig spannerConfig;

  private static final Logger LOG = LoggerFactory.getLogger(SpannerDao.class);

  public SpannerDao(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
  }

  public SpannerDao(String projectId, String instanceId, String databaseId) {
    this.spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    this.spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
  }

  // used for unit testing
  public SpannerDao(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
  }

  public ShadowTableRecord getShadowTableRecord(
      String tableName, com.google.cloud.spanner.Key primaryKey) {
    try {
      DatabaseClient databaseClient = spannerAccessor.getDatabaseClient();
      Struct row =
          databaseClient
              .singleUse()
              .readRow(
                  tableName,
                  primaryKey,
                  Arrays.asList(
                      Constants.PROCESSED_COMMIT_TS_COLUMN_NAME, Constants.RECORD_SEQ_COLUMN_NAME));

      // This is the first event for the primary key and hence the latest event.
      if (row == null) {
        return null;
      }

      return new ShadowTableRecord(row.getTimestamp(0), row.getLong(1));
    } catch (Exception e) {
      LOG.warn("The " + tableName + " table could not be read. ", e);
      // We need to throw the original exception such that the caller can
      // look at SpannerException class to take decision
      throw e;
    }
  }

  public void updateShadowTable(Mutation mutation) {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(mutation);
    spannerAccessor.getDatabaseClient().write(mutations);
  }

  public void close() {
    spannerAccessor.close();
  }
}
