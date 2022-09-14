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
package com.google.cloud.teleport.v2.neo4j.actions.doFn;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;

/** Query action handler. */
public class BigQueryActionFn extends DoFn<Integer, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryActionFn.class);

  private final ActionContext context;
  private final String sql;

  public BigQueryActionFn(ActionContext context) {
    this.context = context;
    this.sql = this.context.action.options.get("sql");
    if (org.apache.commons.lang3.StringUtils.isEmpty(sql)) {
      throw new RuntimeException("Options 'sql' not provided for preload query transform.");
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws InterruptedException {
    executeBqQuery(sql);
  }

  private void executeBqQuery(String sql){

    try {
      com.google.cloud.bigquery.BigQuery bigquery = com.google.cloud.bigquery.BigQueryOptions.getDefaultInstance().getService();
      com.google.cloud.bigquery.QueryJobConfiguration queryConfig = com.google.cloud.bigquery.QueryJobConfiguration.newBuilder(sql).build();
      LOG.info("Executing BQ action sql: {}", sql);
      com.google.cloud.bigquery.TableResult queryResult = bigquery.query(queryConfig);
      LOG.info("Result rows: {}", queryResult.getTotalRows());

    } catch (Exception e) {
      LOG.error("Exception executing BQ action sql {}: {}", sql, e.getMessage());
    }
  }
}
