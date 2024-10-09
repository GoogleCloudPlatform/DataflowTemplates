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
package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.BigQueryAction;

/** Query action handler. */
public class PreloadBigQueryAction implements PreloadAction {

  private BigQueryAction action;

  @Override
  public void configure(Action action, ActionContext context) {
    this.action = (BigQueryAction) action;
  }

  @Override
  public List<String> execute() {
    List<String> msgs = new ArrayList<>();
    String sql = action.getSql();
    if (StringUtils.isEmpty(sql)) {
      throw new RuntimeException("SQL Query not provided for preload query action.");
    }

    try {
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
      msgs.add("Query: " + sql);
      TableResult queryResult = bigquery.query(queryConfig);
      msgs.add("Result rows: " + queryResult.getTotalRows());

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Exception running sql %s: %s", sql, e.getMessage()), e);
    }

    return msgs;
  }
}
