/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn receives {@link BigQueryAction} instances, and issues them into BigQuery. */
public class BigQueryStatementIssuingFn extends DoFn<KV<String, BigQueryAction>, Void> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStatementIssuingFn.class);

  private BigQuery bigQueryClient;

  final String jobIdPrefix;

  private final String projectId;

  BigQueryStatementIssuingFn(String jobIdPrefix, String projectId) {
    this.jobIdPrefix = jobIdPrefix;
    this.projectId = projectId;
  }

  @Setup
  public void setUp() {
    bigQueryClient = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  @ProcessElement
  public void process(ProcessContext c) throws InterruptedException {
    BigQueryAction action = c.element().getValue();

    if (BigQueryAction.CREATE_TABLE.equals(action.action)) {
      Table bqTable = createBigQueryTable(action);
      LOG.info("Created table: {}", bqTable);
    } else {
      assert BigQueryAction.STATEMENT.equals(action.action);
      Job jobInfo = issueQueryToBQ(action.statement);
      LOG.info("Job Info for triggered job: {}", jobInfo);
      jobInfo = jobInfo.waitFor();
    }
  }

  private Table createBigQueryTable(BigQueryAction action) {
    TableDefinition definition =
        StandardTableDefinition.of(
            BigQuerySchemaUtils.beamSchemaToBigQueryClientSchema(action.tableSchema));

    TableId tableId = TableId.of(action.projectId, action.dataset, action.tableName);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, definition).build();

    LOG.info("Creating a new BigQuery table: {}", tableInfo);

    try {
      return bigQueryClient.create(tableInfo);
    } catch (BigQueryException e) {
      if (e.getMessage().startsWith("Already Exists")) {
        return null;
      } else {
        throw e;
      }
    }
  }

  private Job issueQueryToBQ(String statement) throws InterruptedException {
    QueryJobConfiguration jobConfiguration = QueryJobConfiguration.newBuilder(statement).build();

    String jobId = makeJobId(jobIdPrefix, statement);

    LOG.info("Triggering job {} for statement |{}|", jobId, statement);

    TableResult result = bigQueryClient.query(jobConfiguration, JobId.of(jobId));
    return bigQueryClient.getJob(JobId.of(jobId));
  }

  static String makeJobId(String jobIdPrefix, String statement) {
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ssz").withZone(ZoneId.of("UTC"));
    String randomId = UUID.randomUUID().toString();
    return String.format(
        "%s_%d_%s_%s",
        jobIdPrefix, Math.abs(statement.hashCode()), formatter.format(Instant.now()), randomId);
  }
}
