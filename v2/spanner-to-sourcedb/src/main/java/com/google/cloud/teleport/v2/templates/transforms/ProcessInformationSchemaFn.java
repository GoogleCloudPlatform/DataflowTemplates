/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.transforms;

import static com.google.cloud.teleport.v2.templates.transforms.SpannerInformationSchemaProcessorTransform.MAIN_DDL_TAG;
import static com.google.cloud.teleport.v2.templates.transforms.SpannerInformationSchemaProcessorTransform.SHADOW_TABLE_DDL_TAG;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessInformationSchemaFn.class);

  private final SpannerConfig spannerConfig;
  private final SpannerConfig shadowTableSpannerConfig;
  private final String shadowTablePrefix;

  private transient SpannerAccessor spannerAccessor;
  private transient SpannerAccessor shadowTableSpannerAccessor;
  private transient Dialect dialect;
  private transient Dialect shadowTableDialect;

  public ProcessInformationSchemaFn(
      SpannerConfig spannerConfig,
      SpannerConfig shadowTableSpannerConfig,
      String shadowTablePrefix) {
    this.spannerConfig = spannerConfig;
    this.shadowTableSpannerConfig = shadowTableSpannerConfig;
    this.shadowTablePrefix = shadowTablePrefix;
  }

  @Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    shadowTableSpannerAccessor = SpannerAccessor.getOrCreate(shadowTableSpannerConfig);

    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    dialect =
        databaseAdminClient
            .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
            .getDialect();

    DatabaseAdminClient shadowDatabaseAdminClient =
        shadowTableSpannerAccessor.getDatabaseAdminClient();
    shadowTableDialect =
        shadowDatabaseAdminClient
            .getDatabase(
                shadowTableSpannerConfig.getInstanceId().get(),
                shadowTableSpannerConfig.getDatabaseId().get())
            .getDialect();
  }

  @Teardown
  public void teardown() throws Exception {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
    if (shadowTableSpannerAccessor != null) {
      shadowTableSpannerAccessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    // 1. Create Shadow Tables if needed
    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            spannerConfig, shadowTableSpannerConfig, shadowTableDialect, shadowTablePrefix);

    // This method internally fetches DDLs and creates tables.
    shadowTableCreator.createShadowTablesInSpanner();

    // 2. Fetch DDLs
    Ddl mainDdl = getInformationSchemaAsDdl(spannerAccessor, dialect);
    Ddl shadowTableDdl = getInformationSchemaAsDdl(shadowTableSpannerAccessor, shadowTableDialect);

    c.output(MAIN_DDL_TAG, mainDdl);
    c.output(SHADOW_TABLE_DDL_TAG, shadowTableDdl);
  }

  private Ddl getInformationSchemaAsDdl(SpannerAccessor accessor, Dialect dialect) {
    BatchClient batchClient = accessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    return scanner.scan();
  }
}
