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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Beam transform which reads information schema using {@link InformationSchemaScanner} and outputs
 * the information schema as a DDL object.
 */
public class InformationSchemaReader {
  private static final Logger LOG = LoggerFactory.getLogger(InformationSchemaReader.class);

  public static Ddl getInformationSchemaAsDdl(SpannerConfig spannerConfig) {
    SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    Dialect dialect =
        databaseAdminClient
            .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
            .getDialect();
    BatchClient batchClient = spannerAccessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    Ddl ddl = scanner.scan();
    spannerAccessor.close();
    return ddl;
  }
}
