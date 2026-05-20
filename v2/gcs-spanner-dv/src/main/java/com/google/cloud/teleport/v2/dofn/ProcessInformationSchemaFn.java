/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.InformationSchemaScanner;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;

public class ProcessInformationSchemaFn extends DoFn<Void, Ddl> {

  private final SpannerConfig spannerConfig;

  private transient SpannerAccessor spannerAccessor;
  private transient Dialect dialect;

  public ProcessInformationSchemaFn(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Setup
  public void setup() throws Exception {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);

    DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
    dialect =
        databaseAdminClient
            .getDatabase(spannerConfig.getInstanceId().get(), spannerConfig.getDatabaseId().get())
            .getDialect();
  }

  @Teardown
  public void teardown() throws Exception {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Ddl mainDdl = getInformationSchemaAsDdl(spannerAccessor, dialect);
    c.output(mainDdl);
  }

  private Ddl getInformationSchemaAsDdl(SpannerAccessor accessor, Dialect dialect) {
    BatchClient batchClient = accessor.getBatchClient();
    BatchReadOnlyTransaction context =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(context, dialect);
    return scanner.scan();
  }
}
