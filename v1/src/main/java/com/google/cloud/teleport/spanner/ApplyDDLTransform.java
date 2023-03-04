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
package com.google.cloud.teleport.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.gcp.spanner.LocalSpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Beam transform that applies the DDL statements passed in a Cloud Spanner database and outputs
 * the original {@link Ddl}.
 */
class ApplyDDLTransform extends PTransform<PCollection<Ddl>, PCollection<Ddl>> {

  private final SpannerConfig spannerConfig;
  private final PCollectionView<List<String>> pendingDDLStatements;
  private final ValueProvider<Boolean> waitForApply;
  private static final Logger LOG = LoggerFactory.getLogger(ApplyDDLTransform.class);

  /**
   * Default constructor.
   *
   * @param spannerConfig the spanner config for database.
   * @param pendingDDLStatements the list of pending DDL statements to be applied.
   * @param waitForApply wait till all the ddl statements are committed.
   */
  public ApplyDDLTransform(
      SpannerConfig spannerConfig,
      PCollectionView<List<String>> pendingDDLStatements,
      ValueProvider<Boolean> waitForApply) {
    this.spannerConfig = spannerConfig;
    this.pendingDDLStatements = pendingDDLStatements;
    this.waitForApply = waitForApply;
  }

  @Override
  public PCollection<Ddl> expand(PCollection<Ddl> input) {
    return input.apply(
        "Apply DDL statements",
        ParDo.of(
                new DoFn<Ddl, Ddl>() {

                  private transient LocalSpannerAccessor spannerAccessor;

                  @Setup
                  public void setup() {
                    spannerAccessor = LocalSpannerAccessor.getOrCreate(spannerConfig);
                  }

                  @Teardown
                  public void teardown() {
                    spannerAccessor.close();
                  }

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Ddl ddl = c.element();
                    DatabaseAdminClient databaseAdminClient =
                        spannerAccessor.getDatabaseAdminClient();
                    List<String> statements = c.sideInput(pendingDDLStatements);
                    LOG.info("Applying DDL statements: {}", statements);
                    if (!statements.isEmpty()) {
                      // This just kicks off the applying the DDL statements.
                      // It does not wait for it to complete.
                      OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
                          databaseAdminClient.updateDatabaseDdl(
                              spannerConfig.getInstanceId().get(),
                              spannerConfig.getDatabaseId().get(),
                              statements,
                              null);
                      if (waitForApply.get()) {
                        try {
                          op.get();
                        } catch (InterruptedException | ExecutionException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                    c.output(ddl);
                  }
                })
            .withSideInputs(pendingDDLStatements));
  }
}
