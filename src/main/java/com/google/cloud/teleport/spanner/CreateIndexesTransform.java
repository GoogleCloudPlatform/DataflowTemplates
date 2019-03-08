package com.google.cloud.teleport.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Operation;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/** A Beam transform that creates indexes for all tables in a Cloud Spanner database. */
class CreateIndexesTransform extends PTransform<PCollection<Ddl>, PCollection<Void>> {

  private final SpannerConfig spannerConfig;
  private final ValueProvider<Boolean> waitForIndexes;

  public CreateIndexesTransform(
      SpannerConfig spannerConfig, ValueProvider<Boolean> waitForIndexes) {
    this.spannerConfig = spannerConfig;
    this.waitForIndexes = waitForIndexes;
  }

  @Override
  public PCollection<Void> expand(PCollection<Ddl> input) {
    return input.apply(
        "Create Indexes",
        ParDo.of(
            new DoFn<Ddl, Void>() {

              private transient SpannerAccessor spannerAccessor;

              @Setup
              public void setup() {
                spannerAccessor = spannerConfig.connectToSpanner();
              }

              @Teardown
              public void teardown() {
                spannerAccessor.close();
              }

              @ProcessElement
              public void processElement(ProcessContext c) {
                Ddl ddl = c.element();
                DatabaseAdminClient databaseAdminClient = spannerAccessor.getDatabaseAdminClient();
                List<String> createIndexStatements = ddl.createIndexStatements();
                if (!createIndexStatements.isEmpty()) {
                  // This just kicks off the index creation, it does not wait for it to complete.
                  Operation<Void, UpdateDatabaseDdlMetadata> op =
                      databaseAdminClient.updateDatabaseDdl(
                          spannerConfig.getInstanceId().get(),
                          spannerConfig.getDatabaseId().get(),
                          createIndexStatements,
                          null);
                  if (waitForIndexes.get()) {
                    op.waitFor();
                  }
                }
              }
            }));
  }
}
