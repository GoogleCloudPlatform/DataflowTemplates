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
package com.google.cloud.teleport.templates;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.RandomInsertMutationGenerator;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.rules.ExternalResource;

/** Facilitates setup and deletion of a Spanner database for integration tests. */
public class SpannerServerResource extends ExternalResource {
  // Modify the following parameters to match your Cloud Spanner instance.
  private static final String EMULATOR_HOST = System.getenv("SPANNER_EMULATOR_HOST");
  private static final String DEFAULT_PROJECT_ID = "span-cloud-testing";
  private static final String DEFAULT_INSTANCE_ID = "test-instance";
  private final String host = "https://spanner.googleapis.com";
  private final String projectId;
  private final String instanceId;

  private Spanner client;
  private DatabaseAdminClient databaseAdminClient;

  public SpannerServerResource() {
    this.projectId = System.getProperty("projectId", DEFAULT_PROJECT_ID);
    this.instanceId = System.getProperty("instanceId", DEFAULT_INSTANCE_ID);
  }

  @Override
  protected void before() {
    SpannerOptions spannerOptions;
    if (EMULATOR_HOST == null) {
      spannerOptions =
          SpannerOptions.newBuilder().setProjectId(this.projectId).setHost(host).build();
    } else {
      spannerOptions =
          SpannerOptions.newBuilder()
              .setProjectId(this.projectId)
              .setEmulatorHost(EMULATOR_HOST)
              .build();
    }
    client = spannerOptions.getService();
    databaseAdminClient = client.getDatabaseAdminClient();
  }

  @Override
  protected void after() {
    client.close();
  }

  public void createDatabase(String dbName, Iterable<String> ddlStatements) throws Exception {
    // Waits for create database to complete.
    databaseAdminClient.createDatabase(this.instanceId, dbName, ddlStatements).get();
  }

  public void updateDatabase(String dbName, Iterable<String> ddlStatements) throws Exception {
    databaseAdminClient.updateDatabaseDdl(this.instanceId, dbName, ddlStatements, null).get();
  }

  public void dropDatabase(String dbName) {
    try {
      databaseAdminClient.dropDatabase(this.instanceId, dbName);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }
  }

  public BatchClient getBatchClient(String dbName) {
    return client.getBatchClient(DatabaseId.of(this.projectId, this.instanceId, dbName));
  }

  public DatabaseClient getDbClient(String dbName) {
    return client.getDatabaseClient(DatabaseId.of(this.projectId, this.instanceId, dbName));
  }

  public SpannerConfig getSpannerConfig(String dbName) {
    if (EMULATOR_HOST == null) {
      return SpannerConfig.create()
          .withProjectId(this.projectId)
          .withInstanceId(this.instanceId)
          .withDatabaseId(dbName)
          .withHost(ValueProvider.StaticValueProvider.of(host));
    } else {
      return SpannerConfig.create()
          .withProjectId(this.projectId)
          .withInstanceId(this.instanceId)
          .withDatabaseId(dbName)
          .withEmulatorHost(ValueProvider.StaticValueProvider.of(EMULATOR_HOST));
    }
  }

  public void populateRandomData(String db, Ddl ddl, int numBatches) throws Exception {

    final Iterator<MutationGroup> mutations =
        new RandomInsertMutationGenerator(ddl).stream().iterator();

    for (int i = 0; i < numBatches; i++) {
      TransactionRunner transactionRunner = getDbClient(db).readWriteTransaction();
      transactionRunner.run(
          new TransactionRunner.TransactionCallable<Void>() {

            @Nullable
            @Override
            public Void run(TransactionContext transaction) {
              for (int i = 0; i < 10; i++) {
                MutationGroup m = mutations.next();
                transaction.buffer(m);
              }
              return null;
            }
          });
    }
  }
}
