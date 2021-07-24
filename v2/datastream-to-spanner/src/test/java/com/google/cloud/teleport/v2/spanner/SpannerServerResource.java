/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.rules.ExternalResource;

/** Facilitates setup and deletion of a Spanner database for integration tests. */
public class SpannerServerResource extends ExternalResource {
  // Modify the following parameters to match your Cloud Spanner instance.
  private final String projectId = "span-cloud-testing";
  private final String instanceId = "test-instance";
  // TODO: Override host for emulator
  private final String host = "https://spanner.googleapis.com";

  private Spanner client;
  private DatabaseAdminClient databaseAdminClient;

  @Override
  protected void before() {
    SpannerOptions spannerOptions =
        SpannerOptions.newBuilder().setProjectId(projectId).setHost(host).build();
    client = spannerOptions.getService();
    databaseAdminClient = client.getDatabaseAdminClient();
  }

  @Override
  protected void after() {
    client.close();
  }

  public void createDatabase(String dbName, Iterable<String> ddlStatements) throws Exception {
    // Waits for create database to complete.
    databaseAdminClient.createDatabase(instanceId, dbName, ddlStatements).get();
  }

  public void updateDatabase(String dbName, Iterable<String> ddlStatements) throws Exception {
    databaseAdminClient
        .updateDatabaseDdl(instanceId, dbName, ddlStatements, /*operationId=*/ null)
        .get();
  }

  public void dropDatabase(String dbName) {
    try {
      databaseAdminClient.dropDatabase(instanceId, dbName);
    } catch (SpannerException e) {
      // Does not exist, ignore.
    }
  }

  public BatchClient getBatchClient(String dbName) {
    return client.getBatchClient(DatabaseId.of(projectId, instanceId, dbName));
  }

  public DatabaseClient getDbClient(String dbName) {
    return client.getDatabaseClient(DatabaseId.of(projectId, instanceId, dbName));
  }

  public SpannerConfig getSpannerConfig(String dbName) {
    return SpannerConfig.create()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(dbName)
        .withHost(ValueProvider.StaticValueProvider.of(host));
  }
}
