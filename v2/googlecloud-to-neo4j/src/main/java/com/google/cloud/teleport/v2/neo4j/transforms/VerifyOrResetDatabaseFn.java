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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import org.apache.beam.sdk.transforms.DoFn;

public class VerifyOrResetDatabaseFn extends DoFn<Long, Long> {
  private final SerializableSupplier<ConnectionParams> connectionParamsProvider;

  private final String templateVersion;

  private final boolean resetDb;

  private transient Neo4jConnection connection;

  public VerifyOrResetDatabaseFn(
      SerializableSupplier<ConnectionParams> connectionParamsProvider,
      String templateVersion,
      boolean resetDb) {
    this.connectionParamsProvider = connectionParamsProvider;
    this.templateVersion = templateVersion;
    this.resetDb = resetDb;
  }

  @Setup
  public void prepare() {
    this.connection = new Neo4jConnection(connectionParamsProvider.get(), templateVersion);
  }

  @Teardown
  public void close() {
    if (connection != null) {
      connection.close();
    }
  }

  @ProcessElement
  public void process(ProcessContext context) {
    if (resetDb) {
      connection.resetDatabase();
    } else {
      connection.verifyConnectivity();
    }
    context.output(1L);
  }
}
