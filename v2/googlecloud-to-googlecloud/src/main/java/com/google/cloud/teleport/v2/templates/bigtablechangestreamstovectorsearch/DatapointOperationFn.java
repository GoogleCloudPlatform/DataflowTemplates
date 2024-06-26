/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;

public abstract class DatapointOperationFn<InputT> extends DoFn<InputT, String> {
  private String endpoint;
  protected String indexName;

  protected transient IndexServiceClient client;

  protected abstract Logger logger();

  public DatapointOperationFn(String endpoint, String indexName) {
    this.indexName = indexName;
    this.endpoint = endpoint;
  }

  @Setup
  public void setup() {
    logger().info("Connecting to vector search endpoint {}", endpoint);
    logger().info("Using index {}", indexName);

    try {
      client =
          IndexServiceClient.create(
              IndexServiceSettings.newBuilder().setEndpoint(endpoint).build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
