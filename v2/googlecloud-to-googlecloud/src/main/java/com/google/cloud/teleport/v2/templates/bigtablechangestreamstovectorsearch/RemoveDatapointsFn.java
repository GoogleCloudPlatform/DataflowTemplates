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

import com.google.cloud.aiplatform.v1.RemoveDatapointsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveDatapointsFn extends DatapointOperationFn<Iterable<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveDatapointsFn.class);

  protected Logger logger() {
    return LOG;
  }

  public RemoveDatapointsFn(String endpoint, String indexName) {
    super(endpoint, indexName);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    var datapointIds = c.element();
    LOG.info("Deleting datapoints: {}", datapointIds);

    // Appears to work, even when some datapoints don't exist
    RemoveDatapointsRequest request =
        RemoveDatapointsRequest.newBuilder()
            .addAllDatapointIds(datapointIds)
            .setIndex(indexName)
            .build();

    try {
      client.removeDatapoints(request);
    } catch (io.grpc.StatusRuntimeException e) {
      LOG.info("Failed to remove datapoints: {}", e.getLocalizedMessage());
      c.output("Error deleting datapoint: " + e.getLocalizedMessage());
    }

    LOG.info("Done");
  }
}
