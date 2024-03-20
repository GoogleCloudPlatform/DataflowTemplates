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

import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.aiplatform.v1.UpsertDatapointsRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpsertDatapointsFn extends DatapointOperationFn<Iterable<IndexDatapoint>> {

  private static final Logger LOG = LoggerFactory.getLogger(UpsertDatapointsFn.class);

  protected Logger logger() {
    return LOG;
  }

  public UpsertDatapointsFn(String endpoint, String indexPath) {
    super(endpoint, indexPath);
  }

  @ProcessElement
  public void processElement(@Element Iterable<IndexDatapoint> datapoints) {
    LOG.debug("Upserting datapoints: {}", datapoints);
    UpsertDatapointsRequest request =
        UpsertDatapointsRequest.newBuilder()
            .addAllDatapoints(datapoints)
            .setIndex(indexPath)
            .build();

    // TODO(meagar): This may raise exceptions, which I believe kills the pipeline
    //  What is idiomatic: Allow them to kill the pipeline, or catch them an LOG an error?
    client.upsertDatapoints(request);
    LOG.debug("Done");
  }
}
