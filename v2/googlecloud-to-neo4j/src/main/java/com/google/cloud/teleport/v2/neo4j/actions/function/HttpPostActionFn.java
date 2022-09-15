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
package com.google.cloud.teleport.v2.neo4j.actions.function;

import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.values.Row;

/** Http POST action handler. */
public class HttpPostActionFn extends org.apache.beam.sdk.transforms.DoFn<Integer, Row> {

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(HttpGetActionFn.class);

  private final com.google.cloud.teleport.v2.neo4j.model.job.ActionContext context;
  private final String uri;

  public HttpPostActionFn(com.google.cloud.teleport.v2.neo4j.model.job.ActionContext context) {
    this.context = context;
    this.uri = this.context.action.options.get("url");
    if (org.apache.commons.lang3.StringUtils.isEmpty(uri)) {
      throw new RuntimeException("Options 'url' not provided for preload http_get action.");
    }
  }

  @Setup
  public void setup() {
    // Nothing to setup
  }

  @ProcessElement
  public void processElement(ProcessContext context)
      throws InterruptedException { // executing http get *once*
    // note: this is not guaranteed to execute just once.  if there are many input rows, it could be
    // several times.  right now there are just a handful of rows.
    try {
      org.apache.http.client.methods.CloseableHttpResponse response =
          com.google.cloud.teleport.v2.neo4j.utils.HttpUtils.getHttpResponse(
              true, uri, this.context.action.options, this.context.action.headers);
      LOG.info(
          "Executing http_post {} transform, returned: {}",
          this.context.action.name,
          HttpUtils.getResponseContent(response));

    } catch (Exception e) {
      LOG.error(
          "Exception executing http_post {} transform: {}",
          this.context.action.name,
          e.getMessage());
    }
  }

  @Teardown
  public void tearDown() throws Exception {
    // Nothing to tear down
  }
}
