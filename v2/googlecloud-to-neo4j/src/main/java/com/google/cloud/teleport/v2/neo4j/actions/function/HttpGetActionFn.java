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

import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http GET action handler. */
public class HttpGetActionFn extends DoFn<Integer, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpGetActionFn.class);

  private final ActionContext context;
  private final String uri;

  public HttpGetActionFn(ActionContext context) {
    this.context = context;
    this.uri = this.context.action.options.get("url");
    if (StringUtils.isEmpty(uri)) {
      throw new RuntimeException("Options 'url' not provided for preload http_get action.");
    }
  }

  @Setup
  public void setup() {
    // Nothing to setup
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws InterruptedException {
    // executing http get *once*
    // note: this is not guaranteed to execute just once.  if there are many input rows, it could be
    // several times.  right now there are just a handful of rows.
    try {
      CloseableHttpResponse response =
          HttpUtils.getHttpResponse(
              false, uri, this.context.action.options, this.context.action.headers);
      LOG.info(
          "Executing http_get {} transform, returned: {}",
          this.context.action.name,
          HttpUtils.getResponseContent(response));

    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Exception executing http_get %s transform: %s",
              this.context.action.name, e.getMessage()),
          e);
    }
  }

  @Teardown
  public void tearDown() throws Exception {
    // Nothing to tear down
  }
}
