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
package com.google.cloud.teleport.v2.neo4j.actions.transforms;

import com.google.cloud.teleport.v2.neo4j.model.job.Action;
import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http GET action handler. */
public class HttpGetActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpGetActionTransform.class);
  private final Action action;
  private final ActionContext context;

  public HttpGetActionTransform(Action action, ActionContext context) {
    this.action = action;
    this.context = context;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    String uri = action.options.get("url");
    if (StringUtils.isEmpty(uri)) {
      throw new RuntimeException("Options 'uri' not provided for preload http_get action.");
    }
    try {
      CloseableHttpResponse response =
          HttpUtils.getHttpResponse(false, uri, action.options, action.headers);
      LOG.info("Request returned: {}", HttpUtils.getResponseContent(response));

    } catch (Exception e) {
      LOG.error("Exception making http get request: {}", e.getMessage());
    }

    // we are not running anything that generates an output, so can return an input.
    return input;
  }
}
