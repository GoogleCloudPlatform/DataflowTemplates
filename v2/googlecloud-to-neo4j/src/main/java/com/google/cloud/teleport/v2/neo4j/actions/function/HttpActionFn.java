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

import static com.google.cloud.teleport.v2.neo4j.utils.HttpUtils.isPostRequest;

import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.neo4j.importer.v1.actions.HttpAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpActionFn extends DoFn<Integer, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpActionFn.class);

  private final HttpAction action;

  public HttpActionFn(ActionContext context) {
    this.action = ((HttpAction) context.getAction());
    if (StringUtils.isEmpty(action.getUrl())) {
      throw new RuntimeException("URL not provided for HTTP action.");
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws InterruptedException {
    try (CloseableHttpResponse response =
        HttpUtils.getHttpResponse(
            isPostRequest(action.getMethod()), action.getUrl(), action.getHeaders())) {
      LOG.info(
          "Executing HTTP {} transform, returned: {}",
          action.getName(),
          HttpUtils.getResponseContent(response));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Exception executing HTTP %s transform: %s", action.getName(), e.getMessage()),
          e);
    }
  }
}
