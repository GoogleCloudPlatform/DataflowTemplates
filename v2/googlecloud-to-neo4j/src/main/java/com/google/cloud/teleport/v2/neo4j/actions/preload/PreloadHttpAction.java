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
package com.google.cloud.teleport.v2.neo4j.actions.preload;

import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.HttpAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http action handler. */
public class PreloadHttpAction implements PreloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(PreloadHttpAction.class);

  private HttpAction action;

  @Override
  public void configure(Action action, ActionContext context) {
    this.action = (HttpAction) action;
  }

  @Override
  public List<String> execute() {
    List<String> msgs = new ArrayList<>();
    String uri = action.getUrl();
    if (StringUtils.isEmpty(uri)) {
      throw new RuntimeException("Options 'uri' not provided for preload http_post action.");
    }
    try (CloseableHttpResponse response =
        HttpUtils.getHttpResponse(
            HttpUtils.isPostRequest(action.getMethod()), uri, action.getHeaders())) {
      LOG.info("Request returned: {}", HttpUtils.getResponseContent(response));

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Exception making http post request: %s", e.getMessage()), e);
    }

    return msgs;
  }
}
