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

import com.google.cloud.teleport.v2.neo4j.model.job.ActionContext;
import com.google.cloud.teleport.v2.neo4j.utils.HttpUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Http POST action handler. */
public class HttpPostOneActionTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpPostOneActionTransform.class);
  private final ActionContext context;

  public HttpPostOneActionTransform(ActionContext context) {
    this.context = context;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    String uri = this.context.action.options.get("url");
    if (StringUtils.isEmpty(uri)) {
      throw new RuntimeException("Options 'url' not provided for preload http_post action.");
    }

    return input.apply("Actions", ParDo.of(new DoFn<Row, Row>() {
      @Setup
      public void setup() {
        //Nothing to setup
      }

      @ProcessElement
      public void processElement(@Element Row row, OutputReceiver<Row> outputReceiver) {
        // not processing each row here.
        // This will be good for logging down the road
        //outputReceiver.output(row);
      }

      @FinishBundle
      public void bundleProcessed(FinishBundleContext c) {
        // executing http get *once*
        // note: this is not guaranteed to execute just once.  if there are many input rows, it could be several times.  right now there are just a handful of rows.
        try {
          CloseableHttpResponse response =
                  HttpUtils.getHttpResponse(true, uri, context.action.options, context.action.headers);
          LOG.info("Executing http_post {} transform, returned: {}", context.action.name, HttpUtils.getResponseContent(response));

        } catch (Exception e) {
          LOG.error("Exception executing http_post {} transform: {}", context.action.name, e.getMessage());
        }
      }

      @Teardown
      public void tearDown() throws Exception {
        //Nothing to tear down
      }

    })).setCoder(input.getCoder());

  }
}

