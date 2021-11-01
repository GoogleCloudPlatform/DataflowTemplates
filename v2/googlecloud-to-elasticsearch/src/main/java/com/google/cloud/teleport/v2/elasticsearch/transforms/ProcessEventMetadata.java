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
package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.cloud.teleport.v2.elasticsearch.options.PubSubToElasticsearchOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ProcessEventMetadata is used to enrich input message from Pub/Sub with metadata. */
public class ProcessEventMetadata extends PTransform<PCollection<String>, PCollection<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessEventMetadata.class);

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new EventMetadataFn()));
  }

  static class EventMetadataFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      String input = context.element();
      PubSubToElasticsearchOptions options =
          context.getPipelineOptions().as(PubSubToElasticsearchOptions.class);

      //Log input message
      if (options.getVerboseDebugMessages()) {
        LOG.info("Input message: " + input);
      }

      context.output(EventMetadataBuilder.build(input, options).getEnrichedMessageAsString());
    }
  }
}
