/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.auto.AutoTemplate;
import com.google.cloud.teleport.metadata.auto.Preprocessor;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromJdbc;
import com.google.cloud.teleport.v2.auto.blocks.WriteToPubSub;
import org.apache.beam.sdk.options.PipelineOptions;

@Template(
    name = "Jdbc_to_PubSub_Auto",
    category = TemplateCategory.BATCH,
    displayName = "JDBC to Pub/Sub Auto",
    description =
        "The Java Database Connectivity (JDBC) to Pub/Sub template is a batch pipeline that ingests data from "
            + "JDBC source and writes the resulting records to a pre-existing Pub/Sub topic as a JSON string.",
    blocks = {ReadFromJdbc.class, WriteToPubSub.class},
    flexContainerName = "jdbc-to-pubsub-auto",
    contactInformation = "https://cloud.google.com/support")
public class JdbcToPubSubAuto {

  public static void main(String[] args) {
    AutoTemplate.setup(JdbcToPubSubAuto.class, args, new DefaultPreprocessor());
  }

  static class DefaultPreprocessor implements Preprocessor<PipelineOptions> {
    @Override
    public void accept(PipelineOptions options) {}
  }
}
