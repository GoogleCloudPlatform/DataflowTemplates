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
import com.google.cloud.teleport.v2.auto.blocks.PubsubMessageToTableRow;
import com.google.cloud.teleport.v2.auto.blocks.ReadFromPubSub;
import com.google.cloud.teleport.v2.auto.blocks.WriteToBigQuery;

@Template(
    name = "PubSub_to_BigQuery_Auto",
    category = TemplateCategory.STREAMING,
    displayName = "Pub/Sub to BigQuery Auto",
    description =
        "Streaming pipeline. Ingests JSON-encoded messages from a Pub/Sub subscription or topic, transforms them using a JavaScript user-defined function (UDF), and writes them to a pre-existing BigQuery table as BigQuery elements.",
    blocks = {ReadFromPubSub.class, PubsubMessageToTableRow.class, WriteToBigQuery.class},
    flexContainerName = "pubsub-to-bigquery-auto",
    contactInformation = "https://cloud.google.com/support")
public class PubSubToBigQueryAuto {

  public static void main(String[] args) {
    AutoTemplate.setup(PubSubToBigQueryAuto.class, args);
  }
}
