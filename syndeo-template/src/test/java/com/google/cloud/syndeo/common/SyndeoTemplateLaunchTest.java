/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.syndeo.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.syndeo.SyndeoTemplate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SyndeoTemplateLaunchTest {

  private static final String BIGTABLE_SINK_CONFIG =
      "{\n"
          + "  \"urn\": \"bigtable:write\",\n"
          + "  \"configurationParameters\": {\n"
          + "    \"projectId\": \"dataflow-syndeo\",\n"
          + "    \"instanceId\": \"syndeo-bt-test\",\n"
          + "    \"tableId\": \"syndeo-demo-table\",\n"
          + "    \"keyColumns\": [\"ride_id\"]\n"
          + "  }\n"
          + "}";

  private static final String BQ_TO_BQ_CONFIG =
      "{ \"source\": "
          + "{ \"urn\": \"bigquery:read\", "
          + "\"configurationParameters\": { \"tableSpec\": \"dataflow-syndeo:test_dataset.kafka_table\" } }, "
          + "\"sink\": { \"urn\": \"bigquery:write\", "
          + "\"configurationParameters\": { \"tableSpec\": \"dataflow-syndeo:test_dataset.source_table\" } } }";

  @Test
  public void testBuildWithListOfValuesBigTable() throws JsonProcessingException {
    ObjectMapper om = new ObjectMapper();
    JsonNode btconfig = om.readTree(BIGTABLE_SINK_CONFIG);
    SyndeoTemplate.buildFromJsonConfig(btconfig);
    // TODO(pabloem): Add more checks
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildWithBQToBQConfigBadSink() throws JsonProcessingException {
    SyndeoTemplate.buildFromJsonPayload(BQ_TO_BQ_CONFIG);
  }

  @Test
  public void testBuildWithBQToBQConfig() throws JsonProcessingException {
    // TODO(pabloem): Replace this with
    // "beam:schematransform:org.apache.beam:bigquery_storage_write:v1" for Beam  2.45.0.
    SyndeoTemplate.buildFromJsonPayload(
        BQ_TO_BQ_CONFIG.replace("bigquery:write", "schemaIO:bigquery:write"));
  }
}
