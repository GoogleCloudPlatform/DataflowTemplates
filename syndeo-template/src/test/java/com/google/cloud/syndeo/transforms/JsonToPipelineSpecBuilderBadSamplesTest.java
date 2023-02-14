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
package com.google.cloud.syndeo.transforms;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.v1.SyndeoV1;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class JsonToPipelineSpecBuilderBadSamplesTest {
  public static final Logger LOG =
      LoggerFactory.getLogger(JsonToPipelineSpecBuilderBadSamplesTest.class);

  @Parameterized.Parameters
  public static List<JsonNode> loadSampleConfigs() {
    try {
      InputStream is = Resources.getResource("json_spec_payload_bad_samples.json").openStream();
      String sampleData = new String(is.readAllBytes(), StandardCharsets.UTF_8);

      ObjectMapper om = new ObjectMapper();
      JsonNode samples = om.readTree(sampleData);
      return Lists.newArrayList(samples.iterator());
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize sample configs. ", e);
    }
  }

  private Boolean canTestBuildPipeline() {
    return !sample.toString().contains("confluentSchemaRegistryUrl");
  }

  @Parameterized.Parameter public JsonNode sample;

  @Test
  public void testPipelinesCanBeBuiltWithJsonSampleSpecs() throws IOException {
    String errorMessage = sample.get("errorMessage").asText();
    Throwable e =
        assertThrows(
            Throwable.class,
            () -> {
              SyndeoV1.PipelineDescription desc =
                  SyndeoTemplate.buildFromJsonPayload(sample.get("jsonPayload").toString());
              List<ProviderUtil.TransformSpec> specs = new ArrayList<>();
              for (SyndeoV1.ConfiguredSchemaTransform inst : desc.getTransformsList()) {
                specs.add(new ProviderUtil.TransformSpec(inst));
              }
              Pipeline p = Pipeline.create();
              // Run pipeline from configuration.
              ProviderUtil.applyConfigs(specs, PCollectionRowTuple.empty(p));
              // TODO(pabloem): Add assertions to verify applications.
            });
    assertThat(e.getMessage(), Matchers.containsString(errorMessage));
  }
}
