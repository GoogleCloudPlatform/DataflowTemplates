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
package com.google.cloud.syndeo.transforms.datagenerator;

import static org.junit.Assert.assertNotNull;

import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.transforms.datagenerator.DataGeneratorSchemaTransformProvider.DataGeneratorSchemaTransformConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataGeneratorSchemaTransformTest {

  private static final String schema =
      "{\"type\":\"record\",\"name\":\"user_info_flat\",\"namespace\":\"com.google.syndeo\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"username\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"10\"},{\"name\":\"age\",\"type\":\"long\",\"default\":0},{\"name\":\"introduction\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"1000\"},{\"name\":\"street\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"city\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"state\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"25\"},{\"name\":\"country\",\"type\":\"string\",\"default\":\"NONE\",\"size\":\"15\"}]}";

  @Test
  public void testBuildTransform() throws Exception {
    DataGeneratorSchemaTransformProvider provider =
        (DataGeneratorSchemaTransformProvider)
            ProviderUtil.getProvider("syndeo:schematransform:com.google.cloud:data_generator:v1");
    DataGeneratorSchemaTransform schemaTransform =
        (DataGeneratorSchemaTransform)
            provider.from(
                DataGeneratorSchemaTransformConfiguration.builder()
                    .setSchema(schema)
                    .setMinutesToRun(1l)
                    .setRecordsPerSecond(100l)
                    .build());
    PTransform<PCollectionRowTuple, PCollectionRowTuple> transform =
        schemaTransform.buildTransform();
    Pipeline p = Pipeline.create();
    PCollectionRowTuple input = PCollectionRowTuple.empty(p);
    transform.expand(input);
    assertNotNull(transform);
  }
}
