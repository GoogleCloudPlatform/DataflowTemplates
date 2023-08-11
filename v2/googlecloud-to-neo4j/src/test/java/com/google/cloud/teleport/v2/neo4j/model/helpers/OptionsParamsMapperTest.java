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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.options.Neo4jFlexTemplateOptions;
import java.util.Map;
import org.junit.Test;

public class OptionsParamsMapperTest {

  @Test
  public void populatesOverlayTokensFromJsonOptions() {
    Neo4jFlexTemplateOptions templateOptions = mock(Neo4jFlexTemplateOptions.class);
    when(templateOptions.getOptionsJson()).thenReturn("{\"foo\": \"bar\"}");

    OptionsParams options = OptionsParamsMapper.fromPipelineOptions(templateOptions);

    assertThat(options.getTokenMap()).isEqualTo(Map.of("foo", "bar"));
  }
}
