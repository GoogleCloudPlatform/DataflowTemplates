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

import com.google.cloud.teleport.v2.neo4j.model.job.Source;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

public class SourceMapperTest {

  @Test
  public void trimsSourceFields() {
    JSONObject json = new JSONObject();
    json.put("name", "placeholder");
    json.put("ordered_field_names", "foo, bar,   qix\t\r");

    Source source = SourceMapper.fromJson(json);

    assertThat(source.getFieldNames()).isEqualTo(new String[] {"foo", "bar", "qix"});
    assertThat(source.getFieldPosByName()).isEqualTo(Map.of("foo", 1, "bar", 2, "qix", 3));
  }
}
