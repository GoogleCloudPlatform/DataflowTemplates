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

import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.sources.InlineTextSource;
import org.neo4j.importer.v1.sources.Source;

public class SourceMapperTest {

  @Test
  public void trimsSourceFields() {
    JSONObject json = new JSONObject();
    json.put("name", "placeholder");
    json.put("type", "text");
    json.put("ordered_field_names", "foo, bar,   qix\t\r");
    json.put("data", "foovalue,barvalue,qixvalue");

    Source source = SourceMapper.fromJson(json);

    assertThat(source).isInstanceOf(InlineTextSource.class);
    InlineTextSource inlineTextSource = (InlineTextSource) source;
    assertThat(inlineTextSource.getHeader()).isEqualTo(new String[] {"foo", "bar", "qix"});
  }
}
