/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;

/** Test cases for the {@link TextIOToBigQuery} class. */
public class TextIOToBigQueryTest {

  @Test
  public void testTextIOToBigQueryGetFieldsMethod() throws Exception {
    // Test input
    final String payload = "{\"a\": [{\"name\":\"dimension\",\"type\":\"RECORD\",\"fields\":[{\"name\":\"impression\",\"type\":\"STRING\"},{\"name\":\"domain\",\"type\":\"STRING\"},{\"name\":\"ip\",\"type\":\"STRING\"},{\"name\":\"xyz_section\",\"type\":\"STRING\"},{\"name\":\"creative\",\"type\":\"RECORD\",\"fields\":[{\"name\":\"format\",\"type\":\"STRING\"},{\"name\":\"sdk\",\"type\":\"STRING\"},{\"name\":\"template\",\"type\":\"STRING\"},{\"name\":\"size\",\"type\":\"STRING\"}]}]}]}";

    JSONObject schema = new JSONObject(payload);

    List <TableFieldSchema> res = TextIOToBigQuery.getFields(schema.getJSONArray("a"));

    // Assert
    assertThat(res.get(0).getName(), is(equalTo("dimension")));
    assertThat(res.get(0).getType(), is(equalTo("RECORD")));
    assertThat(res.get(0).getFields().get(0).getName(), is(equalTo("impression")));
    assertThat(res.get(0).getFields().get(0).getType(), is(equalTo("STRING")));

  }

}
