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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.values.SpannerSchema;
import com.google.gson.JsonParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for JsonStringToMutationFn. */
@RunWith(JUnit4.class)
public final class JsonStringToMutationFnTest {
  private static final String TEST_TABLE = "testTable";
  private static final String SCHEMA = "id:INT64,name:STRING";
  private static final SpannerSchema SPANNER_SCHEMA = new SpannerSchema(SCHEMA);
  private static final String DATA = "{\"id\": 123, \"name\": \"Bob Marley\"}";

  @Test
  public void testParseRow() {
    JsonStringToMutationFn jsonStringToMutationFn =
        new JsonStringToMutationFn("", "", "", TEST_TABLE);

    Mutation actualMutation =
        jsonStringToMutationFn.parseRow(
            Mutation.newInsertOrUpdateBuilder(TEST_TABLE),
            JsonParser.parseString(DATA).getAsJsonObject(),
            SPANNER_SCHEMA,
            SPANNER_SCHEMA.getColumnList());

    Mutation expectedMutation =
        Mutation.newInsertOrUpdateBuilder(TEST_TABLE)
            .set("id")
            .to(123)
            .set("name")
            .to("Bob Marley")
            .build();
    assertThat(actualMutation).isEqualTo(expectedMutation);
  }
}
