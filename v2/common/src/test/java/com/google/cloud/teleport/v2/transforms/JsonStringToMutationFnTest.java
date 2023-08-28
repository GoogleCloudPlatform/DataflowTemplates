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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonParser;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for JsonStringToMutationFn. */
@RunWith(JUnit4.class)
public final class JsonStringToMutationFnTest {
  private static final String TEST_TABLE = "testTable";
  private static final String DATA = "{\"id\": 123, \"name\": \"Bob Marley\"}";

  @Test
  public void testParseRow() {
    SpannerSchema.Column idColumn = mock(SpannerSchema.Column.class);
    SpannerSchema.Column nameColumn = mock(SpannerSchema.Column.class);
    when(idColumn.getName()).thenReturn("id");
    when(idColumn.getType()).thenReturn(Type.int64());
    when(nameColumn.getName()).thenReturn("name");
    when(nameColumn.getType()).thenReturn(Type.string());

    JsonStringToMutationFn jsonStringToMutationFn = new JsonStringToMutationFn(TEST_TABLE, null);
    Mutation actualMutation =
        jsonStringToMutationFn.parseRow(
            Mutation.newInsertOrUpdateBuilder(TEST_TABLE),
            JsonParser.parseString(DATA).getAsJsonObject(),
            List.of(idColumn, nameColumn));

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
