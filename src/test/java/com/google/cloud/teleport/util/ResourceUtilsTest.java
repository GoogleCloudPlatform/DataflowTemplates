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

package com.google.cloud.teleport.util;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link ResourceUtils}. */
@RunWith(JUnit4.class)
public final class ResourceUtilsTest {
  /**
   * Tests {@link ResourceUtils#getDeadletterTableSchemaJson()} successfully retrieves the schema
   * JSON file from the resources directory.
   */
  @Test
  public void testGetDeadletterTableSchemaJson() {
    // Retrieve the schema JSON.
    String schemaJSON = ResourceUtils.getDeadletterTableSchemaJson();

    // Assert that the schema was retrieved.
    assertThat(schemaJSON, is(notNullValue()));

    // Assert that the schema is valid JSON.
    Gson gson = new Gson();
    Type type = new TypeToken<Map<String, Object>>() {}.getType();
    Map<String, Object> json = gson.fromJson(schemaJSON, type);

    assertThat(json.get("fields"), is(notNullValue()));
    assertThat(json.get("fields"), instanceOf(List.class));
  }
}
