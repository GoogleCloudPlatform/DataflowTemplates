/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static com.google.cloud.teleport.v2.spanner.migrations.utils.SessionFileReaderTest.getSchemaObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/** Tests for Schema class. */
public class SchemaTest {

  @Test
  public void getSpannerColumnNamesBasic() {
    Schema schema = getSchemaObject();
    schema.generateMappings();
    List<String> actualColNames = schema.getSpannerColumnNames("new_cart");
    List<String> expectedColNames = Arrays.asList("new_product_id", "new_quantity", "new_user_id");
    assertThat(actualColNames, is(expectedColNames));
  }

  @Test
  public void getPrimaryKeySetBasic() {
    Schema schema = getSchemaObject();
    schema.generateMappings();
    Set<String> actualKeys = schema.getPrimaryKeySet("new_cart");
    Set<String> expectedKeys = new HashSet<>(Arrays.asList("new_user_id", "new_product_id"));
    assertThat(actualKeys, is(expectedKeys));
  }
}
