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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class ResourceUtilsTest {
  @Test
  public void testResourceAsString() {
    String query = ResourceUtils.resourceAsString("sql/mysql_collation_order_query.sql");
    assertThat(query).isNotEmpty();
    assertThrows(RuntimeException.class, () -> ResourceUtils.resourceAsString("no_such_file.sql"));
  }

  @Test
  public void testPrepareCollationsOrderQuery() {
    String originalQuery =
        "SET @db_charset = 'charset_replacement_tag';\n"
            + " -- You have a blank line below and a comment here.\n"
            + "SET @db_collation = 'collation_replacement_tag';\n\n"
            + "  \n"
            + "SELECT * FROM my_table1;\n"
            + "-- This is another comment\n"
            + " -- This is uet another comment!\n"
            + "SELECT * FROM my_table2;";
    String expectedQuery =
        "SET @db_charset = 'utf8mb4';\n"
            + "SET @db_collation = 'utf8mb4_general_ci';\n"
            + "SELECT * FROM my_table1;\n"
            + "SELECT * FROM my_table2;";

    Map<String, String> tags = new HashMap<>();
    tags.put(ResourceUtils.CHARSET_REPLACEMENT_TAG, "utf8mb4");
    tags.put(ResourceUtils.COLLATION_REPLACEMENT_TAG, "utf8mb4_general_ci");
    String processedQuery = ResourceUtils.replaceTagsAndSanitize(originalQuery, tags);

    assertThat(processedQuery).isEqualTo(expectedQuery);
  }
}
