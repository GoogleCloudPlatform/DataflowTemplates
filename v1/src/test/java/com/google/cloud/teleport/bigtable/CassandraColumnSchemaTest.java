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
package com.google.cloud.teleport.bigtable;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CassandraColumnSchemaTest {

  @Test
  public void processSchemaString() {
    String schemaString =
        "\n"
            + " [json]\n"
            + "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
            + "                            {\"keyspace_name\": \"cycling\", \"table_name\": \"race_winners\", \"column_name\": \"name\", \"clustering_order\": \"none\", \"column_name_bytes\": \"0x6e616d65\", \"kind\": \"regular\", \"position\": -1, \"type\": \"text\"}\n"
            + " {\"keyspace_name\": \"cycling\", \"table_name\": \"race_winners\", \"column_name\": \"race_location\", \"clustering_order\": \"none\", \"column_name_bytes\": \"0x726163655f6c6f636174696f6e\", \"kind\": \"regular\", \"position\": -1, \"type\": \"text\"}\n"
            + "        {\"keyspace_name\": \"cycling\", \"table_name\": \"race_winners\", \"column_name\": \"race_name\", \"clustering_order\": \"none\", \"column_name_bytes\": \"0x726163655f6e616d65\", \"kind\": \"partition_key\", \"position\": 0, \"type\": \"text\"}\n"
            + " {\"keyspace_name\": \"cycling\", \"table_name\": \"race_winners\", \"column_name\": \"race_position\", \"clustering_order\": \"asc\", \"column_name_bytes\": \"0x726163655f706f736974696f6e\", \"kind\": \"clustering\", \"position\": 0, \"type\": \"int\"}\n"
            + "\n"
            + "(4 rows)";

    CassandraColumnSchema schema = new CassandraColumnSchema(schemaString);

    String expectedWritetimeQuery =
        "SELECT race_name,race_position,name,race_location,writetime(name),writetime(race_location) FROM cycling.race_winners";
    assertEquals(expectedWritetimeQuery, schema.createWritetimeQuery());
  }
}
