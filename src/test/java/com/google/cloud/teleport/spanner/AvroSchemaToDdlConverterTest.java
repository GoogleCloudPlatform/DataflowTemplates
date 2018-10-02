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

package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.text.IsEqualIgnoringWhiteSpace.equalToIgnoringWhiteSpace;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import java.util.Collections;
import org.apache.avro.Schema;
import org.junit.Test;

/**
 * Tests {@link AvroSchemaToDdlConverter}.
 */
public class AvroSchemaToDdlConverterTest {

  @Test
  public void emptySchema() {
    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.emptyList());
    assertThat(ddl.allTables(), empty());
  }

  @Test
  public void simple() {
    String avroString = "{"
        + "  \"type\" : \"record\","
        + "  \"name\" : \"Users\","
        + "  \"namespace\" : \"spannertest\","
        + "  \"fields\" : [ {"
        + "    \"name\" : \"id\","
        + "    \"type\" : \"long\","
        + "    \"sqlType\" : \"INT64\""
        + "  }, {"
        + "    \"name\" : \"first_name\","
        + "    \"type\" : [ \"null\", \"string\" ],"
        + "    \"sqlType\" : \"STRING(10)\""
        + "  }, {"
        + "    \"name\" : \"last_name\","
        + "    \"type\" : [ \"null\", \"string\" ],"
        + "    \"sqlType\" : \"STRING(MAX)\""
        + "  } ],"
        + "  \"googleStorage\" : \"CloudSpanner\"," + "  \"spannerParent\" : \"\","
        + "  \"googleFormatVersion\" : \"booleans\","
        + "  \"spannerPrimaryKey_0\" : \"`id` ASC\","
        + "  \"spannerPrimaryKey_1\" : \"`last_name` DESC\""
        + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.allTables(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToIgnoringWhiteSpace(
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                             STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " ) PRIMARY KEY (`id` ASC, `last_name` DESC)"));
  }

  @Test
  public void columnOptions() {
    String avroString = "{"
        + "  \"type\" : \"record\","
        + "  \"name\" : \"Users\","
        + "  \"namespace\" : \"spannertest\","
        + "  \"fields\" : [ {"
        + "    \"name\" : \"id\","
        + "    \"type\" : \"long\","
        + "    \"sqlType\" : \"INT64\""
        + "  }, {"
        + "    \"name\" : \"first_name\","
        + "    \"type\" : [ \"null\", \"string\" ],"
        + "    \"sqlType\" : \"STRING(10)\","
        + "    \"spannerOption_0\" : \"allow_commit_timestamp=TRUE\","
        + "    \"spannerOption_1\" : \"my_random_opt=\\\"1\\\"\""
        + "  }],"
        + "  \"googleStorage\" : \"CloudSpanner\"," + "  \"spannerParent\" : \"\","
        + "  \"googleFormatVersion\" : \"booleans\","
        + "  \"spannerPrimaryKey_0\" : \"`id` ASC\""
        + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.allTables(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToIgnoringWhiteSpace(
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                             STRING(10) "
                + " OPTIONS (allow_commit_timestamp=TRUE,my_random_opt=\"1\"),"
                + " ) PRIMARY KEY (`id` ASC)"));
  }
}
