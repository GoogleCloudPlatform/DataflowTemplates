/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import java.util.Collections;
import org.apache.avro.Schema;
import org.junit.Test;

/** Tests {@link AvroSchemaToDdlConverter}. */
public class AvroSchemaToDdlConverterTest {

  @Test
  public void emptySchema() {
    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.emptyList());
    assertThat(ddl.allTables(), empty());
  }

  @Test
  public void simple() {
    String avroString =
        "{"
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
            + "  }, {"
            + "    \"name\" : \"full_name\","
            + "    \"type\" : \"null\","
            + "    \"sqlType\" : \"STRING(MAX)\","
            + "    \"notNull\" : \"false\","
            + "    \"generationExpression\" : \"CONCAT(first_name, ' ', last_name)\","
            + "    \"stored\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"numeric\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\"}],"
            + "    \"sqlType\" : \"NUMERIC\""
            + "  }, {"
            + "    \"name\" : \"numeric2\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":38,\"scale\":9}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"notNumeric\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":38}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"notNumeric2\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":38,\"scale\":10}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"numericArr\","
            + "    \"type\": [\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"bytes\","
            + "              \"logicalType\":\"decimal\",\"precision\":38,\"scale\":9}]}]"
            // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"notNumericArr\","
            + "    \"type\": [\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"bytes\","
            + "              \"logicalType\":\"decimal\",\"precision\":35}]}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"json\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"JSON\""
            + "  }, {"
            + "    \"name\" : \"notJson\","
            + "    \"type\" : [ \"null\", \"string\" ]" // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"jsonArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"string\"]}],"
            + "    \"sqlType\":\"ARRAY<JSON>\""
            + "  }, {"
            + "    \"name\":\"notJsonArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"string\"]}]"
            // Omitting sqlType
            + "  }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"`id` ASC\","
            + "  \"spannerPrimaryKey_1\" : \"`last_name` DESC\","
            + "  \"spannerIndex_0\" : "
            + "  \"CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)\","
            + "  \"spannerForeignKey_0\" : "
            + "  \"ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`) "
            + "  REFERENCES `AllowedNames` (`first_name`)\","
            + "  \"spannerCheckConstraint_0\" : "
            + "  \"CONSTRAINT `ck` CHECK(`first_name` != 'last_name')\""
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.views(), hasSize(0));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `Users` ("
                + " `id`              INT64 NOT NULL,"
                + " `first_name`      STRING(10),"
                + " `last_name`       STRING(MAX),"
                + " `full_name`       STRING(MAX) AS (CONCAT(first_name, ' ', last_name)) STORED,"
                + " `numeric`         NUMERIC,"
                + " `numeric2`        NUMERIC,"
                + " `notNumeric`      BYTES(MAX),"
                + " `notNumeric2`     BYTES(MAX),"
                + " `numericArr`      ARRAY<NUMERIC>,"
                + " `notNumericArr`   ARRAY<BYTES(MAX)>,"
                + " `json`            JSON,"
                + " `notJson`         STRING(MAX),"
                + " `jsonArr`         ARRAY<JSON>,"
                + " `notJsonArr`      ARRAY<STRING(MAX)>,"
                + " CONSTRAINT `ck` CHECK(`first_name` != 'last_name'),"
                + " ) PRIMARY KEY (`id` ASC, `last_name` DESC)"
                + " CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk`"
                + " FOREIGN KEY (`first_name`) REFERENCES `AllowedNames` (`first_name`)"));
  }

  @Test
  public void invokerRightsView() {
    String avroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Names\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerViewSecurity\" : \"INVOKER\","
            + "  \"spannerViewQuery\" : \"SELECT first_name, last_name FROM Users\""
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.views(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE VIEW `Names` SQL SECURITY INVOKER AS SELECT first_name, last_name FROM Users"));
  }

  @Test
  public void columnOptions() {
    String avroString =
        "{"
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
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"`id` ASC\""
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.allTables(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                             STRING(10) "
                + " OPTIONS (allow_commit_timestamp=TRUE,my_random_opt=\"1\"),"
                + " ) PRIMARY KEY (`id` ASC)"));
  }
}
