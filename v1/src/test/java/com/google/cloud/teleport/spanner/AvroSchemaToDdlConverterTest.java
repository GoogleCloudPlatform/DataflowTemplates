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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import java.util.ArrayList;
import java.util.Collection;
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
  public void pgEmptySchema() {
    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(Collections.emptyList());
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
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
            + "    \"name\" : \"gen_id\","
            + "    \"type\" : \"long\","
            + "    \"sqlType\" : \"INT64\","
            + "    \"notNull\" : \"true\","
            + "    \"generationExpression\" : \"MOD(id+1, 64)\","
            + "    \"stored\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"first_name\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"STRING(10)\","
            + "    \"defaultExpression\" : \"'John'\""
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
            + "  }, {"
            // Omitting sqlType
            + "    \"name\" : \"proto\","
            + "    \"type\" : [ \"null\", \"bytes\" ],"
            + "    \"sqlType\" : "
            + "\"PROTO<com.google.cloud.teleport.spanner.tests.TestMessage>\""
            + "  }, {"
            + "    \"name\" : \"notProto\","
            + "    \"type\" : [ \"null\", \"bytes\" ]" // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"protoArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"bytes\"]}],"
            + "    \"sqlType\":"
            + "\"ARRAY<PROTO<com.google.cloud.teleport.spanner.tests.TestMessage>>\""
            + "  }, {"
            + "    \"name\":\"notProtoArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"bytes\"]}]"
            + "  }, {"
            // Omitting sqlType
            + "    \"name\" : \"enum\","
            + "    \"type\" : [ \"null\", \"long\" ],"
            + "    \"sqlType\" : \"ENUM<com.google.cloud.teleport.spanner.tests.TestEnum>\""
            + "  }, {"
            + "    \"name\" : \"notEnum\","
            + "    \"type\" : [ \"null\", \"long\" ]" // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"enumArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"long\"]}],"
            + "    \"sqlType\":"
            + "\"ARRAY<ENUM<com.google.cloud.teleport.spanner.tests.TestEnum>>\""
            + "  }, {"
            + "    \"name\":\"notEnumArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"long\"]}]"
            + "  }, {"
            // Omitting sqlType
            + "    \"name\" : \"boolean\","
            + "    \"type\" : [ \"null\", \"boolean\" ]"
            + "  }, {"
            + "    \"name\" : \"integer\","
            + "    \"type\" : [ \"null\", \"long\" ]"
            + "  }, {"
            + "    \"name\" : \"float32\","
            + "    \"type\" : [ \"null\", \"float\" ]"
            + "  }, {"
            + "    \"name\" : \"float64\","
            + "    \"type\" : [ \"null\", \"double\" ]"
            + "  }, {"
            + "    \"name\" : \"timestamp\","
            + "    \"type\" : [ \"null\", {\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]"
            + "  }, {"
            + "    \"name\" : \"HiddenColumn\","
            + "    \"type\" : [ \"null\", \"long\" ],"
            + "    \"sqlType\":\"INT64\","
            + "    \"hidden\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"MyTokens\","
            + "    \"type\" : \"null\","
            + "    \"default\" : null,"
            + "    \"sqlType\" : \"TOKENLIST\","
            + "    \"hidden\" : \"true\","
            + "    \"notNull\" : \"false\","
            + "    \"generationExpression\" : \"(TOKENIZE_FULLTEXT(MyData))\","
            + "    \"stored\" : \"false\""
            + "  }, {"
            + "    \"name\" : \"Embeddings\","
            + "    \"type\" : [ \"null\", {"
            + "    \"type\" : \"array\", \"items\" : [ \"null\", \"float\"]}],"
            + "    \"sqlType\" : \"ARRAY<FLOAT32>(vector_length=>128)\""
            + " }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"`id` ASC\","
            + "  \"spannerPrimaryKey_1\" : \"`gen_id` ASC\","
            + "  \"spannerPrimaryKey_2\" : \"`last_name` DESC\","
            + "  \"spannerIndex_0\" : "
            + "  \"CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)\","
            + "  \"spannerIndex_1\" : "
            + "  \"CREATE VECTOR INDEX `VI` ON `Users` (`Embeddings`) WHERE Embeddings IS NOT NULL OPTIONS (distance_type=\'COSINE\')\","
            + "  \"spannerForeignKey_0\" : "
            + "  \"ALTER TABLE `Users` ADD CONSTRAINT `fk` FOREIGN KEY (`first_name`) "
            + "  REFERENCES `AllowedNames` (`first_name`)\","
            + "  \"spannerForeignKey_1\" : "
            + "  \"ALTER TABLE `Users` ADD CONSTRAINT `fk_odc` FOREIGN KEY (`last_name`) "
            + "  REFERENCES `AllowedNames` (`last_name`) ON DELETE CASCADE\","
            + "  \"spannerForeignKey_2\" : "
            + "  \"ALTER TABLE `Users` ADD CONSTRAINT `fk_not_enforced_no_action` FOREIGN KEY (`last_name`) "
            + "  REFERENCES `AllowedNames` (`last_name`) ON DELETE NO ACTION NOT ENFORCED\","
            + "  \"spannerForeignKey_3\" : "
            + "  \"ALTER TABLE `Users` ADD CONSTRAINT `fk_enforced` FOREIGN KEY (`last_name`) "
            + "  REFERENCES `AllowedNames` (`last_name`) ENFORCED\","
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
                + " `gen_id`          INT64 NOT NULL AS (MOD(id+1, 64)) STORED,"
                + " `first_name`      STRING(10) DEFAULT ('John'),"
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
                + " `proto`           `com.google.cloud.teleport.spanner.tests.TestMessage`,"
                + " `notProto`        BYTES(MAX),"
                + " `protoArr`        ARRAY<`com.google.cloud.teleport.spanner.tests.TestMessage`>,"
                + " `notProtoArr`     ARRAY<BYTES(MAX)>,"
                + " `enum`            `com.google.cloud.teleport.spanner.tests.TestEnum`,"
                + " `notEnum`         INT64,"
                + " `enumArr`         ARRAY<`com.google.cloud.teleport.spanner.tests.TestEnum`>,"
                + " `notEnumArr`      ARRAY<INT64>,"
                + " `boolean`         BOOL,"
                + " `integer`         INT64,"
                + " `float32`         FLOAT32,"
                + " `float64`         FLOAT64,"
                + " `timestamp`       TIMESTAMP,"
                + " `HiddenColumn`    INT64 HIDDEN,"
                + " `MyTokens`                              TOKENLIST AS ((TOKENIZE_FULLTEXT(MyData))) HIDDEN,"
                + " `Embeddings`                            ARRAY<FLOAT32>(vector_length=>128),"
                + " CONSTRAINT `ck` CHECK(`first_name` != 'last_name'),"
                + " ) PRIMARY KEY (`id` ASC, `gen_id` ASC, `last_name` DESC)"
                + " CREATE INDEX `UsersByFirstName` ON `Users` (`first_name`)"
                + " CREATE VECTOR INDEX `VI` ON `Users` (`Embeddings`) WHERE Embeddings IS NOT NULL"
                + " OPTIONS (distance_type=\'COSINE\')"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk`"
                + " FOREIGN KEY (`first_name`) REFERENCES `AllowedNames` (`first_name`)"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk_odc`"
                + " FOREIGN KEY (`last_name`) REFERENCES "
                + "`AllowedNames` (`last_name`) ON DELETE CASCADE"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk_not_enforced_no_action`"
                + " FOREIGN KEY (`last_name`) REFERENCES "
                + "`AllowedNames` (`last_name`) ON DELETE NO ACTION NOT ENFORCED"
                + " ALTER TABLE `Users` ADD CONSTRAINT `fk_enforced`"
                + " FOREIGN KEY (`last_name`) REFERENCES "
                + "`AllowedNames` (`last_name`) ENFORCED"));
  }

  @Test
  public void pgSimple() {
    String avroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Users\","
            + "  \"namespace\" : \"spannertest\","
            + "  \"fields\" : [ {"
            + "    \"name\" : \"id\","
            + "    \"type\" : \"long\","
            + "    \"sqlType\" : \"bigint\""
            + "  }, {"
            + "    \"name\" : \"gen_id\","
            + "    \"type\" : \"long\","
            + "    \"sqlType\" : \"bigint\","
            + "    \"notNull\" : \"false\","
            + "    \"generationExpression\" : \"MOD(id+1, 64)\","
            + "    \"stored\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"first_name\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"character varying(10)\","
            + "    \"defaultExpression\" : \"'John'\""
            + "  }, {"
            + "    \"name\" : \"last_name\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"character varying\""
            + "  }, {"
            + "    \"name\" : \"full_name\","
            + "    \"type\" : \"null\","
            + "    \"sqlType\" : \"character varying\","
            + "    \"notNull\" : \"false\","
            + "    \"generationExpression\" : \"CONCAT(first_name, ' ', last_name)\","
            + "    \"stored\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"numeric\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\"}],"
            + "    \"sqlType\" : \"numeric\""
            + "  }, {"
            + "    \"name\" : \"numeric2\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":147455,\"scale\":16383}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"notNumeric\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":147455}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"notNumeric2\","
            + "    \"type\" : [\"null\", {\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "                \"precision\":147455,\"scale\":16384}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"numericArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"bytes\","
            + "    \"logicalType\":\"decimal\",\"precision\":147455,\"scale\":16383}]}]"
            // Omitting sqlType
            + "  }, {"
            + "    \"name\":\"notNumericArr\","
            + "    \"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"bytes\","
            + "    \"logicalType\":\"decimal\",\"precision\":147455}]}]" // Omitting sqlType
            + "  }, {"
            + "    \"name\" : \"bool\","
            + "    \"type\" : [ \"null\", \"boolean\" ],"
            + "    \"sqlType\" : \"boolean\""
            + "  }, {"
            + "    \"name\" : \"float32\","
            + "    \"type\" : [ \"null\", \"float\" ],"
            + "    \"sqlType\" : \"real\""
            + "  }, {"
            + "    \"name\" : \"float64\","
            + "    \"type\" : [ \"null\", \"double\" ],"
            + "    \"sqlType\" : \"double precision\""
            + "  }, {"
            + "    \"name\" : \"bytes\","
            + "    \"type\" : [ \"null\", \"bytes\" ],"
            + "    \"sqlType\" : \"bytea\""
            + "  }, {"
            + "    \"name\" : \"text\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"text\""
            + "  }, {"
            + "    \"name\" : \"timestamptz\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"timestamp with time zone\""
            + "  }, {"
            + "    \"name\" : \"commit_time\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"spanner.commit_timestamp\""
            + "  }, {"
            + "    \"name\" : \"date\","
            + "    \"type\" : [ \"null\", \"string\" ],"
            + "    \"sqlType\" : \"date\""
            + "  }, {"
            + "    \"name\" : \"varcharArr1\","
            + "    \"type\" : [\"null\","
            + "               {\"type\":\"array\",\"items\":[\"null\",{\"type\":\"string\"}]}],"
            + "    \"sqlType\" : \"character varying[]\""
            + "  }, {"
            + "    \"name\" : \"varcharArr2\","
            + "    \"type\" : [\"null\","
            + "               {\"type\":\"array\",\"items\":[\"null\",{\"type\":\"string\"}]}]"
            + "  }, {"
            // Omitting sqlType
            + "    \"name\" : \"boolean\","
            + "    \"type\" : [ \"null\", \"boolean\" ]"
            + "  }, {"
            + "    \"name\" : \"integer1\","
            + "    \"type\" : [ \"null\", \"long\" ]"
            + "  }, {"
            + "    \"name\" : \"float321\","
            + "    \"type\" : [ \"null\", \"float\" ]"
            + "  }, {"
            + "    \"name\" : \"float641\","
            + "    \"type\" : [ \"null\", \"double\" ]"
            + "  }, {"
            + "    \"name\" : \"timestamp1\","
            + "    \"type\" : [ \"null\", {\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]"
            + "  } ],  \"googleStorage\" : \"CloudSpanner\",  \"spannerParent\" : \"\", "
            + " \"googleFormatVersion\" : \"booleans\",  \"spannerPrimaryKey_0\" : \"\\\"id\\\""
            + " ASC\",  \"spannerPrimaryKey_1\" : \"\\\"gen_id\\\" ASC\",  \"spannerPrimaryKey_2\""
            + " : \"\\\"last_name\\\" ASC\",  \"spannerIndex_0\" :"
            + "   \"CREATE INDEX \\\"UsersByFirstName\\\" ON \\\"Users\\\" (\\\"first_name\\\")\", "
            + " \"spannerForeignKey_0\" :   \"ALTER TABLE \\\"Users\\\" ADD CONSTRAINT \\\"fk\\\""
            + " FOREIGN KEY (\\\"first_name\\\")   REFERENCES \\\"AllowedNames\\\""
            + " (\\\"first_name\\\")\", "
            + " \"spannerForeignKey_1\" :   \"ALTER TABLE \\\"Users\\\" ADD CONSTRAINT "
            + "\\\"fk_odc\\\" FOREIGN KEY (\\\"last_name\\\")   REFERENCES \\\"AllowedNames\\\""
            + " (\\\"last_name\\\") ON DELETE CASCADE\", "
            + " \"spannerCheckConstraint_0\" :   \"CONSTRAINT \\\"ck\\\""
            + " CHECK(\\\"first_name\\\" != \\\"last_name\\\")\"}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.views(), hasSize(0));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE \"Users\" ("
                + " \"id\"              bigint NOT NULL,"
                + " \"gen_id\"          bigint GENERATED ALWAYS AS (MOD(id+1, 64)) STORED,"
                + " \"first_name\"      character varying(10) DEFAULT 'John',"
                + " \"last_name\"       character varying,"
                + " \"full_name\"       character varying GENERATED ALWAYS AS"
                + " (CONCAT(first_name, ' ', last_name)) STORED,"
                + " \"numeric\"         numeric,"
                + " \"numeric2\"        numeric,"
                + " \"notNumeric\"      bytea,"
                + " \"notNumeric2\"     bytea,"
                + " \"numericArr\"         numeric[],"
                + " \"notNumericArr\"      bytea[],"
                + " \"bool\" boolean,"
                + " \"float32\" real,"
                + " \"float64\" double precision,"
                + " \"bytes\" bytea,"
                + " \"text\" text,"
                + " \"timestamptz\" timestamp with time zone,"
                + " \"commit_time\"     spanner.commit_timestamp,"
                + " \"date\" date,"
                + " \"varcharArr1\"     character varying[],"
                + " \"varcharArr2\"     character varying[],"
                + " \"boolean\"         boolean,"
                + " \"integer1\"        bigint,"
                + " \"float321\"        real,"
                + " \"float641\"        double precision,"
                + " \"timestamp1\"      timestamp with time zone,"
                + " CONSTRAINT \"ck\" CHECK(\"first_name\" != \"last_name\"),"
                + " PRIMARY KEY (\"id\", \"gen_id\", \"last_name\")"
                + " )"
                + " CREATE INDEX \"UsersByFirstName\" ON \"Users\" (\"first_name\")"
                + " ALTER TABLE \"Users\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"first_name\")"
                + " REFERENCES \"AllowedNames\" (\"first_name\")"
                + " ALTER TABLE \"Users\" ADD CONSTRAINT \"fk_odc\" FOREIGN KEY (\"last_name\")"
                + " REFERENCES \"AllowedNames\" (\"last_name\") ON DELETE CASCADE"));
  }

  @Test
  public void propertyGraphs() {
    String avroString =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"testGraph\",\n"
            + "  \"namespace\": \"spannertest\",\n"
            + "  \"fields\": [],\n"
            + "  \"spannerGraphNodeTable_0_NAME\": \"nodeAlias\",\n"
            + "  \"spannerGraphNodeTable_0_BASE_TABLE_NAME\": \"baseTable\",\n"
            + "  \"spannerName\": \"testGraph\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_0_NAME\": \"dummyLabelName1\",\n"
            + "  \"spannerEntity\": \"PropertyGraph\",\n"
            + "  \"spannerGraphLabel_1_NAME\": \"dummyLabelName2\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_0_PROPERTY_0_NAME\": \"dummyPropName\",\n"
            + "  \"spannerGraphEdgeTable_0_KEY_COLUMNS\": \"edgePrimaryKey\",\n"
            + "  \"spannerGraphEdgeTable_0_TARGET_NODE_KEY_COLUMNS\": \"otherNodeKey\",\n"
            + "  \"googleStorage\": \"CloudSpanner\",\n"
            + "  \"spannerGraphEdgeTable_0_SOURCE_NODE_KEY_COLUMNS\": \"nodeKey\",\n"
            + "  \"spannerGraphNodeTable_0_KIND\": \"NODE\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_1_NAME\": \"dummyLabelName2\",\n"
            + "  \"spannerGraphEdgeTable_0_NAME\": \"edgeAlias\",\n"
            + "  \"spannerGraphEdgeTable_0_BASE_TABLE_NAME\": \"edgeBaseTable\",\n"
            + "  \"spannerGraphEdgeTable_0_KIND\": \"EDGE\",\n"
            + "  \"spannerGraphLabel_0_PROPERTY_1\": \"aliasedPropName\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_0_PROPERTY_1_NAME\": \"aliasedPropName\",\n"
            + "  \"spannerGraphEdgeTable_0_LABEL_0_NAME\": \"dummyLabelName3\",\n"
            + "  \"spannerGraphNodeTable_0_KEY_COLUMNS\": \"primaryKey\",\n"
            + "  \"spannerGraphLabel_2_NAME\": \"dummyLabelName3\",\n"
            + "  \"spannerGraphLabel_0_PROPERTY_0\": \"dummyPropName\",\n"
            + "  \"spannerGraphEdgeTable_0_SOURCE_EDGE_KEY_COLUMNS\": \"sourceEdgeKey\",\n"
            + "  \"spannerGraphEdgeTable_0_TARGET_EDGE_KEY_COLUMNS\": \"destEdgeKey\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_0_PROPERTY_0_VALUE\": \"dummyPropName\",\n"
            + "  \"spannerGraphEdgeTable_0_TARGET_NODE_TABLE_NAME\": \"baseTable\",\n"
            + "  \"spannerGraphEdgeTable_0_SOURCE_NODE_TABLE_NAME\": \"baseTable\",\n"
            + "  \"spannerGraphNodeTable_0_LABEL_0_PROPERTY_1_VALUE\": \"CONCAT(CAST(test_col AS STRING), \\\":\\\", \\\"dummyColumn\\\")\",\n"
            + "  \"spannerGraphLabel_0_NAME\": \"dummyLabelName1\",\n"
            + "  \"googleFormatVersion\": \"booleans\",\n"
            + "  \"spannerGraphPropertyDeclaration_1_NAME\": \"aliasedPropName\",\n"
            + "  \"spannerGraphPropertyDeclaration_1_TYPE\": \"dummyPropType\",\n"
            + "  \"spannerGraphPropertyDeclaration_0_NAME\": \"aliasedPropName\",\n"
            + "  \"spannerGraphPropertyDeclaration_0_TYPE\": \"dummyPropType\"\n"
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.propertyGraphs(), hasSize(1));

    String expectedPg =
        "CREATE PROPERTY GRAPH testGraph\n"
            + "NODE TABLES(\n"
            + "baseTable AS nodeAlias\n"
            + " KEY (primaryKey)\n"
            + "LABEL dummyLabelName1 PROPERTIES(dummyPropName, CONCAT(CAST(test_col AS STRING), \":\", \"dummyColumn\") AS aliasedPropName)\n"
            + "LABEL dummyLabelName2 NO PROPERTIES)\n"
            + "EDGE TABLES(\n"
            + "edgeBaseTable AS edgeAlias\n"
            + " KEY (edgePrimaryKey)\n"
            + "SOURCE KEY(sourceEdgeKey) REFERENCES baseTable DESTINATION KEY(destEdgeKey) REFERENCES baseTable\n"
            + "LABEL dummyLabelName3 NO PROPERTIES)";

    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(expectedPg));
  }

  @Test
  public void models() {
    String modelAllString =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"ModelAll\","
            + "\"namespace\":\"spannertest\","
            + "\"googleFormatVersion\":\"booleans\","
            + "\"googleStorage\":\"CloudSpanner\", "
            + "\"spannerEntity\":\"Model\", "
            + "\"spannerRemote\":\"true\", "
            + "\"spannerOption_0\":\"endpoint=\\\"test\\\"\", "
            + "\"fields\":["
            + "  {\"name\":\"Input\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelAll_Input\","
            + "      \"fields\":["
            + "        {\"name\":\"i1\","
            + "         \"type\":\"boolean\","
            + "         \"sqlType\":\"BOOL\","
            + "         \"spannerOption_0\":\"required=FALSE\"},"
            + "        {\"name\":\"i2\","
            + "         \"type\":\"string\","
            + "         \"sqlType\":\"STRING(MAX)\"}]}},"
            + "  {\"name\":\"Output\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelAll_Output\","
            + "      \"fields\":["
            + "        {\"name\":\"o1\","
            + "         \"type\":\"long\","
            + "         \"sqlType\":\"INT64\","
            + "         \"spannerOption_0\":\"required=TRUE\"},"
            + "        {\"name\":\"o2\", "
            + "         \"type\":\"double\", "
            + "         \"sqlType\":\"FLOAT64\"}]}}] "
            + "}";
    String modelMinString =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"ModelMin\","
            + "\"namespace\":\"spannertest\","
            + "\"googleFormatVersion\":\"booleans\","
            + "\"googleStorage\":\"CloudSpanner\", "
            + "\"spannerEntity\":\"Model\", "
            + "\"spannerRemote\":\"false\", "
            + "\"fields\":["
            + "  {\"name\":\"Input\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelMin_Input\","
            + "      \"fields\":[{\"name\":\"i1\", \"type\":\"boolean\", \"sqlType\":\"BOOL\"}]}},"
            + "  {\"name\":\"Output\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelMin_Output\","
            + "      \"fields\":["
            + "        {\"name\":\"o1\", \"type\":\"long\", \"sqlType\":\"INT64\"}]}}] "
            + "}";
    String modelStructString =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"ModelStruct\","
            + "\"namespace\":\"spannertest\","
            + "\"googleFormatVersion\":\"booleans\","
            + "\"googleStorage\":\"CloudSpanner\", "
            + "\"spannerEntity\":\"Model\", "
            + "\"spannerRemote\":\"false\", "
            + "\"fields\":["
            + "  {\"name\":\"Input\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelStruct_Input\","
            + "      \"fields\":["
            + "        {\"name\":\"i1\","
            + "         \"sqlType\":\"STRUCT<a BOOL>\","
            + "         \"type\": "
            + "           {\"type\":\"record\","
            + "            \"name\":\"ModelStruct_struct_input_0\","
            + "            \"fields\":[{\"name\":\"a\",\"type\":\"boolean\"}]}}]}},"
            + "  {\"name\":\"Output\","
            + "   \"type\":"
            + "     {\"type\":\"record\","
            + "      \"name\":\"ModelStruct_Output\","
            + "      \"fields\":["
            + "        {\"name\":\"o1\","
            + "         \"sqlType\":\"STRUCT<a BOOL, b ARRAY<STRUCT<c STRING(MAX), d ARRAY<FLOAT64>>>, e STRUCT<f STRUCT<g INT64>>>\","
            + "         \"type\": "
            + "           {\"type\":\"record\","
            + "            \"name\":\"ModelStruct_struct_output_0\","
            + "            \"fields\":[{\"name\":\"a\",\"type\":\"boolean\"}]}}]}}]"
            + "}";

    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(modelAllString));
    schemas.add(parser.parse(modelMinString));
    schemas.add(parser.parse(modelStructString));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.models(), hasSize(3));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE MODEL `ModelAll`"
                + " INPUT ("
                + "  `i1` BOOL OPTIONS (required=FALSE),"
                + "  `i2` STRING(MAX),"
                + " )"
                + " OUTPUT ("
                + " `o1` INT64 OPTIONS (required=TRUE),"
                + " `o2` FLOAT64,"
                + " )"
                + " REMOTE OPTIONS (endpoint=\"test\")"
                + " CREATE MODEL `ModelMin`"
                + " INPUT ( `i1` BOOL, )"
                + " OUTPUT ( `o1` INT64, )"
                + " CREATE MODEL `ModelStruct`"
                + " INPUT ( `i1` STRUCT<a BOOL>, )"
                + " OUTPUT ( `o1` STRUCT<a BOOL, b ARRAY<STRUCT<c STRING(MAX), d ARRAY<FLOAT64>>>, e STRUCT<f STRUCT<g INT64>>>, )"));
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
  public void pgInvokerRightsView() {
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

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
    assertThat(ddl.views(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE VIEW \"Names\" SQL SECURITY INVOKER AS SELECT first_name, last_name FROM"
                + " Users"));
  }

  @Test
  public void definerRightsView() {
    String avroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Names\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerViewSecurity\" : \"DEFINER\","
            + "  \"spannerViewQuery\" : \"SELECT first_name, last_name FROM Users\""
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertThat(ddl.views(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE VIEW `Names` SQL SECURITY DEFINER AS SELECT first_name, last_name FROM Users"));
  }

  @Test
  public void pgDefinerRightsView() {
    String avroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Names\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerViewSecurity\" : \"DEFINER\","
            + "  \"spannerViewQuery\" : \"SELECT first_name, last_name FROM Users\""
            + "}";

    Schema schema = new Schema.Parser().parse(avroString);

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(Collections.singleton(schema));
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
    assertThat(ddl.views(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE VIEW \"Names\" SQL SECURITY DEFINER AS SELECT first_name, last_name FROM"
                + " Users"));
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

  @Test
  public void changeStreams() {
    String avroString1 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamAll\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : \"FOR ALL\","
            + "  \"spannerOption_0\" : \"retention_period=\\\"7d\\\"\","
            + "  \"spannerOption_1\" : \"value_capture_type=\\\"OLD_AND_NEW_VALUES\\\"\""
            + "}";
    String avroString2 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamEmpty\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : \"\""
            + "}";
    String avroString3 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamTableColumns\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : \"FOR `T1`, `T2`(`c1`, `c2`), `T3`()\","
            + "  \"spannerOption_0\" : \"retention_period=\\\"24h\\\"\""
            + "}";

    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(avroString1));
    schemas.add(parser.parse(avroString2));
    schemas.add(parser.parse(avroString3));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.changeStreams(), hasSize(3));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE CHANGE STREAM `ChangeStreamAll`"
                + " FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")"
                + " CREATE CHANGE STREAM `ChangeStreamEmpty`"
                + " CREATE CHANGE STREAM `ChangeStreamTableColumns`"
                + " FOR `T1`, `T2`(`c1`, `c2`), `T3`()"
                + " OPTIONS (retention_period=\"24h\")"));
  }

  @Test
  public void pgChangeStreams() {
    String avroString1 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamAll\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : \"FOR ALL\","
            + "  \"spannerOption_0\" : \"retention_period='7d'\","
            + "  \"spannerOption_1\" : \"value_capture_type='OLD_AND_NEW_VALUES'\""
            + "}";
    String avroString2 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamEmpty\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : \"\""
            + "}";
    String avroString3 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"ChangeStreamTableColumns\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerChangeStreamForClause\" : "
            + "    \"FOR \\\"T1\\\", \\\"T2\\\"(\\\"c1\\\", \\\"c2\\\"), \\\"T3\\\"()\","
            + "  \"spannerOption_0\" : \"retention_period='24h'\""
            + "}";

    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(avroString1));
    schemas.add(parser.parse(avroString2));
    schemas.add(parser.parse(avroString3));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(schemas);
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
    assertThat(ddl.changeStreams(), hasSize(3));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE CHANGE STREAM \"ChangeStreamAll\""
                + " FOR ALL"
                + " WITH (retention_period='7d', value_capture_type='OLD_AND_NEW_VALUES')"
                + " CREATE CHANGE STREAM \"ChangeStreamEmpty\""
                + " CREATE CHANGE STREAM \"ChangeStreamTableColumns\""
                + " FOR \"T1\", \"T2\"(\"c1\", \"c2\"), \"T3\"()"
                + " WITH (retention_period='24h')"));
  }

  @Test
  public void sequences() {
    String avroString1 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence1\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceOption_0\" : \"sequence_kind=\\\"bit_reversed_positive\\\"\","
            + "  \"sequenceOption_1\" : \"skip_range_min=0\","
            + "  \"sequenceOption_2\" : \"skip_range_max=1000\","
            + "  \"sequenceOption_3\" : \"start_with_counter=50\""
            + "}";
    String avroString2 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence2\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceOption_0\" : \"sequence_kind=\\\"bit_reversed_positive\\\"\","
            + "  \"sequenceOption_1\" : \"start_with_counter=9999\""
            + "}";
    String avroString3 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence3\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceOption_0\" : \"sequence_kind=\\\"bit_reversed_positive\\\"\""
            + "}";
    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(avroString1));
    schemas.add(parser.parse(avroString2));
    schemas.add(parser.parse(avroString3));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.sequences(), hasSize(3));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE SEQUENCE `Sequence1`\n\t"
                + "OPTIONS (sequence_kind=\"bit_reversed_positive\", "
                + "skip_range_min=0, skip_range_max=1000, start_with_counter=50)\n"
                + "CREATE SEQUENCE `Sequence2`\n\t"
                + "OPTIONS (sequence_kind=\"bit_reversed_positive\", "
                + "start_with_counter=9999)\n"
                + "CREATE SEQUENCE `Sequence3`\n\t"
                + "OPTIONS (sequence_kind=\"bit_reversed_positive\")"));
  }

  @Test
  public void pgSequences() {
    String avroString1 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence1\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceKind\" : \"bit_reversed_positive\","
            + "  \"skipRangeMin\" : \"1\","
            + "  \"skipRangeMax\" : \"1000\","
            + "  \"counterStartValue\" : \"50\""
            + "}";
    String avroString2 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence2\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceKind\" : \"bit_reversed_positive\","
            + "  \"counterStartValue\" : \"9999\""
            + "}";
    String avroString3 =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Sequence3\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"sequenceKind\" : \"bit_reversed_positive\""
            + "}";
    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(avroString1));
    schemas.add(parser.parse(avroString2));
    schemas.add(parser.parse(avroString3));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(schemas);
    assertEquals(ddl.dialect(), Dialect.POSTGRESQL);
    assertThat(ddl.sequences(), hasSize(3));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE SEQUENCE \"Sequence1\" BIT_REVERSED_POSITIVE "
                + "SKIP RANGE 1 1000 START COUNTER WITH 50"
                + "\nCREATE SEQUENCE \"Sequence2\" BIT_REVERSED_POSITIVE "
                + "START COUNTER WITH 9999"
                + "\nCREATE SEQUENCE \"Sequence3\" BIT_REVERSED_POSITIVE"));
  }

  @Test
  public void testInferType() {
    AvroSchemaToDdlConverter avroSchemaToDdlConverter = new AvroSchemaToDdlConverter();

    assertEquals(
        Type.bool(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.BOOLEAN), false));
    assertEquals(
        Type.int64(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.INT), false));
    assertEquals(
        Type.int64(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.LONG), false));
    assertEquals(
        Type.float32(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.FLOAT), false));
    assertEquals(
        Type.float64(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.DOUBLE), false));
    assertEquals(
        Type.string(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.STRING), false));
    assertEquals(
        Type.bytes(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.BYTES), false));

    Schema timestampSchema = Schema.create(Schema.Type.LONG);
    timestampSchema.addProp("logicalType", "timestamp-millis");
    assertEquals(Type.timestamp(), avroSchemaToDdlConverter.inferType(timestampSchema, false));
    timestampSchema = Schema.create(Schema.Type.LONG);
    timestampSchema.addProp("logicalType", "timestamp-micros");
    assertEquals(Type.timestamp(), avroSchemaToDdlConverter.inferType(timestampSchema, false));

    avroSchemaToDdlConverter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);

    assertEquals(
        Type.pgBool(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.BOOLEAN), false));
    assertEquals(
        Type.pgInt8(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.INT), false));
    assertEquals(
        Type.pgInt8(), avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.LONG), false));
    assertEquals(
        Type.pgFloat4(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.FLOAT), false));
    assertEquals(
        Type.pgFloat8(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.DOUBLE), false));
    assertEquals(
        Type.pgVarchar(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.STRING), false));
    assertEquals(
        Type.pgBytea(),
        avroSchemaToDdlConverter.inferType(Schema.create(Schema.Type.BYTES), false));

    timestampSchema = Schema.create(Schema.Type.LONG);
    timestampSchema.addProp("logicalType", "timestamp-millis");
    assertEquals(Type.pgTimestamptz(), avroSchemaToDdlConverter.inferType(timestampSchema, false));
    timestampSchema = Schema.create(Schema.Type.LONG);
    timestampSchema.addProp("logicalType", "timestamp-micros");
    assertEquals(Type.pgTimestamptz(), avroSchemaToDdlConverter.inferType(timestampSchema, false));
  }

  @Test
  public void placements() {
    String avroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"Placement1\","
            + "  \"fields\" : [],"
            + "  \"namespace\" : \"spannertest\","
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerEntity\":\"Placement\", "
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerOption_0\" : \"instance_partition='mr-partition'\","
            + "  \"spannerOption_1\" : \"default_leader='us-east1'\""
            + "}";
    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(avroString));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.placements(), hasSize(1));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE PLACEMENT `Placement1`\n\t"
                + " OPTIONS (instance_partition='mr-partition', default_leader='us-east1')"));
  }

  @Test
  public void placementTable() {
    String placementKeyAsPrimaryKeyAvroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"PlacementKeyAsPrimaryKey\","
            + "  \"namespace\" : \"spannertest\","
            + "  \"fields\" : [ {"
            + "    \"name\" : \"location\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"STRING(MAX)\","
            + "    \"notNull\" : \"true\","
            + "    \"spannerPlacementKey\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"val\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"STRING(MAX)\","
            + "    \"notNull\" : \"true\""
            + "  }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"`location` ASC\""
            + "}";

    String placementKeyAsNonPrimaryKeyAvroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"UsersWithPlacement\","
            + "  \"namespace\" : \"spannertest\","
            + "  \"fields\" : [ {"
            + "    \"name\" : \"id\","
            + "    \"type\" : \"long\","
            + "    \"sqlType\" : \"INT64\""
            + "  }, {"
            + "    \"name\" : \"location\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"STRING(MAX)\","
            + "    \"spannerPlacementKey\" : \"true\""
            + "  }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"`id` ASC\""
            + "}";

    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(placementKeyAsPrimaryKeyAvroString));
    schemas.add(parser.parse(placementKeyAsNonPrimaryKeyAvroString));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter();
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.allTables(), hasSize(2));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `PlacementKeyAsPrimaryKey` (\n\t"
                + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n\t"
                + "`val`                                   STRING(MAX) NOT NULL,\n"
                + ") PRIMARY KEY (`location` ASC)\n\n\n"
                + "CREATE TABLE `UsersWithPlacement` (\n\t"
                + "`id`                                    INT64 NOT NULL,\n\t"
                + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n"
                + ") PRIMARY KEY (`id` ASC)\n\n"));
  }

  @Test
  public void pgPlacementTable() {
    String placementKeyAsPrimaryKeyAvroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"PlacementKeyAsPrimaryKey\","
            + "  \"namespace\" : \"spannertest\","
            + "  \"fields\" : [ {"
            + "    \"name\" : \"location\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"character varying\","
            + "    \"spannerPlacementKey\" : \"true\""
            + "  }, {"
            + "    \"name\" : \"val\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"character varying\","
            + "    \"notNull\" : \"true\""
            + "  }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"location ASC\""
            + "}";

    String placementKeyAsNonPrimaryKeyAvroString =
        "{"
            + "  \"type\" : \"record\","
            + "  \"name\" : \"UsersWithPlacement\","
            + "  \"namespace\" : \"spannertest\","
            + "  \"fields\" : [ {"
            + "    \"name\" : \"id\","
            + "    \"type\" : \"long\","
            + "    \"sqlType\" : \"bigint\""
            + "  }, {"
            + "    \"name\" : \"location\","
            + "    \"type\" : \"string\","
            + "    \"sqlType\" : \"character varying\","
            + "    \"notNull\" : \"true\","
            + "    \"spannerPlacementKey\" : \"true\""
            + "  }],"
            + "  \"googleStorage\" : \"CloudSpanner\","
            + "  \"spannerParent\" : \"\","
            + "  \"googleFormatVersion\" : \"booleans\","
            + "  \"spannerPrimaryKey_0\" : \"id ASC\""
            + "}";

    Collection<Schema> schemas = new ArrayList<>();
    Schema.Parser parser = new Schema.Parser();
    schemas.add(parser.parse(placementKeyAsPrimaryKeyAvroString));
    schemas.add(parser.parse(placementKeyAsNonPrimaryKeyAvroString));

    AvroSchemaToDdlConverter converter = new AvroSchemaToDdlConverter(Dialect.POSTGRESQL);
    Ddl ddl = converter.toDdl(schemas);
    assertThat(ddl.allTables(), hasSize(2));
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE \"PlacementKeyAsPrimaryKey\" (\n\t"
                + "\"location\"                              character varying NOT NULL PLACEMENT KEY,\n\t"
                + "\"val\"                                   character varying NOT NULL,\n\t"
                + "PRIMARY KEY (\"location\")\n)\n\n\n"
                + "CREATE TABLE \"UsersWithPlacement\" (\n\t"
                + "\"id\"                                    bigint NOT NULL,\n\t"
                + "\"location\"                              character varying NOT NULL PLACEMENT KEY,\n\t"
                + "PRIMARY KEY (\"id\")\n)\n\n"));
  }
}
