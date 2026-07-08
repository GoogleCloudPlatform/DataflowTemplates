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
package com.google.cloud.teleport.v2.neo4j.model.validation;

import static com.google.common.truth.Truth.assertThat;
import static org.neo4j.importer.v1.ImportSpecificationDeserializer.deserialize;

import java.io.StringReader;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.importer.v1.validation.InvalidSpecificationException;
import org.neo4j.importer.v1.validation.SpecificationException;

public class TextColumnMappingValidatorTest {

  @Test
  public void
      fails_if_node_target_property_mappings_source_field_is_not_listed_in_text_source_fields() {
    var spec =
        "{\n"
            + "    \"version\": \"1\",\n"
            + "    \"sources\": [{\n"
            + "        \"name\": \"a-source\",\n"
            + "        \"type\": \"text\",\n"
            + "        \"header\": [\"field-1\"],\n"
            + "        \"data\": [\n"
            + "            [\"foo\"], [\"bar\"]\n"
            + "        ]\n"
            + "    }],\n"
            + "    \"targets\": {\n"
            + "        \"nodes\": [{\n"
            + "            \"active\": true,\n"
            + "            \"name\": \"a-target\",\n"
            + "            \"write_mode\": \"create\",\n"
            + "            \"source\": \"a-source\",\n"
            + "            \"labels\": [\"Placeholder\"],\n"
            + "            \"properties\": [\n"
            + "                {\"source_field\": \"invalid-field\", \"target_property\": \"property\"}\n"
            + "            ],\n"
            + "            \"schema\": {\n"
            + "              \"key_constraints\": [\n"
            + "                {\"name\": \"key property\", \"label\": \"Placeholder\", \"properties\": [\"property\"]}\n"
            + "              ]\n"
            + "            }\n"
            + "        }]\n"
            + "    }\n"
            + "}";

    var exception =
        Assert.assertThrows(
            InvalidSpecificationException.class, () -> deserialize(new StringReader(spec)));

    assertThat(exception).hasMessageThat().contains("1 error(s)");
    assertThat(exception).hasMessageThat().contains("0 warning(s)");
    assertThat(exception)
        .hasMessageThat()
        .contains(
            "$.targets.nodes[0].properties[0].source_field field \"invalid-field\" is neither defined in the target's text source nor its source transformations");
  }

  @Test
  public void
      does_not_fail_if_node_target_property_mappings_source_field_is_listed_in_target_aggregations()
          throws SpecificationException {
    var spec =
        "{\n"
            + "    \"version\": \"1\",\n"
            + "    \"sources\": [{\n"
            + "        \"name\": \"a-source\",\n"
            + "        \"type\": \"text\",\n"
            + "        \"header\": [\"field-1\"],\n"
            + "        \"data\": [\n"
            + "            [\"foo\"], [\"bar\"]\n"
            + "        ]\n"
            + "    }],\n"
            + "    \"targets\": {\n"
            + "        \"nodes\": [{\n"
            + "            \"active\": true,\n"
            + "            \"name\": \"a-target\",\n"
            + "            \"write_mode\": \"create\",\n"
            + "            \"source\": \"a-source\",\n"
            + "            \"labels\": [\"Placeholder\"],\n"
            + "            \"source_transformations\": {\n"
            + "              \"aggregations\": [\n"
            + "                {"
            + "\"field_name\": \"aggregated\", "
            + "\"expression\": \"42\""
            + "}\n"
            + "              ]\n"
            + "            },\n"
            + "          "
            + "  \"properties\": [\n"
            + "                {\"source_field\": \"aggregated\", \"target_property\":"
            + " \"property\"}\n"
            + "            ],\n"
            + "            \"schema\": {\n"
            + "              \"key_constraints\": [\n"
            + "                {\"name\": \"key property\", \"label\": \"Placeholder\", \"properties\": [\"property\"]}\n"
            + "              ]\n"
            + "            }\n"
            + "        }]\n"
            + "    }\n"
            + "}";

    var result = deserialize(new StringReader(spec));

    assertThat(result).isNotNull();
  }
}
