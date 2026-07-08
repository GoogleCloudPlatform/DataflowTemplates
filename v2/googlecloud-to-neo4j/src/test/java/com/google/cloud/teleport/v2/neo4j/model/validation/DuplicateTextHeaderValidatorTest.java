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

public class DuplicateTextHeaderValidatorTest {

  @Test
  public void fails_if_external_text_source_header_includes_duplicated_names() {
    var spec =
        "{\n"
            + "    \"version\": \"1\",\n"
            + "    \"sources\": [{\n"
            + "        \"name\": \"a-source\",\n"
            + "        \"type\": \"text\",\n"
            + "        \"header\": [\"duplicate\", \"duplicate\"],\n"
            + "        \"urls\": [\n"
            + "            \"https://example.com/my.csv\"\n"
            + "        ]\n"
            + "    }],\n"
            + "    \"targets\": {\n"
            + "        \"queries\": [{\n"
            + "            \"name\": \"a-target\",\n"
            + "            \"source\": \"a-source\",\n"
            + "            \"query\": \"UNWIND $rows AS row CREATE (n:ANode) SET n = row\"\n"
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
            "$.sources[0].header defines column \"duplicate\" 2 times, it must be defined at most once");
  }
}
