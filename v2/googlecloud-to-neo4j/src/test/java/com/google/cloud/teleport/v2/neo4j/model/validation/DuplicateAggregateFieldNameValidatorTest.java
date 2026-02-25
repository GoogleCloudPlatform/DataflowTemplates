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

public class DuplicateAggregateFieldNameValidatorTest {

  @Test
  public void
      fails_if_node_target_source_transformation_aggregations_field_name_clashes_with_text_source_fields() {
    var spec =
        """
            {
                "version": "1",
                "sources": [{
                    "name": "a-source",
                    "type": "text",
                    "header": ["field_1"],
                    "data": [
                        ["foo"], ["bar"]
                    ]
                }],
                "targets": {
                    "nodes": [{
                        "name": "a-target",
                        "source": "a-source",
                        "write_mode": "merge",
                        "labels": ["Placeholder"],
                        "properties": [
                            {"source_field": "field_1", "target_property": "property"}
                        ],
                        "source_transformations": {
                            "aggregations": [{
                                "expression": "42",
                                "field_name": "field_1"
                            }]
                        },
                        "schema": {
                          "key_constraints": [
                            {"name": "key property", "label": "Placeholder", "properties": ["property"]}
                          ]
                        }
                    }]
                }
            }"""
            .stripIndent();

    var exception =
        Assert.assertThrows(
            InvalidSpecificationException.class, () -> deserialize(new StringReader(spec)));

    assertThat(exception).hasMessageThat().contains("1 error(s)");
    assertThat(exception).hasMessageThat().contains("0 warning(s)");
    assertThat(exception)
        .hasMessageThat()
        .contains(
            "$.targets.nodes[0].source_transformations.aggregations[0].field_name \"field_1\" is already defined in the target's source header");
  }
}
