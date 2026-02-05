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

public class BigQuerySourceProjectDatasetValidatorTest {

  @Test
  public void fails_if_bigquery_source_only_has_temp_project_id_but_not_temp_dataset_id() {
    var spec =
        """
            {
                "version": "1",
                "sources": [{
                    "type": "bigquery",
                    "name": "a-source",
                    "query": "SELECT field_1 FROM project.dataset.table",
                    "query_temp_project": "project"
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
        .contains("$.sources[0] query_temp_project is provided, but query_temp_dataset is missing");
  }
}
