/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.postgres;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link PostgresSrcToSpSourceConnector}. */
@RunWith(JUnit4.class)
public class PostgresSourceConnectorTest {

  private final PostgresSrcToSpSourceConnector connector = new PostgresSrcToSpSourceConnector();

  @Test
  public void testGetSourceType() {
    assertThat(connector.getSourceType()).isEqualTo(Constants.POSTGRES_SOURCE_TYPE);
  }

  @Test
  public void testGetSourceSchemaReference_withDefaultNamespace() {
    SourceSchemaReference schemaRef = connector.getSourceSchemaReference("test_db", null);
    assertThat(schemaRef.jdbc().dbName()).isEqualTo("test_db");
    assertThat(schemaRef.jdbc().namespace()).isEqualTo("public");
  }

  @Test
  public void testGetSourceSchemaReference_withCustomNamespace() {
    SourceSchemaReference schemaRef =
        connector.getSourceSchemaReference("test_db", "custom_schema");
    assertThat(schemaRef.jdbc().dbName()).isEqualTo("test_db");
    assertThat(schemaRef.jdbc().namespace()).isEqualTo("custom_schema");
  }

  @Test
  public void testGetJdbcUrl_constructsUrl() {
    String url = connector.getJdbcUrl(null, "localhost", 5432, "test_db", null, null, null);
    assertThat(url).isEqualTo("jdbc:postgresql://localhost:5432/test_db?currentSchema=public");
  }

  @Test
  public void testGetJdbcUrl_withConnectionProperties() {
    String url =
        connector.getJdbcUrl(null, "localhost", 5432, "test_db", "ssl=true", "myschema", null);
    assertThat(url)
        .isEqualTo("jdbc:postgresql://localhost:5432/test_db?currentSchema=myschema&ssl=true");
  }
}
