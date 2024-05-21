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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Test;
import org.mockito.Mockito;

public class SourceDbToSpannerTest {

  @Test
  public void testCreateSpannerConfig() {
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getProjectId()).thenReturn("testProject");
    when(mockOptions.getSpannerHost()).thenReturn("testHost");
    when(mockOptions.getInstanceId()).thenReturn("testInstance");
    when(mockOptions.getDatabaseId()).thenReturn("testDatabaseId");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(mockOptions);
    assertEquals(config.getProjectId().get(), "testProject");
    assertEquals(config.getInstanceId().get(), "testInstance");
    assertEquals(config.getDatabaseId().get(), "testDatabaseId");
  }

  @Test
  public void createTableIdToRefMap() {
    SourceSchema srcSchema =
        SourceSchema.builder()
            .setSchemaReference(SourceSchemaReference.builder().setDbName("test").build())
            .addTableSchema(
                SourceTableSchema.builder()
                    .setTableName("testTable")
                    .setTableSchemaUUID("uuid1")
                    .addSourceColumnNameToSourceColumnType(
                        "col1", new SourceColumnType("int", new Long[] {}, new Long[] {}))
                    .build())
            .build();

    Map<String, SourceTableReference> tableIDToRefMap =
        SourceDbToSpanner.getTableIDToRefMap(srcSchema);
    assertEquals(tableIDToRefMap.get("uuid1").getName(), "Db.test.Table.testTable");
    assertNull(tableIDToRefMap.get("uuid2"));
  }
}
