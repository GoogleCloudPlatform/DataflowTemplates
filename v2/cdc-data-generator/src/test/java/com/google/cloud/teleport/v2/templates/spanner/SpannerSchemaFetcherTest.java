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
package com.google.cloud.teleport.v2.templates.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.ForeignKey;
import com.google.cloud.teleport.v2.spanner.ddl.Index;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher.DdlFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher.SpannerAccessorFetcher;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class SpannerSchemaFetcherTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private DdlFetcher mockDdlFetcher;
  @Mock private SpannerAccessorFetcher mockSpannerAccessorFetcher;
  @Mock private SpannerAccessor mockSpannerAccessor;
  @Mock private Ddl mockDdl;

  private SpannerSchemaFetcher fetcher;
  private JSONObject defaultJson;

  @Before
  public void setUp() {
    fetcher = new SpannerSchemaFetcher(mockDdlFetcher, mockSpannerAccessorFetcher);
    when(mockSpannerAccessorFetcher.get(any(SpannerConfig.class))).thenReturn(mockSpannerAccessor);
    defaultJson = new JSONObject();
    defaultJson.put("projectId", "test-project");
    defaultJson.put("instanceId", "test-instance");
    defaultJson.put("databaseId", "test-database");
  }

  @Test
  public void testInit_success() {
    fetcher.init(null, defaultJson.toString());
    // No assertion needed, success is no exception
  }

  @Test
  public void testInit_missingFields() {
    JSONObject json = new JSONObject();
    json.put("projectId", "test-project");
    assertThrows(JSONException.class, () -> fetcher.init(null, json.toString()));
  }

  @Test
  public void testInit_invalidJson() {
    String invalidJson = "{\"projectId\": \"test-project\",";

    assertThrows(JSONException.class, () -> fetcher.init(null, invalidJson));
  }

  @Test
  public void testGetSchema_simpleTable() throws Exception {
    fetcher.init(null, defaultJson.toString());
    fetcher.setInsertQps(100);

    Table tableA = Table.builder().name("TableA").build();
    when(mockDdl.allTables()).thenReturn(ImmutableList.of(tableA));
    when(mockDdl.dialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockDdlFetcher.fetch(any(SpannerConfig.class))).thenReturn(mockDdl);

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    assertThat(schema.tables()).containsKey("TableA");
    DataGeneratorTable actualTable = schema.tables().get("TableA");
    assertThat(actualTable.insertQps()).isEqualTo(100);
    assertThat(actualTable.isRoot()).isTrue();
  }

  @Test
  public void testGetSchema_withAllFeatures() throws Exception {
    fetcher.init(null, defaultJson.toString());
    fetcher.setInsertQps(50);

    // Mock DDL components
    Table mockTableB = mock(Table.class);
    when(mockTableB.name()).thenReturn("TableB");

    Column mockColumn1 = mock(Column.class);
    when(mockColumn1.name()).thenReturn("Col1");
    when(mockColumn1.type()).thenReturn(Type.string());
    when(mockColumn1.notNull()).thenReturn(true);

    when(mockTableB.columns()).thenReturn(ImmutableList.of(mockColumn1));

    IndexColumn mockIndexColumn1 = mock(IndexColumn.class);
    when(mockIndexColumn1.name()).thenReturn("Col1");
    when(mockTableB.primaryKeys()).thenReturn(ImmutableList.of(mockIndexColumn1));

    ForeignKey mockForeignKey = mock(ForeignKey.class);
    when(mockForeignKey.name()).thenReturn("fk_b_a");
    when(mockForeignKey.columns()).thenReturn(ImmutableList.of("Col1"));
    when(mockForeignKey.referencedTable()).thenReturn("TableA");
    when(mockForeignKey.referencedColumns()).thenReturn(ImmutableList.of("RefCol"));
    when(mockTableB.foreignKeys()).thenReturn(ImmutableList.of(mockForeignKey));

    Index mockIndex1 = mock(Index.class);
    when(mockIndex1.name()).thenReturn("idx_b_col2");
    when(mockIndex1.unique()).thenReturn(true);
    when(mockIndex1.indexColumns()).thenReturn(ImmutableList.<IndexColumn>of());
    Index mockIndexPK = mock(Index.class);
    when(mockIndexPK.name()).thenReturn("PRIMARY_KEY");
    when(mockIndexPK.unique()).thenReturn(true);
    when(mockIndexPK.indexColumns()).thenReturn(ImmutableList.<IndexColumn>of());
    when(mockTableB.indexes())
        .thenReturn((ImmutableList<Index>) ImmutableList.of(mockIndex1, mockIndexPK));

    when(mockTableB.interleavingParent()).thenReturn("TableA");

    when(mockDdl.allTables()).thenReturn(ImmutableList.of(mockTableB));
    when(mockDdl.dialect()).thenReturn(Dialect.GOOGLE_STANDARD_SQL);
    when(mockDdlFetcher.fetch(any(SpannerConfig.class))).thenReturn(mockDdl);

    DataGeneratorSchema schema = fetcher.getSchema();
    assertThat(schema.tables()).hasSize(1);
    DataGeneratorTable actualTable = schema.tables().get("TableB");
    assertThat(actualTable.name()).isEqualTo("TableB");
    assertThat(actualTable.columns()).hasSize(1);
    assertThat(actualTable.primaryKeys()).containsExactly("Col1");
    assertThat(actualTable.foreignKeys()).hasSize(1);
    assertThat(actualTable.uniqueKeys()).hasSize(1);
    assertThat(actualTable.uniqueKeys().get(0).name()).isEqualTo("idx_b_col2");
    assertThat(actualTable.interleavedInTable()).isEqualTo("TableA");
    assertThat(actualTable.isRoot()).isFalse();
    assertThat(actualTable.insertQps()).isEqualTo(50);
  }

  @Test
  public void testGetSchema_fetchDdlError() throws Exception {
    fetcher.init(null, defaultJson.toString());
    when(mockDdlFetcher.fetch(any(SpannerConfig.class)))
        .thenThrow(new RuntimeException("Failed to fetch DDL"));

    IOException ex = assertThrows(IOException.class, fetcher::getSchema);
    assertThat(ex).hasMessageThat().contains("Failed to fetch Spanner schema");
    assertThat(ex).hasCauseThat().isInstanceOf(RuntimeException.class);
  }
}
