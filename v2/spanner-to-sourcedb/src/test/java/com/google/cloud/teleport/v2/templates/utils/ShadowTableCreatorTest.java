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
package com.google.cloud.teleport.v2.templates.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class ShadowTableCreatorTest {

  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private SpannerAccessor mockSpannerAccessor;

  @Mock private DatabaseAdminClient mockDatabaseClient;
  private SpannerConfig testSpannerConfig;

  @Mock private ReadOnlyTransaction mockReadOnlyTransaction;

  @Mock private OperationFuture<Void, UpdateDatabaseDdlMetadata> mockUpdateDatabaseDdlFuture;

  @Before
  public void doBeforeEachTest() throws Exception {
    testSpannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of("test"))
            .withInstanceId(ValueProvider.StaticValueProvider.of("test"))
            .withDatabaseId(ValueProvider.StaticValueProvider.of("test"));

    when(mockSpannerAccessor.getDatabaseAdminClient()).thenReturn(mockDatabaseClient);
    when(mockDatabaseClient.updateDatabaseDdl(any(), any(), any(), any()))
        .thenReturn(mockUpdateDatabaseDdlFuture);
    when(mockUpdateDatabaseDdlFuture.get(5, TimeUnit.MINUTES)).thenReturn(null);
  }

  @Test
  public void testShadowTableCreated() {
    Ddl primaryDbDdl = getPrimaryDbDdl();
    Ddl metadataDbDdl = getMetadataDbDdl();
    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            Dialect.GOOGLE_STANDARD_SQL,
            "shadow_",
            primaryDbDdl,
            metadataDbDdl,
            mockSpannerAccessor,
            testSpannerConfig);
    List<String> tablesToCreate = shadowTableCreator.getDataTablesWithNoShadowTables();
    List<String> expectedTablesToCreate = ImmutableList.of("table1", "table4");
    assertThat(tablesToCreate).containsExactlyElementsIn(expectedTablesToCreate);

    Table shadowTable = shadowTableCreator.constructShadowTable("table1");
    assertThat(shadowTable.name()).isEqualTo("shadow_table1");
    assertThat(shadowTable.columns()).hasSize(3);
    assertThat(shadowTable.columns().get(0).name()).isEqualTo("id");
    assertThat(shadowTable.columns().get(1).name())
        .isEqualTo(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME);
    assertThat(shadowTable.columns().get(2).name()).isEqualTo(Constants.RECORD_SEQ_COLUMN_NAME);
    assertThat(shadowTable.primaryKeys()).hasSize(1);
    assertThat(shadowTable.primaryKeys().get(0).name()).isEqualTo("id");
    assertThat(shadowTable.primaryKeys().get(0).order()).isEqualTo(IndexColumn.Order.ASC);

    Table shadowTable4 = shadowTableCreator.constructShadowTable("table4");
    assertThat(shadowTable4.name()).isEqualTo("shadow_table4");
    assertThat(shadowTable4.columns()).hasSize(4);
    assertThat(shadowTable4.columns().get(0).name()).isEqualTo("id4");
    assertThat(shadowTable4.columns().get(1).name()).isEqualTo("id5");
    assertThat(shadowTable4.columns().get(2).name())
        .isEqualTo(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME);
    assertThat(shadowTable4.columns().get(3).name()).isEqualTo(Constants.RECORD_SEQ_COLUMN_NAME);
    assertThat(shadowTable4.primaryKeys()).hasSize(2);
    assertThat(shadowTable4.primaryKeys().get(0).name()).isEqualTo("id4");
    assertThat(shadowTable4.primaryKeys().get(0).order()).isEqualTo(IndexColumn.Order.ASC);
    assertThat(shadowTable4.primaryKeys().get(1).name()).isEqualTo("id5");
    assertThat(shadowTable4.primaryKeys().get(1).order()).isEqualTo(IndexColumn.Order.DESC);

    shadowTableCreator.createShadowTablesInSpanner();
    verify(mockDatabaseClient).updateDatabaseDdl(any(), any(), any(), any());
  }

  @Test
  public void testNoShadowTableCreated() {
    Ddl primaryDbDdl = getMetadataDbDdl();
    Ddl metadataDbDdl = getMetadataDbDdl();
    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            Dialect.GOOGLE_STANDARD_SQL,
            "shadow_",
            primaryDbDdl,
            metadataDbDdl,
            mockSpannerAccessor,
            testSpannerConfig);
    List<String> tablesToCreate = shadowTableCreator.getDataTablesWithNoShadowTables();
    assertThat(tablesToCreate).isEmpty();

    shadowTableCreator.createShadowTablesInSpanner();
    verify(mockDatabaseClient, never()).updateDatabaseDdl(any(), any(), any(), any());
  }

  private Ddl getPrimaryDbDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("table1")
            .column("id")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("table2")
            .column("id2")
            .int64()
            .endColumn()
            .column("update_ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id2")
            .end()
            .endTable()
            .createTable("shadow_table3")
            .column("id3")
            .int64()
            .endColumn()
            .column("shadow_update_ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id3")
            .end()
            .endTable()
            .createTable("table4")
            .column("id4")
            .int64()
            .endColumn()
            .column("id5")
            .int64()
            .endColumn()
            .column("shadow_update_ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id4")
            .desc("id5")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private Ddl getMetadataDbDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("shadow_table2")
            .column("id2")
            .int64()
            .endColumn()
            .column("shadow_update_ts")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("id2")
            .end()
            .endTable()
            .build();
    return ddl;
  }
}
