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

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ShadowTableCreatorTest {

  @Test
  public void behaviorBeingTested_expectedResult() {
    Ddl mainDbDdl = getMainDbDdl();
    Ddl metadataDbDdl = getMetadataDbDdl();
    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(Dialect.GOOGLE_STANDARD_SQL, "shadow_", mainDbDdl, metadataDbDdl);
    List<String> tablesToCreate = shadowTableCreator.getDataTablesWithNoShadowTables();
    assertThat(tablesToCreate).containsExactly("table1");
    Table shadowTable = shadowTableCreator.constructShadowTable("table1");
    assertThat(shadowTable.name()).isEqualTo("shadow_table1");
    assertThat(shadowTable.columns()).hasSize(2);
    assertThat(shadowTable.columns().get(0).name()).isEqualTo("id");
    assertThat(shadowTable.columns().get(1).name())
        .isEqualTo(Constants.PROCESSED_COMMIT_TS_COLUMN_NAME);
    assertThat(shadowTable.primaryKeys()).hasSize(1);
    assertThat(shadowTable.primaryKeys().get(0).name()).isEqualTo("id");
    assertThat(shadowTable.primaryKeys().get(0).order()).isEqualTo(IndexColumn.Order.ASC);
  }

  private Ddl getMainDbDdl() {
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
