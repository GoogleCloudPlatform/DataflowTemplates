/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerSchemaTest {

  @Test
  public void testSpannerSchemaPopulation() throws Exception {
    Ddl ddl =
        Ddl.builder()
            .createTable("Users")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("first_name")
            .string()
            .size(10)
            .endColumn()
            .column("last_name")
            .type(com.google.cloud.teleport.v2.spanner.type.Type.string())
            .max()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .createTable("Account")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("balanceId")
            .int64()
            .notNull()
            .endColumn()
            .column("balance")
            .float64()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .interleaveInParent("Users")
            .onDeleteCascade()
            .endTable()
            .build();

    Map<String, SpannerTable> spannerTables =
        SpannerSchema.convertDDLTableToSpannerTable(ddl.allTables());
    assertNotNull(spannerTables);

    SpannerTable usersTable = spannerTables.get("Users");
    assertNotNull(usersTable);
    assertEquals("Users", usersTable.getName());

    SpannerTable accountTable = spannerTables.get("Account");
    assertNotNull(accountTable);
    assertEquals("Account", accountTable.getName());
    assertEquals(3, accountTable.getColDefs().size());

    Map<String, NameAndCols> nameAndColsTable =
        SpannerSchema.convertDDLTableToSpannerNameAndColsTable(ddl.allTables());
    assertNotNull(nameAndColsTable);

    NameAndCols usersColumns = nameAndColsTable.get("Users");
    assertNotNull(usersColumns);
    assertTrue(usersColumns.getCols().containsKey("id"));
    assertTrue(usersColumns.getCols().containsKey("first_name"));
    assertTrue(usersColumns.getCols().containsKey("last_name"));

    NameAndCols accountColumns = nameAndColsTable.get("Account");
    assertNotNull(accountColumns);
    assertTrue(accountColumns.getCols().containsKey("id"));
    assertTrue(accountColumns.getCols().containsKey("balanceId"));
    assertTrue(accountColumns.getCols().containsKey("balance"));
  }
}
