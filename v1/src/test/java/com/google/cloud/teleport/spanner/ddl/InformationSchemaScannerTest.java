/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import org.junit.Test;

/** Unit tests for InformationSchemaScanner class. */
public class InformationSchemaScannerTest {

  final InformationSchemaScanner googleSQLInfoScanner =
      new InformationSchemaScanner(null, Dialect.GOOGLE_STANDARD_SQL);
  final InformationSchemaScanner postgresSQLInfoScanner =
      new InformationSchemaScanner(null, Dialect.POSTGRESQL);

  @Test
  public void testDatabaseOptionsSQL() {
    assertThat(
        googleSQLInfoScanner.databaseOptionsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.option_name, t.option_type, t.option_value  "
                + "FROM information_schema.database_options AS t  WHERE t.schema_name = ''"));

    assertThat(
        postgresSQLInfoScanner.databaseOptionsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.option_name, t.option_type, t.option_value  "
                + "FROM information_schema.database_options AS t  WHERE t.schema_name = 'public'"));
  }

  @Test
  public void testListColumnsSQL() {
    assertThat(
        googleSQLInfoScanner.listColumnsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "WITH placementkeycolumns AS ( SELECT c.table_name, c.column_name, c.constraint_name"
                + " FROM information_schema.constraint_column_usage AS c"
                + " WHERE c.constraint_name = CONCAT('PLACEMENT_KEY_', c.table_name))"
                + " SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored,"
                + " c.column_default, c.is_identity, c.identity_kind, c.identity_start_with_counter,"
                + " c.identity_skip_range_min, c.identity_skip_range_max, c.is_hidden,"
                + " pkc.constraint_name IS NOT NULL AS is_placement_key"
                + " FROM information_schema.columns as c"
                + " LEFT JOIN placementkeycolumns AS pkc"
                + " ON c.table_name = pkc.table_name AND c.column_name = pkc.column_name"
                + " WHERE c.table_schema NOT IN"
                + " ('INFORMATION_SCHEMA', 'SPANNER_SYS') AND c.spanner_state = 'COMMITTED' "
                + " ORDER BY c.table_name, c.ordinal_position"));

    assertThat(
        postgresSQLInfoScanner.listColumnsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "WITH placementkeycolumns AS ( SELECT c.table_name, c.column_name, c.constraint_name"
                + " FROM information_schema.constraint_column_usage AS c"
                + " WHERE c.constraint_name = CONCAT('PLACEMENT_KEY_', c.table_name))"
                + " SELECT c.table_schema, c.table_name, c.column_name, c.ordinal_position, c.spanner_type, c.is_nullable,"
                + " c.is_generated, c.generation_expression, c.is_stored, c.column_default,"
                + " c.is_identity, c.identity_kind, c.identity_start_with_counter,"
                + " c.identity_skip_range_min, c.identity_skip_range_max,"
                + " pkc.constraint_name IS NOT NULL AS is_placement_key"
                + " FROM information_schema.columns as c"
                + " LEFT JOIN placementkeycolumns AS pkc"
                + " ON c.table_name = pkc.table_name AND c.column_name = pkc.column_name"
                + " WHERE c.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')  AND c.spanner_state ="
                + " 'COMMITTED'  ORDER BY c.table_name, c.ordinal_position"));
  }

  @Test
  public void testListIndexesSQL() {
    assertThat(
        googleSQLInfoScanner.listIndexesSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered, t.filter, t.index_type, t.search_partition_by, t.search_order_by"
                + " FROM information_schema.indexes AS t"
                + " WHERE t.table_schema NOT IN"
                + " ('INFORMATION_SCHEMA', 'SPANNER_SYS') AND"
                + " (t.index_type='INDEX' OR t.index_type='SEARCH' OR t.index_type='VECTOR') AND t.spanner_is_managed = FALSE"
                + " ORDER BY t.table_name, t.index_name"));

    assertThat(
        postgresSQLInfoScanner.listIndexesSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.index_name, t.parent_table_name, t.is_unique,"
                + " t.is_null_filtered, t.filter FROM information_schema.indexes AS t "
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.index_type='INDEX' AND t.spanner_is_managed = 'NO' "
                + " ORDER BY t.table_name, t.index_name"));
  }

  @Test
  public void testListIndexColumnsSQL() {
    assertThat(
        googleSQLInfoScanner.listIndexColumnsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.column_name, t.column_ordering, t.index_name, "
                + "t.index_type, t.spanner_type "
                + "FROM information_schema.index_columns AS t "
                + " WHERE t.table_schema NOT IN"
                + " ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position"));

    assertThat(
        postgresSQLInfoScanner.listIndexColumnsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.column_name, t.column_ordering, t.index_name "
                + "FROM information_schema.index_columns AS t "
                + "WHERE t.table_schema NOT IN "
                + "('information_schema', 'spanner_sys', 'pg_catalog') "
                + "ORDER BY t.table_name, t.index_name, t.ordinal_position"));
  }

  @Test
  public void testListColumnOptionsSQL() {
    assertThat(
        googleSQLInfoScanner.listColumnOptionsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_schema NOT IN"
                + " ('INFORMATION_SCHEMA', 'SPANNER_SYS')"
                + " ORDER BY t.table_name, t.column_name"));

    assertThat(
        postgresSQLInfoScanner.listColumnOptionsSQL().getSql(),
        equalToCompressingWhiteSpace(
            "SELECT t.table_schema, t.table_name, t.column_name, t.option_name, t.option_type,"
                + " t.option_value"
                + " FROM information_schema.column_options AS t"
                + " WHERE t.table_schema NOT IN "
                + " ('information_schema', 'spanner_sys', 'pg_catalog')"
                + " AND t.option_name NOT IN ('allow_commit_timestamp')"
                + " ORDER BY t.table_name, t.column_name"));
  }
}
