/*
 * Copyright (C) 2018 Google LLC
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.spanner.IntegrationTest;
import com.google.cloud.teleport.spanner.SpannerServerResource;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.common.collect.HashMultimap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test coverage for {@link InformationSchemaScanner}. This requires an active GCP project with a
 * Spanner instance. Hence this test can only be run locally with a project set up using 'gcloud
 * config'.
 */
@Category(IntegrationTest.class)
public class InformationSchemaScannerTest {

  private final String dbId = "informationschemascannertest";

  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  @Before
  public void setup() {
    // Just to make sure an old database is not left over.
    spannerServer.dropDatabase(dbId);
  }

  @After
  public void tearDown() {
    spannerServer.dropDatabase(dbId);
  }

  private Ddl getDatabaseDdl() {
    BatchClient batchClient = spannerServer.getBatchClient(dbId);
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx);
    return scanner.scan();
  }

  @Test
  public void emptyDatabase() throws Exception {
    spannerServer.createDatabase(dbId, Collections.emptyList());
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl, equalTo(Ddl.builder().build()));
  }

  @Test
  public void tableWithAllTypes() throws Exception {
    String allTypes =
        "CREATE TABLE `alltypes` ("
            + " `first_name`                            STRING(MAX),"
            + " `last_name`                             STRING(5),"
            + " `id`                                    INT64 NOT NULL,"
            + " `bool_field`                            BOOL,"
            + " `int64_field`                           INT64,"
            + " `float64_field`                         FLOAT64,"
            + " `string_field`                          STRING(76),"
            + " `bytes_field`                           BYTES(13),"
            + " `timestamp_field`                       TIMESTAMP,"
            + " `date_field`                            DATE,"
            + " `arr_bool_field`                        ARRAY<BOOL>,"
            + " `arr_int64_field`                       ARRAY<INT64>,"
            + " `arr_float64_field`                     ARRAY<FLOAT64>,"
            + " `arr_string_field`                      ARRAY<STRING(15)>,"
            + " `arr_bytes_field`                       ARRAY<BYTES(MAX)>,"
            + " `arr_timestamp_field`                   ARRAY<TIMESTAMP>,"
            + " `arr_date_field`                        ARRAY<DATE>,"
            + " ) PRIMARY KEY (`first_name` ASC, `last_name` DESC, `id` ASC)";

    spannerServer.createDatabase(dbId, Collections.singleton(allTypes));
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("alltypes"), notNullValue());
    assertThat(ddl.table("aLlTYPeS"), notNullValue());

    Table table = ddl.table("alltypes");
    assertThat(table.columns(), hasSize(17));

    // Check case sensitiveness.
    assertThat(table.column("first_name"), notNullValue());
    assertThat(table.column("fIrst_NaME"), notNullValue());
    assertThat(table.column("last_name"), notNullValue());
    assertThat(table.column("LAST_name"), notNullValue());

    // Check types/sizes.
    assertThat(table.column("bool_field").type(), equalTo(Type.bool()));
    assertThat(table.column("int64_field").type(), equalTo(Type.int64()));
    assertThat(table.column("float64_field").type(), equalTo(Type.float64()));
    assertThat(table.column("string_field").type(), equalTo(Type.string()));
    assertThat(table.column("string_field").size(), equalTo(76));
    assertThat(table.column("bytes_field").type(), equalTo(Type.bytes()));
    assertThat(table.column("bytes_field").size(), equalTo(13));
    assertThat(table.column("timestamp_field").type(), equalTo(Type.timestamp()));
    assertThat(table.column("date_field").type(), equalTo(Type.date()));
    assertThat(table.column("arr_bool_field").type(), equalTo(Type.array(Type.bool())));
    assertThat(table.column("arr_int64_field").type(), equalTo(Type.array(Type.int64())));
    assertThat(table.column("arr_float64_field").type(), equalTo(Type.array(Type.float64())));
    assertThat(table.column("arr_string_field").type(), equalTo(Type.array(Type.string())));
    assertThat(table.column("arr_string_field").size(), equalTo(15));
    assertThat(table.column("arr_bytes_field").type(), equalTo(Type.array(Type.bytes())));
    assertThat(table.column("arr_bytes_field").size(), equalTo(-1 /*max*/));
    assertThat(table.column("arr_timestamp_field").type(), equalTo(Type.array(Type.timestamp())));
    assertThat(table.column("arr_date_field").type(), equalTo(Type.array(Type.date())));

    // Check not-null.
    assertThat(table.column("first_name").notNull(), is(false));
    assertThat(table.column("last_name").notNull(), is(false));
    assertThat(table.column("id").notNull(), is(true));

    // Check primary key.
    assertThat(table.primaryKeys(), hasSize(3));
    List<IndexColumn> pk = table.primaryKeys();
    assertThat(pk.get(0).name(), equalTo("first_name"));
    assertThat(pk.get(0).order(), equalTo(IndexColumn.Order.ASC));
    assertThat(pk.get(1).name(), equalTo("last_name"));
    assertThat(pk.get(1).order(), equalTo(IndexColumn.Order.DESC));
    assertThat(pk.get(2).name(), equalTo("id"));
    assertThat(pk.get(2).order(), equalTo(IndexColumn.Order.ASC));

    // Verify pretty print.
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(allTypes));
  }

  @Test
  public void simpleView() throws Exception {
    String tableDef =
        "CREATE TABLE Users ("
            + " id INT64 NOT NULL,"
            + " name STRING(MAX),"
            + ") PRIMARY KEY (id)";
    String viewDef = "CREATE VIEW Names SQL SECURITY INVOKER AS SELECT u.name FROM Users u";

    spannerServer.createDatabase(dbId, Arrays.asList(tableDef, viewDef));
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("Users"), notNullValue());
    assertThat(ddl.table("uSers"), notNullValue());

    assertThat(ddl.views(), hasSize(1));
    View view = ddl.view("Names");
    assertThat(view, notNullValue());
    assertThat(ddl.view("nAmes"), sameInstance(view));

    assertThat(view.query(), equalTo("SELECT u.name FROM Users u"));
  }

  @Test
  public void interleavedIn() throws Exception {
    List<String> statements =
        Arrays.asList(
            " CREATE TABLE lEVEl0 ("
                + " id0                                   INT64 NOT NULL,"
                + " val0                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC)",
            " CREATE TABLE level1 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " val1                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC), INTERLEAVE IN PARENT lEVEl0",
            " CREATE TABLE level2 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " id2                                   INT64 NOT NULL,"
                + " val2                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC, id2 ASC), INTERLEAVE IN PARENT level1",
            " CREATE TABLE level2_1 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " id2_1                                 INT64 NOT NULL,"
                + " val2                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC, id2_1 ASC),"
                + " INTERLEAVE IN PARENT level1 ON DELETE CASCADE");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(4));
    HashMultimap<Integer, String> levels = ddl.perLevelView();
    assertThat(levels.get(0), hasSize(1));
    assertThat(levels.get(1), hasSize(1));
    assertThat(levels.get(2), hasSize(2));
    assertThat(levels.get(3), hasSize(0));
    assertThat(levels.get(4), hasSize(0));
    assertThat(levels.get(5), hasSize(0));
    assertThat(levels.get(6), hasSize(0));
    assertThat(levels.get(7), hasSize(0));

    assertThat(ddl.table("lEVEl0").interleaveInParent(), nullValue());
    assertThat(ddl.table("level1").interleaveInParent(), equalTo("lEVEl0"));
    assertThat(ddl.table("level2").interleaveInParent(), equalTo("level1"));
    assertThat(ddl.table("level2").onDeleteCascade(), is(false));
    assertThat(ddl.table("level2_1").interleaveInParent(), equalTo("level1"));
    assertThat(ddl.table("level2_1").onDeleteCascade(), is(true));
  }

  @Test
  public void reserved() throws Exception {
    String statement =
        "CREATE TABLE `where` ("
            + " `JOIN`                                  STRING(MAX) NOT NULL,"
            + " `TABLE`                                 INT64,"
            + " `NULL`                                  INT64,"
            + " ) PRIMARY KEY (`NULL` ASC)";

    spannerServer.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));

    assertThat(ddl.table("where"), notNullValue());
    Table table = ddl.table("where");
    assertThat(table.column("join"), notNullValue());
    assertThat(table.column("table"), notNullValue());
    assertThat(table.column("null"), notNullValue());

    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void indexes() throws Exception {
    // Prefix indexes to ensure ordering.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE UNIQUE NULL_FILTERED INDEX `a_last_name_idx` ON "
                + " `Users`(`last_name` ASC) STORING (`first_name`)",
            " CREATE INDEX `b_age_idx` ON `Users`(`age` DESC)",
            " CREATE UNIQUE INDEX `c_first_name_idx` ON `Users`(`first_name` ASC)");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void foreignKeys() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `Ref` ("
                + " `id1`                               INT64 NOT NULL,"
                + " `id2`                               INT64 NOT NULL,"
                + " ) PRIMARY KEY (`id1` ASC, `id2` ASC)",
            " CREATE TABLE `Tab` ("
                + " `key`                               INT64 NOT NULL,"
                + " `id1`                               INT64 NOT NULL,"
                + " `id2`                               INT64 NOT NULL,"
                + " ) PRIMARY KEY (`key` ASC)",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id2`, `id1`)");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  // TODO: enable this test once CHECK constraints are enabled
  // @Test
  public void checkConstraints() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `T` ("
                + " `id`     INT64 NOT NULL,"
                + " `A`      INT64 NOT NULL,"
                + " CONSTRAINT `ck` CHECK(A>0),"
                + " ) PRIMARY KEY (`id` ASC)");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void commitTimestamp() throws Exception {
    String statement =
        "CREATE TABLE `Users` ("
            + " `id`                                    INT64 NOT NULL,"
            + " `birthday`                              TIMESTAMP NOT NULL "
            + " OPTIONS (allow_commit_timestamp=TRUE),"
            + " ) PRIMARY KEY (`id` ASC)";

    spannerServer.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  // TODO: enable this test once generated columns are supported.
  // @Test
  public void generatedColumns() throws Exception {
    String statement =
        "CREATE TABLE `T` ("
            + " `id`                                     INT64 NOT NULL,"
            + " `generated`                              INT64 NOT NULL AS (`id`) STORED, "
            + " ) PRIMARY KEY (`id` ASC)";

    spannerServer.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void databaseOptions() throws Exception {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE `" + dbId + "` SET OPTIONS ( version_retention_period = \"5d\" )\n",
            "CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE UNIQUE NULL_FILTERED INDEX `a_last_name_idx` ON "
                + " `Users`(`last_name` ASC) STORING (`first_name`)",
            " CREATE INDEX `b_age_idx` ON `Users`(`age` DESC)",
            " CREATE UNIQUE INDEX `c_first_name_idx` ON `Users`(`first_name` ASC)");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    String alterStatement = statements.get(0);
    statements.set(0, alterStatement.replace(dbId, "%db_name%"));
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }
}
