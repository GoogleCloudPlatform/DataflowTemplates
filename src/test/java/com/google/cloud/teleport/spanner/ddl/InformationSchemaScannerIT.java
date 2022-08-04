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
import com.google.cloud.spanner.Dialect;
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
public class InformationSchemaScannerIT {

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

  private Ddl getPgDatabaseDdl() {
    BatchClient batchClient = spannerServer.getBatchClient(dbId);
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx, Dialect.POSTGRESQL);
    return scanner.scan();
  }

  @Test
  public void emptyDatabase() throws Exception {
    spannerServer.createDatabase(dbId, Collections.emptyList());
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl, equalTo(Ddl.builder().build()));
  }

  @Test
  public void pgEmptyDatabase() throws Exception {
    spannerServer.createPgDatabase(dbId, Collections.emptyList());
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl, equalTo(Ddl.builder(Dialect.POSTGRESQL).build()));
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
  public void tableWithAllPgTypes() throws Exception {
    String allTypes =
        "CREATE TABLE \"alltypes\" ("
            + " \"first_name\"                            character varying NOT NULL,"
            + " \"last_name\"                             character varying(5) NOT NULL,"
            + " \"id\"                                    bigint NOT NULL,"
            + " \"bool_field\"                            boolean,"
            + " \"int64_field\"                           bigint,"
            + " \"float64_field\"                         double precision,"
            + " \"string_field\"                          character varying(76),"
            + " \"bytes_field\"                           bytea,"
            + " \"numeric_field\"                         numeric,"
            + " \"timestamp_field\"                       timestamp with time zone,"
            + " \"date_field\"                            date,"
            + " \"arr_bool_field\"                        boolean[],"
            + " \"arr_int64_field\"                       bigint[],"
            + " \"arr_float64_field\"                     double precision[],"
            + " \"arr_string_field\"                      character varying(15)[],"
            + " \"arr_bytes_field\"                       bytea[],"
            + " \"arr_timestamp_field\"                   timestamp with time zone[],"
            + " \"arr_date_field\"                        date[],"
            + " \"arr_numeric_field\"                     numeric[],"
            + " PRIMARY KEY (\"first_name\", \"last_name\", \"id\")"
            + " )";

    spannerServer.createPgDatabase(dbId, Collections.singleton(allTypes));
    Ddl ddl = getPgDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("alltypes"), notNullValue());
    assertThat(ddl.table("aLlTYPeS"), notNullValue());

    Table table = ddl.table("alltypes");
    assertThat(table.columns(), hasSize(19));

    // Check case sensitiveness.
    assertThat(table.column("first_name"), notNullValue());
    assertThat(table.column("fIrst_NaME"), notNullValue());
    assertThat(table.column("last_name"), notNullValue());
    assertThat(table.column("LAST_name"), notNullValue());

    // Check types/sizes.
    assertThat(table.column("bool_field").type(), equalTo(Type.pgBool()));
    assertThat(table.column("int64_field").type(), equalTo(Type.pgInt8()));
    assertThat(table.column("float64_field").type(), equalTo(Type.pgFloat8()));
    assertThat(table.column("string_field").type(), equalTo(Type.pgVarchar()));
    assertThat(table.column("string_field").size(), equalTo(76));
    assertThat(table.column("bytes_field").type(), equalTo(Type.pgBytea()));
    assertThat(table.column("timestamp_field").type(), equalTo(Type.pgTimestamptz()));
    assertThat(table.column("numeric_field").type(), equalTo(Type.pgNumeric()));
    assertThat(table.column("date_field").type(), equalTo(Type.pgDate()));
    assertThat(table.column("arr_bool_field").type(), equalTo(Type.pgArray(Type.pgBool())));
    assertThat(table.column("arr_int64_field").type(), equalTo(Type.pgArray(Type.pgInt8())));
    assertThat(table.column("arr_float64_field").type(), equalTo(Type.pgArray(Type.pgFloat8())));
    assertThat(table.column("arr_string_field").type(), equalTo(Type.pgArray(Type.pgVarchar())));
    assertThat(table.column("arr_string_field").size(), equalTo(15));
    assertThat(table.column("arr_bytes_field").type(), equalTo(Type.pgArray(Type.pgBytea())));
    assertThat(
        table.column("arr_timestamp_field").type(), equalTo(Type.pgArray(Type.pgTimestamptz())));
    assertThat(table.column("arr_date_field").type(), equalTo(Type.pgArray(Type.pgDate())));
    assertThat(table.column("arr_numeric_field").type(), equalTo(Type.pgArray(Type.pgNumeric())));

    // Check not-null. Primary keys are implicitly forced to be not-null.
    assertThat(table.column("first_name").notNull(), is(true));
    assertThat(table.column("last_name").notNull(), is(true));
    assertThat(table.column("id").notNull(), is(true));

    // Check primary key.
    assertThat(table.primaryKeys(), hasSize(3));
    List<IndexColumn> pk = table.primaryKeys();
    assertThat(pk.get(0).name(), equalTo("first_name"));
    assertThat(pk.get(1).name(), equalTo("last_name"));
    assertThat(pk.get(2).name(), equalTo("id"));

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
  public void pgSimpleView() throws Exception {
    String tableDef =
        "CREATE TABLE \"Users\" ("
            + " id bigint NOT NULL,"
            + " name character varying,"
            + " PRIMARY KEY (id)) ";
    String viewDef = "CREATE VIEW \"Names\" SQL SECURITY INVOKER AS SELECT name FROM \"Users\"";

    spannerServer.createPgDatabase(dbId, Arrays.asList(tableDef, viewDef));
    Ddl ddl = getPgDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("Users"), notNullValue());
    assertThat(ddl.table("uSers"), notNullValue());

    assertThat(ddl.views(), hasSize(1));
    View view = ddl.view("Names");
    assertThat(view, notNullValue());
    assertThat(ddl.view("nAmes"), sameInstance(view));

    assertThat(view.query(), equalTo("SELECT name FROM \"Users\""));
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
  public void pgInterleavedIn() throws Exception {
    List<String> statements =
        Arrays.asList(
            " CREATE TABLE level0 ("
                + " id0                                   bigint NOT NULL,"
                + " val0                                  character varying,"
                + " PRIMARY KEY (id0)"
                + " )",
            " CREATE TABLE level1 ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " val1                                  character varying,"
                + " PRIMARY KEY (id0, id1)"
                + " ) INTERLEAVE IN PARENT level0",
            " CREATE TABLE level2 ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " id2                                   bigint NOT NULL,"
                + " val2                                  character varying,"
                + " PRIMARY KEY (id0, id1, id2)"
                + " ) INTERLEAVE IN PARENT level1",
            " CREATE TABLE level2_1 ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " id2_1                                 bigint NOT NULL,"
                + " val2                                  character varying,"
                + " PRIMARY KEY (id0, id1, id2_1)"
                + " ) INTERLEAVE IN PARENT level1 ON DELETE CASCADE");

    spannerServer.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();

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
    assertThat(ddl.table("level1").interleaveInParent(), equalTo("level0"));
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
  public void pgReserved() throws Exception {
    String statement =
        "CREATE TABLE \"where\" ("
            + " \"JOIN\"                                  character varying NOT NULL,"
            + " \"TABLE\"                                 bigint,"
            + " \"NULL\"                                  bigint NOT NULL,"
            + " PRIMARY KEY (\"NULL\")"
            + " )";

    spannerServer.createPgDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getPgDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));

    assertThat(ddl.table("where"), notNullValue());
    Table table = ddl.table("where");
    assertThat(table.column("JOIN"), notNullValue());
    assertThat(table.column("Table"), notNullValue());
    assertThat(table.column("NULL"), notNullValue());

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
  public void pgIndexes() throws Exception {
    // Prefix indexes to ensure ordering.
    // Unique index is implicitly null-filtered.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"AGE\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE UNIQUE INDEX \"a_last_name_idx\" ON  \"Users\"(\"last_name\" ASC) INCLUDE"
                + " (\"first_name\") WHERE first_name IS NOT NULL AND last_name IS NOT"
                + " NULL",
            " CREATE INDEX \"b_age_idx\" ON \"Users\"(\"id\" ASC, \"AGE\" DESC) INTERLEAVE IN"
                + " \"Users\" WHERE \"AGE\" IS NOT NULL",
            " CREATE UNIQUE INDEX \"c_first_name_idx\" ON \"Users\"(\"first_name\" ASC) WHERE"
                + " first_name IS NOT NULL",
            " CREATE INDEX \"null_ordering_idx\" ON \"Users\"(\"id\" ASC NULLS FIRST,"
                + " \"first_name\" ASC, \"last_name\" DESC, \"AGE\" DESC NULLS LAST)");

    spannerServer.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
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

  @Test
  public void pgForeignKeys() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"Ref\" ("
                + " \"id1\"                               bigint NOT NULL,"
                + " \"id2\"                               bigint NOT NULL,"
                + " PRIMARY KEY (\"id1\", \"id2\")"
                + " )",
            " CREATE TABLE \"Tab\" ("
                + " \"key\"                               bigint NOT NULL,"
                + " \"id1\"                               bigint NOT NULL,"
                + " \"id2\"                               bigint NOT NULL,"
                + " PRIMARY KEY (\"key\")"
                + " )",
            " ALTER TABLE \"Tab\" ADD CONSTRAINT \"fk\" FOREIGN KEY (\"id1\", \"id2\")"
                + " REFERENCES \"Ref\" (\"id2\", \"id1\")");

    spannerServer.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
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
  public void pgCheckConstraints() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"T\" ("
                + " \"id\"     bigint NOT NULL,"
                + " \"A\"      bigint NOT NULL,"
                + " CONSTRAINT \"ck\" CHECK ((\"A\" > '0'::bigint)),"
                + " PRIMARY KEY (\"id\")"
                + " )");

    spannerServer.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
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
  public void pgGeneratedColumns() throws Exception {
    String statement =
        "CREATE TABLE \"T\" ( \"id\"                     bigint NOT NULL,"
            + " \"generated\" bigint NOT NULL GENERATED ALWAYS AS ((id / '1'::bigint)) STORED, "
            + " PRIMARY KEY (\"id\") )";

    spannerServer.createPgDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void pgDefaultColumns() throws Exception {
    String statement =
        "CREATE TABLE \"T\" ( \"id\"                       bigint NOT NULL,"
            + " \"generated\"                              bigint NOT NULL DEFAULT '10'::bigint,"
            + " PRIMARY KEY (\"id\") )";

    spannerServer.createPgDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getPgDatabaseDdl();
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

  @Test
  public void pgDatabaseOptions() throws Exception {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE \"" + dbId + "\" SET spanner.version_retention_period = '5d'\n",
            "CREATE TABLE \"Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"age\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " ) ",
            " CREATE INDEX \"a_last_name_idx\" ON "
                + " \"Users\"(\"last_name\" ASC) INCLUDE (\"first_name\")",
            " CREATE INDEX \"b_age_idx\" ON \"Users\"(\"age\" DESC)",
            " CREATE INDEX \"c_first_name_idx\" ON \"Users\"(\"first_name\" ASC)");

    spannerServer.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
    String alterStatement = statements.get(0);
    statements.set(0, alterStatement.replace(dbId, "%db_name%"));
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void changeStreams() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `Account` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `balanceId`                             INT64 NOT NULL,"
                + " `balance`                               FLOAT64 NOT NULL,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE TABLE `Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE CHANGE STREAM `ChangeStreamAll` FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")",
            " CREATE CHANGE STREAM `ChangeStreamEmpty`" + " OPTIONS (retention_period=\"24h\")",
            " CREATE CHANGE STREAM `ChangeStreamKeyColumns` FOR `Account`(), `Users`()",
            " CREATE CHANGE STREAM `ChangeStreamTableColumns`"
                + " FOR `Account`, `Users`(`first_name`, `last_name`)");

    spannerServer.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }
}
