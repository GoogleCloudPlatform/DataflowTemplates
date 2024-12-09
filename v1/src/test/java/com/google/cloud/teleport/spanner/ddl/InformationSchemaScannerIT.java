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
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.spanner.IntegrationTest;
import com.google.cloud.teleport.spanner.SpannerServerResource;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.common.collect.HashMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
  public static final String INSTANCE_PARTITION_ID = "mr-partition";

  /** Class rule for Spanner server resource. */
  @ClassRule public static final SpannerServerResource SPANNER_SERVER = new SpannerServerResource();

  @Before
  public void setup() {
    // Just to make sure an old database is not left over.
    SPANNER_SERVER.dropDatabase(dbId);
  }

  @After
  public void tearDown() {
    // Drop database before deleting the partition.
    SPANNER_SERVER.dropDatabase(dbId);
  }

  @BeforeClass
  public static void setupInstancePartition() throws Exception {
    SPANNER_SERVER.createInstancePartition(INSTANCE_PARTITION_ID, "nam3");
  }

  @AfterClass
  public static void tearDownInstancePartition() throws Exception {
    SPANNER_SERVER.deleteInstancePartition(INSTANCE_PARTITION_ID);
  }

  private Ddl getDatabaseDdl() {
    BatchClient batchClient = SPANNER_SERVER.getBatchClient(dbId);
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());

    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx);
    return scanner.scan();
  }

  private Ddl getPgDatabaseDdl() {
    BatchClient batchClient = SPANNER_SERVER.getBatchClient(dbId);
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx, Dialect.POSTGRESQL);
    return scanner.scan();
  }

  @Test
  public void emptyDatabase() throws Exception {
    SPANNER_SERVER.createDatabase(dbId, Collections.emptyList());
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl, equalTo(Ddl.builder().build()));
  }

  @Test
  public void pgEmptyDatabase() throws Exception {
    SPANNER_SERVER.createPgDatabase(dbId, Collections.emptyList());
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl, equalTo(Ddl.builder(Dialect.POSTGRESQL).build()));
  }

  @Test
  public void tableWithAllTypes() throws Exception {
    String createProtoBundleStmt =
        "CREATE PROTO BUNDLE ("
            + "\n\t`com.google.cloud.teleport.spanner.tests.TestMessage`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.Order.PaymentMode`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.Order.Item`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.Order.Address`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.Order`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.TestEnum`,"
            + "\n\t`com.google.cloud.teleport.spanner.tests.OrderHistory`,)";

    String createTableStmt =
        "CREATE TABLE `alltypes` ("
            + " `first_name`            STRING(MAX),"
            + " `last_name`             STRING(5),"
            + " `id`                    INT64 NOT NULL,"
            + " `bool_field`            BOOL,"
            + " `int64_field`           INT64,"
            + " `float32_field`         FLOAT32,"
            + " `float64_field`         FLOAT64,"
            + " `string_field`          STRING(76),"
            + " `bytes_field`           BYTES(13),"
            + " `timestamp_field`       TIMESTAMP,"
            + " `date_field`            DATE,"
            + " `proto_field`           `com.google.cloud.teleport.spanner.tests.TestMessage`,"
            + " `proto_field_2`         `com.google.cloud.teleport.spanner.tests.Order`,"
            + " `nested_enum`           `com.google.cloud.teleport.spanner.tests.Order.PaymentMode`,"
            + " `enum_field`            `com.google.cloud.teleport.spanner.tests.TestEnum`,"
            + " `arr_bool_field`        ARRAY<BOOL>,"
            + " `arr_int64_field`       ARRAY<INT64>,"
            + " `arr_float32_field`     ARRAY<FLOAT32>,"
            + " `arr_float64_field`     ARRAY<FLOAT64>,"
            + " `arr_string_field`      ARRAY<STRING(15)>,"
            + " `arr_bytes_field`       ARRAY<BYTES(MAX)>,"
            + " `arr_timestamp_field`   ARRAY<TIMESTAMP>,"
            + " `arr_date_field`        ARRAY<DATE>,"
            + " `embedding_vector`      ARRAY<FLOAT64>(vector_length=>16),"
            + " `arr_proto_field`       ARRAY<`com.google.cloud.teleport.spanner.tests.TestMessage`>,"
            + " `arr_proto_field_2`     ARRAY<`com.google.cloud.teleport.spanner.tests.Order`>,"
            + " `arr_nested_enum`       ARRAY<`com.google.cloud.teleport.spanner.tests.Order.PaymentMode`>,"
            + " `arr_enum_field`        ARRAY<`com.google.cloud.teleport.spanner.tests.TestEnum`>,"
            + " `hidden_column`         STRING(MAX) HIDDEN,"
            + " ) PRIMARY KEY (`first_name` ASC, `last_name` DESC, `id` ASC)";

    FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
    fileDescriptorSetBuilder.addFile(
        com.google.cloud.teleport.spanner.tests.TestMessage.getDescriptor().getFile().toProto());
    ByteString protoDescriptorBytes = fileDescriptorSetBuilder.build().toByteString();

    List<String> statements = new ArrayList<>();
    statements.add(createProtoBundleStmt);
    statements.add(createTableStmt);
    SPANNER_SERVER.createDatabase(dbId, statements, protoDescriptorBytes);
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("alltypes"), notNullValue());
    assertThat(ddl.table("aLlTYPeS"), notNullValue());

    Table table = ddl.table("alltypes");
    assertThat(table.columns(), hasSize(29));

    // Check case sensitiveness.
    assertThat(table.column("first_name"), notNullValue());
    assertThat(table.column("fIrst_NaME"), notNullValue());
    assertThat(table.column("last_name"), notNullValue());
    assertThat(table.column("LAST_name"), notNullValue());

    // Check types/sizes.
    assertThat(table.column("bool_field").type(), equalTo(Type.bool()));
    assertThat(table.column("int64_field").type(), equalTo(Type.int64()));
    assertThat(table.column("float32_field").type(), equalTo(Type.float32()));
    assertThat(table.column("float64_field").type(), equalTo(Type.float64()));
    assertThat(table.column("string_field").type(), equalTo(Type.string()));
    assertThat(table.column("string_field").size(), equalTo(76));
    assertThat(table.column("bytes_field").type(), equalTo(Type.bytes()));
    assertThat(table.column("bytes_field").size(), equalTo(13));
    assertThat(table.column("timestamp_field").type(), equalTo(Type.timestamp()));
    assertThat(table.column("date_field").type(), equalTo(Type.date()));
    assertThat(
        table.column("proto_field").type(),
        equalTo(Type.proto("com.google.cloud.teleport.spanner.tests.TestMessage")));
    assertThat(
        table.column("proto_field_2").type(),
        equalTo(Type.proto("com.google.cloud.teleport.spanner.tests.Order")));
    assertThat(
        table.column("nested_enum").type(),
        equalTo(Type.protoEnum("com.google.cloud.teleport.spanner.tests.Order.PaymentMode")));
    assertThat(
        table.column("enum_field").type(),
        equalTo(Type.protoEnum("com.google.cloud.teleport.spanner.tests.TestEnum")));
    assertThat(table.column("arr_bool_field").type(), equalTo(Type.array(Type.bool())));
    assertThat(table.column("arr_int64_field").type(), equalTo(Type.array(Type.int64())));
    assertThat(table.column("arr_float32_field").type(), equalTo(Type.array(Type.float32())));
    assertThat(table.column("arr_float64_field").type(), equalTo(Type.array(Type.float64())));
    assertThat(table.column("arr_string_field").type(), equalTo(Type.array(Type.string())));
    assertThat(table.column("arr_string_field").size(), equalTo(15));
    assertThat(table.column("arr_bytes_field").type(), equalTo(Type.array(Type.bytes())));
    assertThat(table.column("arr_bytes_field").size(), equalTo(-1 /*max*/));
    assertThat(table.column("arr_timestamp_field").type(), equalTo(Type.array(Type.timestamp())));
    assertThat(table.column("arr_date_field").type(), equalTo(Type.array(Type.date())));
    assertThat(table.column("embedding_vector").type(), equalTo(Type.array(Type.float64())));
    assertThat(table.column("embedding_vector").arrayLength(), equalTo(16));
    assertThat(
        table.column("arr_proto_field").type(),
        equalTo(Type.array(Type.proto("com.google.cloud.teleport.spanner.tests.TestMessage"))));
    assertThat(
        table.column("arr_proto_field_2").type(),
        equalTo(Type.array(Type.proto("com.google.cloud.teleport.spanner.tests.Order"))));
    assertThat(
        table.column("arr_nested_enum").type(),
        equalTo(
            Type.array(
                Type.protoEnum("com.google.cloud.teleport.spanner.tests.Order.PaymentMode"))));
    assertThat(
        table.column("arr_enum_field").type(),
        equalTo(Type.array(Type.protoEnum("com.google.cloud.teleport.spanner.tests.TestEnum"))));
    assertThat(table.column("hidden_column").type(), equalTo(Type.string()));
    assertThat(table.column("hidden_column").isHidden(), is(true));

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
    assertThat(
        ddl.prettyPrint(), equalToCompressingWhiteSpace(createProtoBundleStmt + createTableStmt));
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
            + " \"float32_field\"                         real,"
            + " \"float64_field\"                         double precision,"
            + " \"string_field\"                          character varying(76),"
            + " \"bytes_field\"                           bytea,"
            + " \"numeric_field\"                         numeric,"
            + " \"timestamp_field\"                       timestamp with time zone,"
            + " \"date_field\"                            date,"
            + " \"arr_bool_field\"                        boolean[],"
            + " \"arr_int64_field\"                       bigint[],"
            + " \"arr_float32_field\"                     real[],"
            + " \"arr_float64_field\"                     double precision[],"
            + " \"arr_string_field\"                      character varying(15)[],"
            + " \"arr_bytes_field\"                       bytea[],"
            + " \"arr_timestamp_field\"                   timestamp with time zone[],"
            + " \"arr_date_field\"                        date[],"
            + " \"arr_numeric_field\"                     numeric[],"
            + " \"embedding_vector\"                      double precision[] vector length 8,"
            + " PRIMARY KEY (\"first_name\", \"last_name\", \"id\")"
            + " )";

    SPANNER_SERVER.createPgDatabase(dbId, Collections.singleton(allTypes));
    Ddl ddl = getPgDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(1));
    assertThat(ddl.table("alltypes"), notNullValue());
    assertThat(ddl.table("aLlTYPeS"), notNullValue());

    Table table = ddl.table("alltypes");
    assertThat(table.columns(), hasSize(20));

    // Check case sensitiveness.
    assertThat(table.column("first_name"), notNullValue());
    assertThat(table.column("fIrst_NaME"), notNullValue());
    assertThat(table.column("last_name"), notNullValue());
    assertThat(table.column("LAST_name"), notNullValue());

    // Check types/sizes.
    assertThat(table.column("bool_field").type(), equalTo(Type.pgBool()));
    assertThat(table.column("int64_field").type(), equalTo(Type.pgInt8()));
    assertThat(table.column("float32_field").type(), equalTo(Type.pgFloat4()));
    assertThat(table.column("float64_field").type(), equalTo(Type.pgFloat8()));
    assertThat(table.column("string_field").type(), equalTo(Type.pgVarchar()));
    assertThat(table.column("string_field").size(), equalTo(76));
    assertThat(table.column("bytes_field").type(), equalTo(Type.pgBytea()));
    assertThat(table.column("timestamp_field").type(), equalTo(Type.pgTimestamptz()));
    assertThat(table.column("numeric_field").type(), equalTo(Type.pgNumeric()));
    assertThat(table.column("date_field").type(), equalTo(Type.pgDate()));
    assertThat(table.column("arr_bool_field").type(), equalTo(Type.pgArray(Type.pgBool())));
    assertThat(table.column("arr_int64_field").type(), equalTo(Type.pgArray(Type.pgInt8())));
    assertThat(table.column("arr_float32_field").type(), equalTo(Type.pgArray(Type.pgFloat4())));
    assertThat(table.column("arr_float64_field").type(), equalTo(Type.pgArray(Type.pgFloat8())));
    assertThat(table.column("arr_string_field").type(), equalTo(Type.pgArray(Type.pgVarchar())));
    assertThat(table.column("arr_string_field").size(), equalTo(15));
    assertThat(table.column("arr_bytes_field").type(), equalTo(Type.pgArray(Type.pgBytea())));
    assertThat(
        table.column("arr_timestamp_field").type(), equalTo(Type.pgArray(Type.pgTimestamptz())));
    assertThat(table.column("arr_date_field").type(), equalTo(Type.pgArray(Type.pgDate())));
    assertThat(table.column("arr_numeric_field").type(), equalTo(Type.pgArray(Type.pgNumeric())));
    assertThat(table.column("embedding_vector").type(), equalTo(Type.pgArray(Type.pgFloat8())));
    assertThat(table.column("embedding_vector").arrayLength(), equalTo(8));

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
  public void simpleModel() throws Exception {
    String modelDef =
        "CREATE MODEL `Iris` INPUT ( `f1` FLOAT64, `f2` FLOAT64, `f3` FLOAT64, `f4` FLOAT64, )"
            + " OUTPUT ( `classes` ARRAY<STRING(MAX)>, `scores` ARRAY<FLOAT64>, ) REMOTE OPTIONS"
            + " (endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/endpoints/4608339105032437760\")";

    SPANNER_SERVER.createDatabase(dbId, Arrays.asList(modelDef));
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.models(), hasSize(1));
    Model model = ddl.model("Iris");
    assertThat(model, notNullValue());
    assertThat(ddl.model("iriS"), sameInstance(model));
    assertThat(model.inputColumns(), hasSize(4));
    assertThat(model.inputColumns().get(0).name(), is("f1"));
    assertThat(model.inputColumns().get(0).type(), is(Type.float64()));
    assertThat(model.inputColumns().get(0).columnOptions(), hasSize(1));
    assertThat(model.inputColumns().get(0).columnOptions(), hasItems("required=TRUE"));
    assertThat(model.outputColumns(), hasSize(2));
    assertThat(model.remote(), equalTo(true));
    assertThat(
        model.options(),
        hasItems(
            "endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/endpoints/4608339105032437760\""));

    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(modelDef));
  }

  @Test
  public void simplePropertyGraph() throws Exception {
    String nodeTableDef =
        "CREATE TABLE NodeTest (\n" + "  Id INT64 NOT NULL,\n" + ") PRIMARY KEY(Id)";
    String edgeTableDef =
        "CREATE TABLE EdgeTest (\n"
            + "FromId INT64 NOT NULL,\n"
            + "ToId INT64 NOT NULL,\n"
            + ") PRIMARY KEY(FromId, ToId)";
    String propertyGraphDef =
        "CREATE PROPERTY GRAPH testGraph\n"
            + "  NODE TABLES(\n"
            + "    NodeTest\n"
            + "      KEY(Id)\n"
            + "      LABEL Test PROPERTIES(\n"
            + "        Id))"
            + "  EDGE TABLES(\n"
            + "    EdgeTest\n"
            + "      KEY(FromId, ToId)\n"
            + "      SOURCE KEY(FromId) REFERENCES NodeTest(Id)\n"
            + "      DESTINATION KEY(ToId) REFERENCES NodeTest(Id)\n"
            + "      DEFAULT LABEL PROPERTIES ALL COLUMNS)";

    SPANNER_SERVER.createDatabase(
        dbId, Arrays.asList(nodeTableDef, edgeTableDef, propertyGraphDef));
    Ddl ddl = getDatabaseDdl();

    assertThat(ddl.allTables(), hasSize(2));
    assertThat(ddl.table("NodeTest"), notNullValue());
    assertThat(ddl.propertyGraphs(), hasSize(1));

    PropertyGraph testGraph = ddl.propertyGraph("testGraph");

    assertEquals(testGraph.name(), "testGraph");
    assertThat(testGraph.propertyDeclarations(), hasSize(3));
    assertThat(testGraph.getPropertyDeclaration("Id"), notNullValue());
    assertThat(testGraph.getPropertyDeclaration("FromId"), notNullValue());
    assertThat(testGraph.getPropertyDeclaration("ToId"), notNullValue());

    assertThat(testGraph.labels(), hasSize(2));
    assertThat(testGraph.getLabel("Test"), notNullValue());
    assertThat(testGraph.getLabel("EdgeTest"), notNullValue());

    assertThat(testGraph.nodeTables(), hasSize(1));
    assertThat(testGraph.getNodeTable("NodeTest"), notNullValue());

    assertThat(testGraph.edgeTables(), hasSize(1));
    assertThat(testGraph.getEdgeTable("EdgeTest"), notNullValue());

    // --- Assertions for Node Table ---
    GraphElementTable nodeTestTable = testGraph.getNodeTable("NodeTest");
    assertThat(nodeTestTable, notNullValue());
    assertThat(nodeTestTable.name(), equalTo("NodeTest"));
    assertThat(nodeTestTable.baseTableName(), equalTo("NodeTest"));
    assertThat(nodeTestTable.kind(), equalTo(GraphElementTable.Kind.NODE));
    assertIterableEquals(List.of("Id"), nodeTestTable.keyColumns());

    assertThat(nodeTestTable.labelToPropertyDefinitions(), hasSize(1));
    GraphElementTable.LabelToPropertyDefinitions nodeTestLabel =
        nodeTestTable.getLabelToPropertyDefinitions("Test");
    assertThat(nodeTestLabel, notNullValue());
    assertThat(nodeTestLabel.labelName, equalTo("Test"));
    assertThat(nodeTestLabel.propertyDefinitions(), hasSize(1));
    GraphElementTable.PropertyDefinition nodeTestIdProperty =
        nodeTestLabel.getPropertyDefinition("Id");
    assertThat(nodeTestIdProperty, notNullValue());
    assertThat(nodeTestIdProperty.name, equalTo("Id"));
    assertThat(nodeTestIdProperty.valueExpressionString, equalTo("Id"));

    // --- Assertions for Edge Table ---
    GraphElementTable edgeTestTable = testGraph.getEdgeTable("EdgeTest");
    assertThat(edgeTestTable, notNullValue());
    assertThat(edgeTestTable.name(), equalTo("EdgeTest"));
    assertThat(edgeTestTable.baseTableName(), equalTo("EdgeTest"));
    assertThat(edgeTestTable.kind(), equalTo(GraphElementTable.Kind.EDGE));
    assertIterableEquals(List.of("FromId", "ToId"), edgeTestTable.keyColumns());

    assertThat(edgeTestTable.labelToPropertyDefinitions(), hasSize(1));
    GraphElementTable.LabelToPropertyDefinitions edgeTestLabel =
        edgeTestTable.getLabelToPropertyDefinitions("EdgeTest");
    assertThat(edgeTestLabel, notNullValue());
    assertThat(edgeTestLabel.labelName, equalTo("EdgeTest"));
    assertThat(edgeTestLabel.propertyDefinitions(), hasSize(2)); // FromId and ToId

    GraphElementTable.PropertyDefinition edgeTestFromIdProperty =
        edgeTestLabel.getPropertyDefinition("FromId");
    assertThat(edgeTestFromIdProperty, notNullValue());
    assertThat(edgeTestFromIdProperty.name, equalTo("FromId"));
    assertThat(edgeTestFromIdProperty.valueExpressionString, equalTo("FromId"));

    GraphElementTable.PropertyDefinition edgeTestToIdProperty =
        edgeTestLabel.getPropertyDefinition("ToId");
    assertThat(edgeTestToIdProperty, notNullValue());
    assertThat(edgeTestToIdProperty.name, equalTo("ToId"));
    assertThat(edgeTestToIdProperty.valueExpressionString, equalTo("ToId"));

    // --- Assertions for Edge Table References ---
    assertThat(edgeTestTable.sourceNodeTable().nodeTableName, equalTo("NodeTest"));
    assertIterableEquals(List.of("Id"), edgeTestTable.sourceNodeTable().nodeKeyColumns);

    assertIterableEquals(List.of("FromId"), edgeTestTable.sourceNodeTable().edgeKeyColumns);

    assertThat(edgeTestTable.targetNodeTable().nodeTableName, equalTo("NodeTest"));
    assertIterableEquals(List.of("Id"), edgeTestTable.targetNodeTable().nodeKeyColumns);
    assertIterableEquals(List.of("ToId"), edgeTestTable.targetNodeTable().edgeKeyColumns);
  }

  @Test
  public void complexPropertyGraph() throws Exception {}

  @Test
  public void simpleView() throws Exception {
    String tableDef =
        "CREATE TABLE Users ("
            + " id INT64 NOT NULL,"
            + " name STRING(MAX),"
            + ") PRIMARY KEY (id)";
    String viewDef = "CREATE VIEW Names SQL SECURITY INVOKER AS SELECT u.name FROM Users u";

    SPANNER_SERVER.createDatabase(dbId, Arrays.asList(tableDef, viewDef));
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

    SPANNER_SERVER.createPgDatabase(dbId, Arrays.asList(tableDef, viewDef));
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

    SPANNER_SERVER.createDatabase(dbId, statements);
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

    SPANNER_SERVER.createPgDatabase(dbId, statements);
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

    SPANNER_SERVER.createDatabase(dbId, Collections.singleton(statement));
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

    SPANNER_SERVER.createPgDatabase(dbId, Collections.singleton(statement));
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
            " CREATE INDEX `b_age_idx` ON `Users`(`age` DESC) WHERE age IS NOT NULL",
            " CREATE UNIQUE INDEX `c_first_name_idx` ON `Users`(`first_name` ASC)");

    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void searchIndexes() throws Exception {
    // Prefix indexes to ensure ordering.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `Users` ("
                + "  `UserId`                                INT64 NOT NULL,"
                + " ) PRIMARY KEY (`UserId` ASC)",
            " CREATE TABLE `Messages` ("
                + "  `UserId`                                INT64 NOT NULL,"
                + "  `MessageId`                             INT64 NOT NULL,"
                + "  `Subject`                               STRING(MAX),"
                + "  `Subject_Tokens`                        TOKENLIST AS (TOKENIZE_FULLTEXT(`Subject`)) HIDDEN,"
                + "  `Body`                                  STRING(MAX),"
                + "  `Body_Tokens`                           TOKENLIST AS (TOKENIZE_FULLTEXT(`Body`)) HIDDEN,"
                + "  `Data`                                  STRING(MAX),"
                + " ) PRIMARY KEY (`UserId` ASC, `MessageId` ASC), INTERLEAVE IN PARENT `Users`",
            " CREATE SEARCH INDEX `SearchIndex` ON `Messages`(`Subject_Tokens` , `Body_Tokens` )"
                + " STORING (`Data`)"
                + " PARTITION BY `UserId`,"
                + " INTERLEAVE IN `Users`"
                + " OPTIONS (sort_order_sharding=TRUE)");

    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void vectorIndexes() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `Base` ("
                + " `K`                                     INT64,"
                + " `V`                                     INT64,"
                + " `Embeddings`                            ARRAY<FLOAT32>(vector_length=>128),"
                + " ) PRIMARY KEY (`K` ASC)",
            " CREATE VECTOR INDEX `VI` ON `Base`(`Embeddings` ) WHERE Embeddings IS NOT NULL"
                + " OPTIONS (distance_type=\"COSINE\")");

    SPANNER_SERVER.createDatabase(dbId, statements);
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

    SPANNER_SERVER.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void foreignKeys() throws Exception {
    List<String> dbCreationStatements =
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
                + " REFERENCES `Ref` (`id2`, `id1`)",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_2` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) ON DELETE CASCADE ENFORCED",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_3` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) NOT ENFORCED",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_4` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED");

    List<String> dbVerificationStatements =
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
                // Unspecified DELETE action defaults to "NO ACTION"
                + " REFERENCES `Ref` (`id2`, `id1`) ON DELETE NO ACTION",
            // "ENFORCED" keyword is dropped.
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_2` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) ON DELETE CASCADE",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_3` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED",
            " ALTER TABLE `Tab` ADD CONSTRAINT `fk_4` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED");

    SPANNER_SERVER.createDatabase(dbId, dbCreationStatements);
    Ddl ddl = getDatabaseDdl();
    assertThat(
        ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", dbVerificationStatements)));
  }

  @Test
  public void pgForeignKeys() throws Exception {
    List<String> dbCreationStatements =
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
    List<String> dbVerificationStatements =
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
                // Unspecified DELETE action defaults to "NO ACTION"
                + " REFERENCES \"Ref\" (\"id2\", \"id1\") ON DELETE NO ACTION");

    SPANNER_SERVER.createPgDatabase(dbId, dbCreationStatements);
    Ddl ddl = getPgDatabaseDdl();
    assertThat(
        ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", dbVerificationStatements)));
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

    SPANNER_SERVER.createDatabase(dbId, statements);
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

    SPANNER_SERVER.createPgDatabase(dbId, statements);
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

    SPANNER_SERVER.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void pgCommitTimestamp() throws Exception {
    String statement =
        "CREATE TABLE \"Users\" ("
            + " \"id\"                                    bigint NOT NULL,"
            + " \"birthday\"                              spanner.commit_timestamp NOT NULL,"
            + " PRIMARY KEY (\"id\") )";

    SPANNER_SERVER.createPgDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getPgDatabaseDdl();
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

    SPANNER_SERVER.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void pgGeneratedColumns() throws Exception {
    String statement =
        "CREATE TABLE \"T\" ( \"id\"                     bigint NOT NULL,"
            + " \"generated\" bigint NOT NULL GENERATED ALWAYS AS ((id / '1'::bigint)) STORED, "
            + " PRIMARY KEY (\"id\") )";

    SPANNER_SERVER.createPgDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void defaultColumns() throws Exception {
    String statement =
        "CREATE TABLE `T` ("
            + " `id`                                     INT64 NOT NULL,"
            + " `generated`                              INT64 NOT NULL DEFAULT (10), "
            + " ) PRIMARY KEY (`id` ASC)";

    SPANNER_SERVER.createDatabase(dbId, Collections.singleton(statement));
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(statement));
  }

  @Test
  public void pgDefaultColumns() throws Exception {
    String statement =
        "CREATE TABLE \"T\" ( \"id\"                       bigint NOT NULL,"
            + " \"generated\"                              bigint NOT NULL DEFAULT '10'::bigint,"
            + " PRIMARY KEY (\"id\") )";

    SPANNER_SERVER.createPgDatabase(dbId, Collections.singleton(statement));
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

    SPANNER_SERVER.createDatabase(dbId, statements);
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

    SPANNER_SERVER.createPgDatabase(dbId, statements);
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
            " CREATE CHANGE STREAM `ChangeStreamEmpty` OPTIONS (retention_period=\"24h\")",
            " CREATE CHANGE STREAM `ChangeStreamKeyColumns` FOR `Account`(), `Users`()",
            " CREATE CHANGE STREAM `ChangeStreamTableColumns`"
                + " FOR `Account`, `Users`(`first_name`, `last_name`)");

    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  // TODO: Enable the test once change streams are supported in PG.
  // @Test
  public void pgChangeStreams() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"Account\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"balanceId\"                             bigint NOT NULL,"
                + " \"balance\"                               double precision NOT NULL,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE TABLE \"Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"age\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE CHANGE STREAM \"ChangeStreamAll\" FOR ALL"
                + " WITH (retention_period='7d', value_capture_type='OLD_AND_NEW_VALUES')",
            " CREATE CHANGE STREAM \"ChangeStreamEmpty\" WITH (retention_period='24h')",
            " CREATE CHANGE STREAM \"ChangeStreamKeyColumns\" FOR \"Account\"(), \"Users\"()",
            " CREATE CHANGE STREAM \"ChangeStreamTableColumns\""
                + " FOR \"Account\", \"Users\"(\"first_name\", \"last_name\")");

    SPANNER_SERVER.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  @Test
  public void sequences() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE SEQUENCE `MySequence` OPTIONS (" + "sequence_kind = \"bit_reversed_positive\")",
            "CREATE SEQUENCE `MySequence2` OPTIONS ("
                + "sequence_kind = \"bit_reversed_positive\","
                + "skip_range_min = 1,"
                + "skip_range_max = 1000,"
                + "start_with_counter = 100)",
            "CREATE TABLE `Account` ("
                + " `id`        INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),"
                + " `balanceId` INT64 NOT NULL,"
                + " ) PRIMARY KEY (`id` ASC)");

    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    String expectedDdl =
        "\nCREATE SEQUENCE `MySequence`\n\tOPTIONS "
            + "(sequence_kind=\"bit_reversed_positive\")\n"
            + "CREATE SEQUENCE `MySequence2`\n\tOPTIONS "
            + "(sequence_kind=\"bit_reversed_positive\","
            + " skip_range_max=1000,"
            + " skip_range_min=1,"
            + " start_with_counter=100)"
            + "CREATE TABLE `Account` ("
            + "\n\t`id`                                    INT64 DEFAULT"
            + "  (GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySequence)),"
            + "\n\t`balanceId`                             INT64 NOT NULL,"
            + "\n) PRIMARY KEY (`id` ASC)\n\n";
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(expectedDdl));
  }

  @Test
  public void pgSequences() throws Exception {
    List<String> statements =
        Arrays.asList(
            "CREATE SEQUENCE \"MyPGSequence\" BIT_REVERSED_POSITIVE",
            "CREATE SEQUENCE \"MyPGSequence2\" BIT_REVERSED_POSITIVE"
                + " SKIP RANGE 1 1000 START COUNTER WITH 100",
            "CREATE TABLE \"Account\" ("
                + " \"id\"        bigint DEFAULT nextval('\"MyPGSequence\"'),"
                + " \"balanceId\" bigint NOT NULL,"
                + " PRIMARY KEY (\"id\"))");

    SPANNER_SERVER.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
    String expectedDdl =
        "\nCREATE SEQUENCE \"MyPGSequence\" BIT_REVERSED_POSITIVE"
            + " START COUNTER WITH 1"
            + "\nCREATE SEQUENCE \"MyPGSequence2\" BIT_REVERSED_POSITIVE"
            + " SKIP RANGE 1 1000 START COUNTER WITH 100"
            + "CREATE TABLE \"Account\" ("
            + "\n\t\"id\"                                    bigint NOT NULL"
            + " DEFAULT nextval('\"MyPGSequence\"'::text),\n\t"
            + "\"balanceId\"                             bigint NOT NULL,"
            + "\n\tPRIMARY KEY (\"id\")\n)\n\n";
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(expectedDdl));
  }

  @Test
  public void placements() throws Exception {
    List<String> statements =
        // Create placements pointing to MR partition, which uses nam3 config.
        Arrays.asList(
            "ALTER DATABASE `" + dbId + "` SET OPTIONS ( opt_in_dataplacement_preview = TRUE )\n\n",
            "CREATE PLACEMENT `pl1`\n\tOPTIONS (instance_partition=\""
                + INSTANCE_PARTITION_ID
                + "\")\n",
            "CREATE PLACEMENT `pl2`\n\tOPTIONS (default_leader=\"us-east1\", instance_partition=\""
                + INSTANCE_PARTITION_ID
                + "\")\n",
            "CREATE PLACEMENT `pl3`\n\tOPTIONS (default_leader=\"us-east4\", instance_partition=\""
                + INSTANCE_PARTITION_ID
                + "\")");

    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    statements.set(0, statements.get(0).replace(dbId, "%db_name%"));
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  // TODO: Add PG test once placements and placement_options are available.

  // TODO: Re-enable once placement table constraints are available.
  // @Test
  public void placementTables() throws Exception {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE `" + dbId + "` SET OPTIONS ( opt_in_dataplacement_preview = TRUE )\n\n",
            "CREATE TABLE `PlacementKeyAsPrimaryKey` (\n\t"
                + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n\t"
                + "`val`                                   STRING(MAX),\n"
                + ") PRIMARY KEY (`location` ASC)\n\n\n",
            "CREATE TABLE `PlacedUsers` (\n\t"
                + "`location`                              STRING(MAX) NOT NULL,\n\t"
                + "`user_id`                               INT64 NOT NULL,\n"
                + ") PRIMARY KEY (`location` ASC, `user_id` ASC),\n"
                + "INTERLEAVE IN PARENT `PlacementKeyAsPrimaryKey`\n\n\n",
            "CREATE TABLE `UsersByPlacement` (\n\t"
                + "`user_id`                               INT64 NOT NULL,\n\t"
                + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n"
                + ") PRIMARY KEY (`user_id` ASC)\n\n");

    // Validate the PLACEMENT KEY constraint is available in placement tables.
    SPANNER_SERVER.createDatabase(dbId, statements);
    Ddl ddl = getDatabaseDdl();
    statements.set(0, statements.get(0).replace(dbId, "%db_name%"));
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }

  // TODO: Re-enable once placement table constraints are available.
  // @Test
  public void pgPlacementTables() throws Exception {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE \"" + dbId + "\" SET spanner.opt_in_dataplacement_preview = TRUE\n",
            " CREATE TABLE \"PlacementKeyAsPrimaryKey\" ("
                + " \"location\"                              character varying NOT NULL PLACEMENT KEY,"
                + " \"val\"                                   character varying,"
                + " PRIMARY KEY (\"location\")"
                + " )",
            " CREATE TABLE \"PlacedUsers\" ("
                + " \"location\"                              character varying NOT NULL,"
                + " \"user_id\"                               character varying NOT NULL,"
                + " PRIMARY KEY (\"location\", \"user_id\") "
                + ") INTERLEAVE IN PARENT \"PlacementKeyAsPrimaryKey\"",
            " CREATE TABLE \"UsersWithPlacement\" ("
                + " \"user_id\"                               bigint NOT NULL,"
                + " \"location\"                              character varying NOT NULL PLACEMENT KEY,"
                + " PRIMARY KEY (\"user_id\")"
                + " )");
    // Validate the PLACEMENT KEY constraint is available in placement tables.
    SPANNER_SERVER.createPgDatabase(dbId, statements);
    Ddl ddl = getPgDatabaseDdl();
    statements.set(0, statements.get(0).replace(dbId, "%db_name%"));
    assertThat(ddl.prettyPrint(), equalToCompressingWhiteSpace(String.join("", statements)));
  }
}
