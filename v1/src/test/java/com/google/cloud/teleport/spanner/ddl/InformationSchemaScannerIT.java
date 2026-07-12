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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
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
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.spanner.DdlToAvroSchemaConverter;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.tests.TestMessage;
import com.google.common.collect.HashMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test coverage for {@link InformationSchemaScanner}. This requires an active GCP project with a
 * Spanner instance. Hence this test can only be run locally with a project set up using 'gcloud
 * config'.
 *
 * <p>Note: {@link InformationSchemaScanner} and the resulting {@link Ddl} objects are actively used
 * for canonical schema assertions in the end-to-end {@link CopyDbIT} integration test. Testing it
 * comprehensively here is extremely important to complete the loop of testing the Export and Import
 * Pipelines.
 */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
public class InformationSchemaScannerIT extends SpannerTemplateITBase {

  public static final String INSTANCE_PARTITION_ID = "default";

  public static SpannerResourceManager sharedSpannerResourceManager;
  public static SpannerResourceManager sharedPgSpannerResourceManager;

  static class SharedTestCase {
    private final List<String> ddlStatements;
    private final Consumer<Ddl> assertions;

    SharedTestCase(List<String> ddlStatements, Consumer<Ddl> assertions) {
      this.ddlStatements = ddlStatements;
      this.assertions = assertions;
    }

    public List<String> getDdlStatements() {
      return ddlStatements;
    }

    public Consumer<Ddl> getAssertions() {
      return assertions;
    }
  }

  private SpannerResourceManager spannerResourceManager;
  private SpannerResourceManager pgSpannerResourceManager;

  /*
   * Guidelines for Contributing New Test Cases to InformationSchemaScannerIT:
   *
   * 1. Use Shared Test Cases Where Possible:
   *    To avoid hitting the Spanner 100-database per instance limit and to dramatically reduce test
   *    execution time, prefer adding tests to the shared test suites (e.g., `sharedInformationSchemaScannerTestGsql`
   *    or `sharedInformationSchemaScannerTestPg`) over creating standalone `@Test` methods.
   *
   *    A shared test case returns a `SharedTestCase` object containing:
   *    - A list of DDL statements to create the schema objects.
   *    - A `Consumer<Ddl>` lambda containing assertions that validate the parsed schema.
   *    The suite collects all statements, applies them together on the shared database (`sharedSpannerResourceManager`
   *    for GSQL or `sharedPgSpannerResourceManager` for PostgreSQL), parses the schema once, and executes all assertions.
   *
   * 2. Shared Database Context:
   *    Keep in mind that shared test cases execute within a single multiplexed database. This database will contain
   *    other tables, views, indices, and schema objects created by sibling test cases.
   *
   * 3. Unique Naming Conventions:
   *    Because the database is shared, DDL statement names must be globally unique to prevent collisions.
   *    Always use the convention `<type_prefix>_<test_case_name>_<suffix>`. For example:
   *    - Tables: `t_myNewTestCase_Users`
   *    - Indices: `i_myNewTestCase_Users_NameIdx`
   *    - Views: `v_myNewTestCase_ActiveUsers`
   *
   * 4. Targeted Assertions:
   *    Do not write assertions that rely on global counts (e.g., asserting that the database has exactly 10 tables
   *    or exactly 3 views in total). Instead, use the `Ddl` POJO methods to selectively retrieve your test's objects
   *    (e.g., `ddl.table("t_myNewTestCase_Users")`) and assert their properties in isolation.
   *
   * 5. Independence of Execution:
   *    Do not make assumptions about the order of execution. Shared test cases should be completely stateless and
   *    independent of one another.
   */
  @BeforeClass
  public static void setupSharedResourceManagers() {
    String projectId = TestProperties.project();
    String region = TestProperties.region();
    String spannerHost = System.getProperty("spannerHost");

    FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
    fileDescriptorSetBuilder.addFile(TestMessage.getDescriptor().getFile().toProto());
    ByteString protoDescriptorBytes = fileDescriptorSetBuilder.build().toByteString();

    SpannerResourceManager.Builder builder =
        SpannerResourceManager.builder(
                "sharedGsql-" + UUID.randomUUID().toString().substring(0, 8),
                projectId,
                region,
                Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .setProtoDescriptors(protoDescriptorBytes.toByteArray());
    if (spannerHost != null) {
      builder.useCustomHost(spannerHost);
    }
    sharedSpannerResourceManager = builder.build();

    SpannerResourceManager.Builder pgBuilder =
        SpannerResourceManager.builder(
                "sharedPgsql-" + UUID.randomUUID().toString().substring(0, 8),
                projectId,
                region,
                Dialect.POSTGRESQL)
            .maybeUseStaticInstance();
    if (spannerHost != null) {
      pgBuilder.useCustomHost(spannerHost);
    }
    sharedPgSpannerResourceManager = pgBuilder.build();
  }

  @AfterClass
  public static void teardownSharedResourceManagers() {
    if (sharedSpannerResourceManager != null) {
      sharedSpannerResourceManager.cleanupAll();
    }
    if (sharedPgSpannerResourceManager != null) {
      sharedPgSpannerResourceManager.cleanupAll();
    }
  }

  private void setupResourceManager(Dialect dialect) {
    setupResourceManager(dialect, null);
  }

  private void setupResourceManager(Dialect dialect, byte[] protoDescriptors) {
    String projectId = TestProperties.project();

    SpannerResourceManager.Builder builder =
        SpannerResourceManager.builder(
            testName + "-" + UUID.randomUUID().toString().substring(0, 8),
            projectId,
            System.getProperty("spannerMultiRegion", "nam3"),
            dialect);
    if (protoDescriptors != null) {
      builder.setProtoDescriptors(protoDescriptors);
    }
    if (spannerHost != null) {
      builder.useCustomHost(spannerHost);
    }
    if (dialect == Dialect.GOOGLE_STANDARD_SQL) {
      if (spannerResourceManager != null) {
        spannerResourceManager.cleanupAll();
      }
      spannerResourceManager = builder.build();
    } else {
      if (pgSpannerResourceManager != null) {
        pgSpannerResourceManager.cleanupAll();
      }
      pgSpannerResourceManager = builder.build();
    }
  }

  private Ddl getDatabaseDdl(SpannerResourceManager manager) {
    BatchClient batchClient = manager.getBatchClient();
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());

    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx);
    return scanner.scan();
  }

  private Ddl getPgDatabaseDdl(SpannerResourceManager manager) {
    BatchClient batchClient = manager.getBatchClient();
    BatchReadOnlyTransaction batchTx =
        batchClient.batchReadOnlyTransaction(TimestampBound.strong());
    InformationSchemaScanner scanner = new InformationSchemaScanner(batchTx, Dialect.POSTGRESQL);
    return scanner.scan();
  }

  private SharedTestCase tableWithAllTypes() {
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
        "CREATE TABLE `t_tableWithAllTypes_alltypes` ("
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

    return new SharedTestCase(
        Arrays.asList(createProtoBundleStmt, createTableStmt),
        ddl -> {
          assertThat(ddl.table("t_tableWithAllTypes_alltypes"), notNullValue());
          assertThat(ddl.table("t_tAbLEwITHAlLTYPeS_AlLtYPeS"), notNullValue());

          Table table = ddl.table("t_tableWithAllTypes_alltypes");
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
          assertThat(
              table.column("arr_timestamp_field").type(), equalTo(Type.array(Type.timestamp())));
          assertThat(table.column("arr_date_field").type(), equalTo(Type.array(Type.date())));
          assertThat(table.column("embedding_vector").type(), equalTo(Type.array(Type.float64())));
          assertThat(table.column("embedding_vector").arrayLength(), equalTo(16));
          assertThat(
              table.column("arr_proto_field").type(),
              equalTo(
                  Type.array(Type.proto("com.google.cloud.teleport.spanner.tests.TestMessage"))));
          assertThat(
              table.column("arr_proto_field_2").type(),
              equalTo(Type.array(Type.proto("com.google.cloud.teleport.spanner.tests.Order"))));
          assertThat(
              table.column("arr_nested_enum").type(),
              equalTo(
                  Type.array(
                      Type.protoEnum(
                          "com.google.cloud.teleport.spanner.tests.Order.PaymentMode"))));
          assertThat(
              table.column("arr_enum_field").type(),
              equalTo(
                  Type.array(Type.protoEnum("com.google.cloud.teleport.spanner.tests.TestEnum"))));
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
        });
  }

  // This test safely uses the shared PostgreSQL ResourceManager because PostgreSQL databases
  // do not support or use Proto Bundles, so there is no risk of database-level schema conflicts.
  private SharedTestCase tableWithAllPgTypes() {
    String allTypes =
        "CREATE TABLE \"t_tableWithAllPgTypes_alltypes\" ("
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

    return new SharedTestCase(
        Collections.singletonList(allTypes),
        ddl -> {
          assertThat(ddl.table("t_tableWithAllPgTypes_alltypes"), notNullValue());
          assertThat(ddl.table("t_tableWithAllPgTypes_aLlTYPeS"), notNullValue());

          Table table = ddl.table("t_tableWithAllPgTypes_alltypes");
          assertThat(table.columns(), hasSize(22));

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
          assertThat(
              table.column("arr_float32_field").type(), equalTo(Type.pgArray(Type.pgFloat4())));
          assertThat(
              table.column("arr_float64_field").type(), equalTo(Type.pgArray(Type.pgFloat8())));
          assertThat(
              table.column("arr_string_field").type(), equalTo(Type.pgArray(Type.pgVarchar())));
          assertThat(table.column("arr_string_field").size(), equalTo(15));
          assertThat(table.column("arr_bytes_field").type(), equalTo(Type.pgArray(Type.pgBytea())));
          assertThat(
              table.column("arr_timestamp_field").type(),
              equalTo(Type.pgArray(Type.pgTimestamptz())));
          assertThat(table.column("arr_date_field").type(), equalTo(Type.pgArray(Type.pgDate())));
          assertThat(
              table.column("arr_numeric_field").type(), equalTo(Type.pgArray(Type.pgNumeric())));
          assertThat(
              table.column("embedding_vector").type(), equalTo(Type.pgArray(Type.pgFloat8())));
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
        });
  }

  private SharedTestCase simpleModel() {
    String endpoint =
        String.format(
            "\"//aiplatform.googleapis.com/projects/%s/locations/us-central1/publishers/google/models/text-bison\"",
            TestProperties.project());
    String modelDef =
        "CREATE MODEL `m_simpleModel_text_bison` INPUT ( `prompt` STRING(MAX) )"
            + " OUTPUT ( `content` STRING(MAX) ) REMOTE OPTIONS"
            + " (endpoint="
            + endpoint
            + ")";

    return new SharedTestCase(
        Arrays.asList(modelDef),
        ddl -> {
          Model model = ddl.model("m_simpleModel_text_bison");
          assertThat(model, notNullValue());
          assertThat(ddl.model("m_simpleModel_tExT_bIsOn"), sameInstance(model));
          assertThat(model.inputColumns(), hasSize(1));
          assertThat(model.inputColumns().get(0).name(), is("prompt"));
          assertThat(model.inputColumns().get(0).type(), is(Type.string()));
          assertThat(model.outputColumns(), hasSize(1));
          assertThat(model.outputColumns().get(0).name(), is("content"));
          assertThat(model.outputColumns().get(0).type(), is(Type.string()));
          assertThat(model.remote(), equalTo(true));
          assertThat(model.options(), hasItems("endpoint=" + endpoint));
        });
  }

  private SharedTestCase simplePropertyGraph() {
    String nodeTableDef =
        "CREATE TABLE t_simplePropertyGraph_NodeTest (\n"
            + "  Id INT64 NOT NULL,\n"
            + ") PRIMARY KEY(Id)";
    String edgeTableDef =
        "CREATE TABLE t_simplePropertyGraph_EdgeTest (\n"
            + "FromId INT64 NOT NULL,\n"
            + "ToId INT64 NOT NULL,\n"
            + ") PRIMARY KEY(FromId, ToId)";
    String propertyGraphDef =
        "CREATE PROPERTY GRAPH g_simplePropertyGraph_testGraph\n"
            + "  NODE TABLES(\n"
            + "    t_simplePropertyGraph_NodeTest\n"
            + "      KEY(Id)\n"
            + "      LABEL Test PROPERTIES(\n"
            + "        Id))"
            + "  EDGE TABLES(\n"
            + "    t_simplePropertyGraph_EdgeTest\n"
            + "      KEY(FromId, ToId)\n"
            + "      SOURCE KEY(FromId) REFERENCES t_simplePropertyGraph_NodeTest(Id)\n"
            + "      DESTINATION KEY(ToId) REFERENCES t_simplePropertyGraph_NodeTest(Id)\n"
            + "      DEFAULT LABEL PROPERTIES ALL COLUMNS)";

    return new SharedTestCase(
        Arrays.asList(nodeTableDef, edgeTableDef, propertyGraphDef),
        ddl -> {
          assertThat(ddl.table("t_simplePropertyGraph_NodeTest"), notNullValue());

          PropertyGraph testGraph = ddl.propertyGraph("g_simplePropertyGraph_testGraph");

          assertEquals(testGraph.name(), "g_simplePropertyGraph_testGraph");
          assertThat(testGraph.propertyDeclarations(), hasSize(3));
          assertThat(testGraph.getPropertyDeclaration("Id"), notNullValue());
          assertThat(testGraph.getPropertyDeclaration("FromId"), notNullValue());
          assertThat(testGraph.getPropertyDeclaration("ToId"), notNullValue());

          assertThat(testGraph.labels(), hasSize(2));
          assertThat(testGraph.getLabel("Test"), notNullValue());
          assertThat(testGraph.getLabel("t_simplePropertyGraph_EdgeTest"), notNullValue());

          assertThat(testGraph.nodeTables(), hasSize(1));
          assertThat(testGraph.getNodeTable("t_simplePropertyGraph_NodeTest"), notNullValue());

          assertThat(testGraph.edgeTables(), hasSize(1));
          assertThat(testGraph.getEdgeTable("t_simplePropertyGraph_EdgeTest"), notNullValue());

          // --- Assertions for Node Table ---
          GraphElementTable nodeTestTable =
              testGraph.getNodeTable("t_simplePropertyGraph_NodeTest");
          assertThat(nodeTestTable, notNullValue());
          assertThat(nodeTestTable.name(), equalTo("t_simplePropertyGraph_NodeTest"));
          assertThat(nodeTestTable.baseTableName(), equalTo("t_simplePropertyGraph_NodeTest"));
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
          GraphElementTable edgeTestTable =
              testGraph.getEdgeTable("t_simplePropertyGraph_EdgeTest");
          assertThat(edgeTestTable, notNullValue());
          assertThat(edgeTestTable.name(), equalTo("t_simplePropertyGraph_EdgeTest"));
          assertThat(edgeTestTable.baseTableName(), equalTo("t_simplePropertyGraph_EdgeTest"));
          assertThat(edgeTestTable.kind(), equalTo(GraphElementTable.Kind.EDGE));
          assertIterableEquals(List.of("FromId", "ToId"), edgeTestTable.keyColumns());

          assertThat(edgeTestTable.labelToPropertyDefinitions(), hasSize(1));
          GraphElementTable.LabelToPropertyDefinitions edgeTestLabel =
              edgeTestTable.getLabelToPropertyDefinitions("t_simplePropertyGraph_EdgeTest");
          assertThat(edgeTestLabel, notNullValue());
          assertThat(edgeTestLabel.labelName, equalTo("t_simplePropertyGraph_EdgeTest"));
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
          assertThat(
              edgeTestTable.sourceNodeTable().nodeTableName,
              equalTo("t_simplePropertyGraph_NodeTest"));
          assertIterableEquals(List.of("Id"), edgeTestTable.sourceNodeTable().nodeKeyColumns);

          assertIterableEquals(List.of("FromId"), edgeTestTable.sourceNodeTable().edgeKeyColumns);

          assertThat(
              edgeTestTable.targetNodeTable().nodeTableName,
              equalTo("t_simplePropertyGraph_NodeTest"));
          assertIterableEquals(List.of("Id"), edgeTestTable.targetNodeTable().nodeKeyColumns);
          assertIterableEquals(List.of("ToId"), edgeTestTable.targetNodeTable().edgeKeyColumns);
        });
  }

  private SharedTestCase dynamicPropertyGraph() {
    String nodeTableDef =
        "CREATE TABLE t_dynamicPropertyGraph_NodeTest (\n"
            + "  Id INT64 NOT NULL,\n"
            + "  DynamicLabelCol STRING(MAX)\n"
            + ") PRIMARY KEY(Id)";
    String edgeTableDef =
        "CREATE TABLE t_dynamicPropertyGraph_EdgeTest (\n"
            + "  FromId INT64 NOT NULL,\n"
            + "  ToId INT64 NOT NULL,\n"
            + "  DynamicPropsCol JSON\n"
            + ") PRIMARY KEY(FromId, ToId)";
    String propertyGraphDef =
        "CREATE PROPERTY GRAPH g_dynamicPropertyGraph_testGraph\n"
            + "  NODE TABLES(\n"
            + "    t_dynamicPropertyGraph_NodeTest\n"
            + "      KEY(Id)\n"
            + "      LABEL Test PROPERTIES(\n"
            + "        Id)\n"
            // Add the DYNAMIC LABEL clause
            + "      DYNAMIC LABEL(DynamicLabelCol))\n"
            + "  EDGE TABLES(\n"
            + "    t_dynamicPropertyGraph_EdgeTest\n"
            + "      KEY(FromId, ToId)\n"
            + "      SOURCE KEY(FromId) REFERENCES t_dynamicPropertyGraph_NodeTest(Id)\n"
            + "      DESTINATION KEY(ToId) REFERENCES t_dynamicPropertyGraph_NodeTest(Id)\n"
            + "      DEFAULT LABEL PROPERTIES ALL COLUMNS\n"
            // Add the DYNAMIC PROPERTIES clause
            + "      DYNAMIC PROPERTIES(DynamicPropsCol))";

    return new SharedTestCase(
        Arrays.asList(nodeTableDef, edgeTableDef, propertyGraphDef),
        ddl -> {
          assertThat(ddl.table("t_dynamicPropertyGraph_NodeTest"), notNullValue());

          PropertyGraph testGraph = ddl.propertyGraph("g_dynamicPropertyGraph_testGraph");

          assertEquals(testGraph.name(), "g_dynamicPropertyGraph_testGraph");
          assertThat(testGraph.propertyDeclarations(), hasSize(4));

          // --- Assertions for Node Table ---
          GraphElementTable nodeTestTable =
              testGraph.getNodeTable("t_dynamicPropertyGraph_NodeTest");
          assertThat(nodeTestTable, notNullValue());
          assertThat(nodeTestTable.name(), equalTo("t_dynamicPropertyGraph_NodeTest"));
          // Assert that the dynamic label expression is correctly captured
          assertThat(
              nodeTestTable.dynamicLabelExpression().dynamicLabelExpression,
              equalTo("DynamicLabelCol"));

          // --- Assertions for Edge Table ---
          GraphElementTable edgeTestTable =
              testGraph.getEdgeTable("t_dynamicPropertyGraph_EdgeTest");
          assertThat(edgeTestTable, notNullValue());
          assertThat(edgeTestTable.name(), equalTo("t_dynamicPropertyGraph_EdgeTest"));
          // Assert that the dynamic properties expression is correctly captured
          assertThat(
              edgeTestTable.dynamicPropertiesExpression().dynamicPropertiesExpression,
              equalTo("DynamicPropsCol"));

          // Assertions for the edge's default label properties
          GraphElementTable.LabelToPropertyDefinitions edgeTestLabel =
              edgeTestTable.getLabelToPropertyDefinitions("t_dynamicPropertyGraph_EdgeTest");
          assertThat(edgeTestLabel, notNullValue());
          // The number of properties for the default label now includes the new column
          assertThat(
              edgeTestLabel.propertyDefinitions(), hasSize(3)); // FromId, ToId, DynamicPropsCol

          GraphElementTable.PropertyDefinition edgeTestFromIdProperty =
              edgeTestLabel.getPropertyDefinition("FromId");
          assertThat(edgeTestFromIdProperty, notNullValue());

          GraphElementTable.PropertyDefinition edgeTestToIdProperty =
              edgeTestLabel.getPropertyDefinition("ToId");
          assertThat(edgeTestToIdProperty, notNullValue());

          GraphElementTable.PropertyDefinition edgeTestDynamicPropsProperty =
              edgeTestLabel.getPropertyDefinition("DynamicPropsCol");
          assertThat(edgeTestDynamicPropsProperty, notNullValue());
          assertThat(edgeTestDynamicPropsProperty.name, equalTo("DynamicPropsCol"));
          assertThat(
              edgeTestDynamicPropsProperty.valueExpressionString, equalTo("DynamicPropsCol"));
        });
  }

  private SharedTestCase simpleView() {
    String tableDef =
        "CREATE TABLE t_simpleView_Users ("
            + " id INT64 NOT NULL,"
            + " name STRING(MAX),"
            + ") PRIMARY KEY (id)";
    String viewDef =
        "CREATE VIEW v_simpleView_Names SQL SECURITY INVOKER AS SELECT u.name FROM t_simpleView_Users u";

    return new SharedTestCase(
        Arrays.asList(tableDef, viewDef),
        ddl -> {
          assertThat(ddl.table("t_simpleView_Users"), notNullValue());
          assertThat(ddl.table("t_simpleView_uSers"), notNullValue());

          View view = ddl.view("v_simpleView_Names");
          assertThat(view, notNullValue());
          assertThat(ddl.view("v_simpleView_nAmes"), sameInstance(view));

          assertThat(view.query(), equalTo("SELECT u.name FROM t_simpleView_Users u"));
        });
  }

  private SharedTestCase pgSimpleView() {
    String tableDef =
        "CREATE TABLE \"t_pgSimpleView_Users\" ("
            + " id bigint NOT NULL,"
            + " name character varying,"
            + " PRIMARY KEY (id)) ";
    String viewDef =
        "CREATE VIEW \"v_pgSimpleView_Names\" SQL SECURITY INVOKER AS SELECT name FROM \"t_pgSimpleView_Users\"";

    return new SharedTestCase(
        Arrays.asList(tableDef, viewDef),
        ddl -> {
          assertThat(ddl.table("t_pgSimpleView_Users"), notNullValue());
          assertThat(ddl.table("t_pgSimpleView_uSers"), notNullValue());

          View view = ddl.view("v_pgSimpleView_Names");
          assertThat(view, notNullValue());
          assertThat(ddl.view("v_pgSimpleView_nAmes"), sameInstance(view));

          assertThat(view.query(), equalTo("SELECT name FROM \"t_pgSimpleView_Users\""));
        });
  }

  private SharedTestCase simpleUdf() {
    String namedSchemaDef = "CREATE SCHEMA s_simpleUdf";
    String udfDef1 = "CREATE FUNCTION s_simpleUdf.u_simpleUdf_foo() AS (1)";
    String udfDef2 =
        "CREATE FUNCTION s_simpleUdf.u_simpleUdf_default_values("
            + "A STRING, "
            + "B STRING DEFAULT NULL, "
            + "C STRING DEFAULT 'NULL', "
            + "D STRING DEFAULT '') "
            + "RETURNS STRING AS (CONCAT(A, '::', B, '::', C, '::', D))";
    String udfDef3 =
        "CREATE FUNCTION s_simpleUdf.u_remote_udf(x INT64, y INT64) "
            + "RETURNS INT64 NOT DETERMINISTIC LANGUAGE REMOTE "
            + "OPTIONS ( endpoint = 'https://us-central1-myproject.cloudfunctions.net/myfunc', max_batching_rows = 50 )";

    return new SharedTestCase(
        Arrays.asList(namedSchemaDef, udfDef1, udfDef2, udfDef3),
        ddl -> {
          assertThat(ddl.schema("s_simpleUdf"), notNullValue());

          Udf udf1 = ddl.udf("s_simpleUdf.u_simpleUdf_foo");
          assertThat(udf1, notNullValue());
          assertThat(ddl.udf("s_simpleUdf.u_simpleUdf_foo"), sameInstance(udf1));

          Udf udf2 = ddl.udf("s_simpleUdf.u_simpleUdf_default_values");
          assertThat(udf2, notNullValue());
          assertThat(ddl.udf("s_simpleUdf.u_simpleUdf_default_values"), sameInstance(udf2));

          Udf udf3 = ddl.udf("s_simpleUdf.u_remote_udf");
          assertThat(udf3, notNullValue());
          assertThat(ddl.udf("S_SIMPLEUDF.U_REMOTE_UDF"), sameInstance(udf3));

          assertThat(udf1.name(), equalTo("s_simpleUdf.u_simpleUdf_foo"));
          assertThat(udf1.type(), equalTo("INT64"));
          assertEquals(udf1.language(), "SQL");
          assertThat(udf1.options(), empty());
          assertThat(udf1.definition(), equalTo("1"));
          assertEquals(udf1.security(), Udf.SqlSecurity.INVOKER);

          assertThat(udf2.name(), equalTo("s_simpleUdf.u_simpleUdf_default_values"));
          assertThat(udf2.type(), equalTo("STRING"));
          assertEquals(udf2.language(), "SQL");
          assertThat(udf2.options(), empty());
          assertThat(udf2.definition(), equalTo("CONCAT(A, '::', B, '::', C, '::', D)"));
          assertEquals(udf2.security(), Udf.SqlSecurity.INVOKER);
          assertThat(
              udf2.parameters(),
              hasItems(
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_simpleUdf_default_values")
                      .name("A")
                      .type("STRING")
                      .defaultExpression(null)
                      .autoBuild(),
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_simpleUdf_default_values")
                      .name("B")
                      .type("STRING")
                      .defaultExpression("NULL")
                      .autoBuild(),
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_simpleUdf_default_values")
                      .name("C")
                      .type("STRING")
                      .defaultExpression("'NULL'")
                      .autoBuild(),
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_simpleUdf_default_values")
                      .name("D")
                      .type("STRING")
                      .defaultExpression("''")
                      .autoBuild()));
          assertThat(udf3.name(), equalTo("s_simpleUdf.u_remote_udf"));
          assertThat(udf3.type(), equalTo("INT64"));
          assertEquals(udf3.language(), "REMOTE");
          assertThat(
              udf3.options(),
              hasItems(
                  "endpoint=\"https://us-central1-myproject.cloudfunctions.net/myfunc\"",
                  "max_batching_rows=50"));
          assertEquals(udf3.definition(), "");
          assertEquals(udf3.security(), Udf.SqlSecurity.INVOKER);
          assertThat(
              udf3.parameters(),
              hasItems(
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_remote_udf")
                      .name("x")
                      .type("INT64")
                      .defaultExpression(null)
                      .autoBuild(),
                  UdfParameter.builder()
                      .functionSpecificName("s_simpleUdf.u_remote_udf")
                      .name("y")
                      .type("INT64")
                      .defaultExpression(null)
                      .autoBuild()));
        });
  }

  // TODO(b/485601737): Add PG UDFs.

  private SharedTestCase interleavedIn() {
    List<String> statements =
        Arrays.asList(
            " CREATE TABLE t_interleavedIn_lEVEl0 ("
                + " id0                                   INT64 NOT NULL,"
                + " val0                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC)",
            " CREATE TABLE t_interleavedIn_level1 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " val1                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC), INTERLEAVE IN PARENT t_interleavedIn_lEVEl0",
            " CREATE TABLE t_interleavedIn_level2 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " id2                                   INT64 NOT NULL,"
                + " val2                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC, id2 ASC), INTERLEAVE IN PARENT t_interleavedIn_level1",
            " CREATE TABLE t_interleavedIn_level2_1 ("
                + " id0                                   INT64 NOT NULL,"
                + " id1                                   INT64 NOT NULL,"
                + " id2_1                                 INT64 NOT NULL,"
                + " val2                                  STRING(MAX),"
                + " ) PRIMARY KEY (id0 ASC, id1 ASC, id2_1 ASC),"
                + " INTERLEAVE IN PARENT t_interleavedIn_level1 ON DELETE CASCADE");

    return new SharedTestCase(
        statements,
        ddl -> {
          HashMultimap<Integer, String> levels = ddl.perLevelView();
          // Since we use a shared database, level 0 will contain many other tables.
          // Therefore, we only assert that our specific tables exist at the correct levels.
          assertThat(levels.get(0), hasItems("t_interleavedin_level0"));
          assertThat(levels.get(1), hasItems("t_interleavedin_level1"));
          assertThat(levels.get(2), hasItems("t_interleavedin_level2", "t_interleavedin_level2_1"));

          assertThat(ddl.table("t_interleavedIn_lEVEl0").interleaveInParent(), nullValue());
          assertThat(
              ddl.table("t_interleavedIn_level1").interleaveInParent(),
              equalTo("t_interleavedIn_lEVEl0"));
          assertThat(
              ddl.table("t_interleavedIn_level2").interleaveInParent(),
              equalTo("t_interleavedIn_level1"));
          assertThat(ddl.table("t_interleavedIn_level2").onDeleteCascade(), is(false));
          assertThat(
              ddl.table("t_interleavedIn_level2_1").interleaveInParent(),
              equalTo("t_interleavedIn_level1"));
          assertThat(ddl.table("t_interleavedIn_level2_1").onDeleteCascade(), is(true));
        });
  }

  private SharedTestCase pgInterleavedIn() {
    List<String> statements =
        Arrays.asList(
            " CREATE TABLE \"t_pgInterleavedIn_level0\" ("
                + " id0                                   bigint NOT NULL,"
                + " val0                                  character varying,"
                + " PRIMARY KEY (id0)"
                + " )",
            " CREATE TABLE \"t_pgInterleavedIn_level1\" ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " val1                                  character varying,"
                + " PRIMARY KEY (id0, id1)"
                + " ) INTERLEAVE IN PARENT \"t_pgInterleavedIn_level0\"",
            " CREATE TABLE \"t_pgInterleavedIn_level2\" ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " id2                                   bigint NOT NULL,"
                + " val2                                  character varying,"
                + " PRIMARY KEY (id0, id1, id2)"
                + " ) INTERLEAVE IN PARENT \"t_pgInterleavedIn_level1\"",
            " CREATE TABLE \"t_pgInterleavedIn_level2_1\" ("
                + " id0                                   bigint NOT NULL,"
                + " id1                                   bigint NOT NULL,"
                + " id2_1                                 bigint NOT NULL,"
                + " val2                                  character varying,"
                + " PRIMARY KEY (id0, id1, id2_1)"
                + " ) INTERLEAVE IN PARENT \"t_pgInterleavedIn_level1\" ON DELETE CASCADE");

    return new SharedTestCase(
        statements,
        ddl -> {
          HashMultimap<Integer, String> levels = ddl.perLevelView();
          // Since we use a shared database, level 0 will contain many other tables.
          // Therefore, we only assert that our specific tables exist at the correct levels.
          assertThat(levels.get(0), hasItems("t_pginterleavedin_level0"));
          assertThat(levels.get(1), hasItems("t_pginterleavedin_level1"));
          assertThat(
              levels.get(2), hasItems("t_pginterleavedin_level2", "t_pginterleavedin_level2_1"));

          assertThat(ddl.table("t_pgInterleavedIn_level0").interleaveInParent(), nullValue());
          assertThat(
              ddl.table("t_pgInterleavedIn_level1").interleaveInParent(),
              equalTo("t_pgInterleavedIn_level0"));
          assertThat(
              ddl.table("t_pgInterleavedIn_level2").interleaveInParent(),
              equalTo("t_pgInterleavedIn_level1"));
          assertThat(ddl.table("t_pgInterleavedIn_level2").onDeleteCascade(), is(false));
          assertThat(
              ddl.table("t_pgInterleavedIn_level2_1").interleaveInParent(),
              equalTo("t_pgInterleavedIn_level1"));
          assertThat(ddl.table("t_pgInterleavedIn_level2_1").onDeleteCascade(), is(true));
        });
  }

  private SharedTestCase reserved() {
    String statement =
        "CREATE TABLE `t_reserved_where` ("
            + " `JOIN`                                  STRING(MAX) NOT NULL,"
            + " `TABLE`                                 INT64,"
            + " `NULL`                                  INT64,"
            + " ) PRIMARY KEY (`NULL` ASC)";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_reserved_where"), notNullValue());
          Table table = ddl.table("t_reserved_where");
          assertThat(table.column("join"), notNullValue());
          assertThat(table.column("table"), notNullValue());
          assertThat(table.column("null"), notNullValue());
        });
  }

  private SharedTestCase pgReserved() {
    String statement =
        "CREATE TABLE \"t_pgReserved_where\" ("
            + " \"JOIN\"                                  character varying NOT NULL,"
            + " \"TABLE\"                                 bigint,"
            + " \"NULL\"                                  bigint NOT NULL,"
            + " PRIMARY KEY (\"NULL\")"
            + " )";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_pgReserved_where"), notNullValue());
          Table table = ddl.table("t_pgReserved_where");
          assertThat(table.column("JOIN"), notNullValue());
          assertThat(table.column("Table"), notNullValue());
          assertThat(table.column("NULL"), notNullValue());
        });
  }

  private SharedTestCase indexes() {
    // Prefix indexes to ensure ordering.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `t_indexes_Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE UNIQUE NULL_FILTERED INDEX `i_indexes_a_last_name_idx` ON "
                + " `t_indexes_Users`(`last_name` ASC) STORING (`first_name`)",
            " CREATE INDEX `i_indexes_b_age_idx` ON `t_indexes_Users`(`age` DESC) WHERE age IS NOT NULL",
            " CREATE UNIQUE INDEX `i_indexes_c_first_name_idx` ON `t_indexes_Users`(`first_name` ASC)",
            " CREATE TABLE `t_indexes_Logs` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `log_id`                                INT64 NOT NULL,"
                + " `message`                               STRING(MAX),"
                + " ) PRIMARY KEY (`id` ASC, `log_id` ASC), INTERLEAVE IN PARENT `t_indexes_Users`",
            " CREATE INDEX `i_indexes_d_log_message_idx` ON `t_indexes_Logs`(`id` ASC, `message` ASC) WHERE message IS NOT NULL, INTERLEAVE IN `t_indexes_Users`");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table usersTable = ddl.table("t_indexes_Users");
          assertThat(usersTable, notNullValue());
          assertThat(usersTable.indexes(), hasSize(3));
          assertThat(
              usersTable.indexes().get(0),
              equalTo(
                  "CREATE UNIQUE NULL_FILTERED INDEX `i_indexes_a_last_name_idx` ON `t_indexes_Users`(`last_name` ASC) STORING (`first_name`)"));
          assertThat(
              usersTable.indexes().get(1),
              equalTo(
                  "CREATE INDEX `i_indexes_b_age_idx` ON `t_indexes_Users`(`age` DESC) WHERE age IS NOT NULL"));
          assertThat(
              usersTable.indexes().get(2),
              equalTo(
                  "CREATE UNIQUE INDEX `i_indexes_c_first_name_idx` ON `t_indexes_Users`(`first_name` ASC)"));

          Table logsTable = ddl.table("t_indexes_Logs");
          assertThat(logsTable, notNullValue());
          assertThat(logsTable.indexes(), hasSize(1));
          assertThat(
              logsTable.indexes().get(0),
              equalTo(
                  "CREATE INDEX `i_indexes_d_log_message_idx` ON `t_indexes_Logs`(`id` ASC, `message` ASC) WHERE message IS NOT NULL, INTERLEAVE IN `t_indexes_Users`"));
        });
  }

  private SharedTestCase searchIndexes() {
    // Prefix indexes to ensure ordering.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `t_searchIndexes_Users` ("
                + "  `UserId`                                INT64 NOT NULL,"
                + " ) PRIMARY KEY (`UserId` ASC)",
            " CREATE TABLE `t_searchIndexes_Messages` ("
                + "  `UserId`                                INT64 NOT NULL,"
                + "  `MessageId`                             INT64 NOT NULL,"
                + "  `Subject`                               STRING(MAX),"
                + "  `Subject_Tokens`                        TOKENLIST AS (TOKENIZE_FULLTEXT(`Subject`)) HIDDEN,"
                + "  `Body`                                  STRING(MAX),"
                + "  `Body_Tokens`                           TOKENLIST AS (TOKENIZE_FULLTEXT(`Body`)) HIDDEN,"
                + "  `Data`                                  STRING(MAX),"
                + " ) PRIMARY KEY (`UserId` ASC, `MessageId` ASC), INTERLEAVE IN PARENT `t_searchIndexes_Users`",
            " CREATE SEARCH INDEX `i_searchIndexes_SearchIndex` ON `t_searchIndexes_Messages`(`Subject_Tokens` , `Body_Tokens` )"
                + " STORING (`Data`)"
                + " PARTITION BY `UserId`,"
                + " INTERLEAVE IN `t_searchIndexes_Users`"
                + " OPTIONS (sort_order_sharding=TRUE)");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table messagesTable = ddl.table("t_searchIndexes_Messages");
          assertThat(messagesTable, notNullValue());
          assertThat(messagesTable.indexes(), hasSize(1));
          assertThat(
              messagesTable.indexes().get(0),
              equalTo(
                  "CREATE SEARCH INDEX `i_searchIndexes_SearchIndex` ON `t_searchIndexes_Messages`(`Subject_Tokens` , `Body_Tokens` ) STORING (`Data`) PARTITION BY `UserId`, INTERLEAVE IN `t_searchIndexes_Users` OPTIONS (sort_order_sharding=TRUE)"));
        });
  }

  private SharedTestCase pgSearchIndexes() {
    // Prefix indexes to ensure ordering.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"t_pgSearchIndexes_Users\" ("
                + "  \"userid\"                              bigint NOT NULL,"
                + "  PRIMARY KEY (\"userid\")"
                + " )",
            " CREATE TABLE \"t_pgSearchIndexes_Messages\" ("
                + "  \"userid\"                              bigint NOT NULL,"
                + "  \"messageid\"                           bigint NOT NULL,"
                + "  \"orderid\"                             bigint NOT NULL,"
                + "  \"orderid_tokens\"                      spanner.tokenlist GENERATED ALWAYS AS (spanner.tokenize_number(orderid)) VIRTUAL HIDDEN,"
                + "  \"subject\"                             character varying,"
                + "  \"subject_tokens\"                      spanner.tokenlist GENERATED ALWAYS AS (spanner.tokenize_fulltext(subject)) STORED HIDDEN,"
                + "  \"body\"                                character varying,"
                + "  \"body_tokens\"                         spanner.tokenlist GENERATED ALWAYS AS (spanner.tokenize_fulltext(body)) STORED HIDDEN,"
                + "  \"data\"                                character varying,"
                + "   PRIMARY KEY (\"userid\", \"messageid\")"
                + " ) INTERLEAVE IN PARENT \"t_pgSearchIndexes_Users\"",
            " CREATE SEARCH INDEX \"i_pgSearchIndexes_SearchIndex\" ON \"t_pgSearchIndexes_Messages\"(\"subject_tokens\" , \"body_tokens\" )"
                + " INCLUDE (\"data\")"
                + " PARTITION BY \"userid\""
                + " ORDER BY \"orderid\""
                + " INTERLEAVE IN \"t_pgSearchIndexes_Users\""
                + " WITH (sort_order_sharding=TRUE)");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_pgSearchIndexes_Messages");
          assertThat(
              table.indexes(),
              hasItems(
                  "CREATE SEARCH INDEX \"i_pgSearchIndexes_SearchIndex\" ON \"t_pgSearchIndexes_Messages\"(\"subject_tokens\" , \"body_tokens\" ) INCLUDE (\"data\") PARTITION BY \"userid\" ORDER BY \"orderid\" INTERLEAVE IN \"t_pgSearchIndexes_Users\" WITH (sort_order_sharding=TRUE)"));
        });
  }

  private SharedTestCase vectorIndexes() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `t_vectorIndexes_Base` ("
                + " `K`                                     INT64,"
                + " `V`                                     INT64,"
                + " `Embeddings`                            ARRAY<FLOAT32>(vector_length=>128),"
                + " ) PRIMARY KEY (`K` ASC)",
            " CREATE VECTOR INDEX `i_vectorIndexes_VI` ON `t_vectorIndexes_Base`(`Embeddings` ) WHERE Embeddings IS NOT NULL"
                + " OPTIONS (distance_type=\"COSINE\")");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_vectorIndexes_Base");
          assertThat(
              table.indexes(),
              hasItems(
                  "CREATE VECTOR INDEX `i_vectorIndexes_VI` ON `t_vectorIndexes_Base`(`Embeddings` ) WHERE Embeddings IS NOT NULL OPTIONS (distance_type=\"COSINE\")"));
        });
  }

  // CREATE INDEX vector_index ON Base USING ScaNN (embedding_column)
  // INCLUDE (v1, v2) WITH (distance_type = 'COSINE', tree_depth = 3)
  // WHERE (embedding_column IS NOT NULL);
  private SharedTestCase pgVectorIndexes() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"t_pgVectorIndexes_Base\" ("
                + " \"K\"                                     bigint NOT NULL,"
                + " \"V\"                                     bigint,"
                + " \"Embeddings\"                            double precision[] vector length 128,"
                + " PRIMARY KEY (\"K\")"
                + " )",
            " CREATE INDEX \"i_pgVectorIndexes_VI\" ON \"t_pgVectorIndexes_Base\" USING ScaNN (\"Embeddings\" )"
                + " WITH (distance_type='COSINE') WHERE \"Embeddings\" IS NOT NULL");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_pgVectorIndexes_Base");
          assertThat(
              table.indexes(),
              hasItems(
                  "CREATE INDEX \"i_pgVectorIndexes_VI\" ON \"t_pgVectorIndexes_Base\" USING ScaNN (\"Embeddings\" ) WITH (distance_type='COSINE') WHERE \"Embeddings\" IS NOT NULL"));
        });
  }

  private SharedTestCase pgIndexes() {
    // Prefix indexes to ensure ordering.
    // Unique index is implicitly null-filtered.
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"t_pgIndexes_Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"AGE\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE UNIQUE INDEX \"i_pgIndexes_a_last_name_idx\" ON  \"t_pgIndexes_Users\"(\"last_name\" ASC) INCLUDE"
                + " (\"first_name\") WHERE first_name IS NOT NULL AND last_name IS NOT"
                + " NULL",
            " CREATE INDEX \"i_pgIndexes_b_age_idx\" ON \"t_pgIndexes_Users\"(\"id\" ASC, \"AGE\" DESC) INTERLEAVE IN"
                + " \"t_pgIndexes_Users\" WHERE \"AGE\" IS NOT NULL",
            " CREATE UNIQUE INDEX \"i_pgIndexes_c_first_name_idx\" ON \"t_pgIndexes_Users\"(\"first_name\" ASC) WHERE"
                + " first_name IS NOT NULL",
            " CREATE INDEX \"i_pgIndexes_null_ordering_idx\" ON \"t_pgIndexes_Users\"(\"id\" ASC NULLS FIRST,"
                + " \"first_name\" ASC, \"last_name\" DESC, \"AGE\" DESC NULLS LAST)");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table usersTable = ddl.table("t_pgIndexes_Users");
          assertThat(usersTable, notNullValue());
          assertThat(usersTable.indexes(), hasSize(4));
          assertThat(
              usersTable.indexes().get(0),
              equalTo(
                  "CREATE UNIQUE INDEX \"i_pgIndexes_a_last_name_idx\" ON \"t_pgIndexes_Users\"(\"last_name\" ASC) INCLUDE (\"first_name\") WHERE first_name IS NOT NULL AND last_name IS NOT NULL"));
          assertThat(
              usersTable.indexes().get(1),
              equalTo(
                  "CREATE INDEX \"i_pgIndexes_b_age_idx\" ON \"t_pgIndexes_Users\"(\"id\" ASC, \"AGE\" DESC) INTERLEAVE IN \"t_pgIndexes_Users\" WHERE \"AGE\" IS NOT NULL"));
          assertThat(
              usersTable.indexes().get(2),
              equalTo(
                  "CREATE UNIQUE INDEX \"i_pgIndexes_c_first_name_idx\" ON \"t_pgIndexes_Users\"(\"first_name\" ASC) WHERE first_name IS NOT NULL"));
          assertThat(
              usersTable.indexes().get(3),
              equalTo(
                  "CREATE INDEX \"i_pgIndexes_null_ordering_idx\" ON \"t_pgIndexes_Users\"(\"id\" ASC NULLS FIRST, \"first_name\" ASC, \"last_name\" DESC, \"AGE\" DESC NULLS LAST)"));
        });
  }

  private SharedTestCase foreignKeys() {
    List<String> dbCreationStatements =
        Arrays.asList(
            "CREATE TABLE `t_foreignKeys_Ref` ("
                + " `id1`                               INT64 NOT NULL,"
                + " `id2`                               INT64 NOT NULL,"
                + " ) PRIMARY KEY (`id1` ASC, `id2` ASC)",
            " CREATE TABLE `t_foreignKeys_Tab` ("
                + " `key`                               INT64 NOT NULL,"
                + " `id1`                               INT64 NOT NULL,"
                + " `id2`                               INT64 NOT NULL,"
                + " ) PRIMARY KEY (`key` ASC)",
            " ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `t_foreignKeys_Ref` (`id2`, `id1`)",
            " ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_2` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) ON DELETE CASCADE ENFORCED",
            " ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_3` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) NOT ENFORCED",
            " ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_4` FOREIGN KEY (`id1`, `id2`)"
                + " REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED");

    return new SharedTestCase(
        dbCreationStatements,
        ddl -> {
          Table tabTable = ddl.table("t_foreignKeys_Tab");
          assertThat(tabTable, notNullValue());
          assertThat(tabTable.foreignKeys(), hasSize(4));
          assertThat(
              tabTable.foreignKeys(),
              hasItems(
                  "ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk` FOREIGN KEY (`id1`, `id2`) REFERENCES `t_foreignKeys_Ref` (`id2`, `id1`) ON DELETE NO ACTION",
                  "ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_2` FOREIGN KEY (`id1`, `id2`) REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) ON DELETE CASCADE",
                  "ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_3` FOREIGN KEY (`id1`, `id2`) REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED",
                  "ALTER TABLE `t_foreignKeys_Tab` ADD CONSTRAINT `c_foreignKeys_fk_4` FOREIGN KEY (`id1`, `id2`) REFERENCES `t_foreignKeys_Ref` (`id1`, `id2`) ON DELETE NO ACTION NOT ENFORCED"));
        });
  }

  private SharedTestCase pgForeignKeys() {
    List<String> dbCreationStatements =
        Arrays.asList(
            "CREATE TABLE \"t_pgForeignKeys_Ref\" ("
                + " \"id1\"                               bigint NOT NULL,"
                + " \"id2\"                               bigint NOT NULL,"
                + " PRIMARY KEY (\"id1\", \"id2\")"
                + " )",
            " CREATE TABLE \"t_pgForeignKeys_Tab\" ("
                + " \"key\"                               bigint NOT NULL,"
                + " \"id1\"                               bigint NOT NULL,"
                + " \"id2\"                               bigint NOT NULL,"
                + " PRIMARY KEY (\"key\")"
                + " )",
            " ALTER TABLE \"t_pgForeignKeys_Tab\" ADD CONSTRAINT \"c_pgForeignKeys_fk\" FOREIGN KEY (\"id1\", \"id2\")"
                + " REFERENCES \"t_pgForeignKeys_Ref\" (\"id2\", \"id1\")");

    return new SharedTestCase(
        dbCreationStatements,
        ddl -> {
          Table pgTabTable = ddl.table("t_pgForeignKeys_Tab");
          assertThat(pgTabTable, notNullValue());
          assertThat(pgTabTable.foreignKeys(), hasSize(1));
          assertThat(
              pgTabTable.foreignKeys(),
              hasItems(
                  "ALTER TABLE \"t_pgForeignKeys_Tab\" ADD CONSTRAINT \"c_pgForeignKeys_fk\" FOREIGN KEY (\"id1\", \"id2\") REFERENCES \"t_pgForeignKeys_Ref\" (\"id2\", \"id1\") ON DELETE NO ACTION"));
        });
  }

  private SharedTestCase checkConstraints() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `t_checkConstraints_T` ("
                + " `id`     INT64 NOT NULL,"
                + " `A`      INT64 NOT NULL,"
                + " CONSTRAINT `c_checkConstraints_ck` CHECK(A>0),"
                + " ) PRIMARY KEY (`id` ASC)");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_checkConstraints_T");
          assertThat(table, notNullValue());
          assertThat(
              table.checkConstraints(), hasItems("CONSTRAINT `c_checkConstraints_ck` CHECK (A>0)"));
        });
  }

  private SharedTestCase pgCheckConstraints() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"t_pgCheckConstraints_T\" ("
                + " \"id\"     bigint NOT NULL,"
                + " \"A\"      bigint NOT NULL,"
                + " CONSTRAINT \"c_pgCheckConstraints_ck\" CHECK ((\"A\" > '0'::bigint)),"
                + " PRIMARY KEY (\"id\")"
                + " )");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table pgTable = ddl.table("t_pgCheckConstraints_T");
          assertThat(pgTable, notNullValue());
          assertThat(
              pgTable.checkConstraints(),
              hasItems("CONSTRAINT \"c_pgCheckConstraints_ck\" CHECK ((\"A\" > '0'::bigint))"));
        });
  }

  private SharedTestCase commitTimestamp() {
    String statement =
        "CREATE TABLE `t_commitTimestamp_Users` ("
            + " `id`                                    INT64 NOT NULL,"
            + " `birthday`                              TIMESTAMP NOT NULL "
            + " OPTIONS (allow_commit_timestamp=TRUE),"
            + " ) PRIMARY KEY (`id` ASC)";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_commitTimestamp_Users"), notNullValue());
          assertThat(
              ddl.table("t_commitTimestamp_Users").column("birthday").columnOptions(),
              hasItems("allow_commit_timestamp=TRUE"));
        });
  }

  private SharedTestCase pgCommitTimestamp() {
    String statement =
        "CREATE TABLE \"t_pgCommitTimestamp_Users\" ("
            + " \"id\"                                    bigint NOT NULL,"
            + " \"birthday\"                              spanner.commit_timestamp NOT NULL,"
            + " PRIMARY KEY (\"id\") )";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_pgCommitTimestamp_Users"), notNullValue());
          assertThat(
              ddl.table("t_pgCommitTimestamp_Users").column("birthday").type().toString(),
              equalTo("PG_SPANNER_COMMIT_TIMESTAMP"));
        });
  }

  private SharedTestCase generatedColumns() {
    String statement =
        "CREATE TABLE `t_generatedColumns_T` ("
            + " `id`                                     INT64 NOT NULL,"
            + " `generated`                              INT64 NOT NULL AS (`id`) STORED, "
            + " ) PRIMARY KEY (`id` ASC)";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_generatedColumns_T"), notNullValue());
          assertThat(
              ddl.table("t_generatedColumns_T").column("generated").generationExpression(),
              equalTo("`id`"));
        });
  }

  private SharedTestCase pgGeneratedColumns() {
    String statement =
        "CREATE TABLE \"t_pgGeneratedColumns_T\" ( \"id\"                     bigint NOT NULL,"
            + " \"generated_stored\" bigint NOT NULL GENERATED ALWAYS AS ((id / '1'::bigint)) STORED, "
            + " \"generated_virtual\" bigint GENERATED ALWAYS AS ((id / '1'::bigint)) VIRTUAL, "
            + " PRIMARY KEY (\"id\") )";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          assertThat(ddl.table("t_pgGeneratedColumns_T"), notNullValue());
          assertThat(
              ddl.table("t_pgGeneratedColumns_T").column("generated_stored").generationExpression(),
              equalTo("(id / '1'::bigint)"));
          assertThat(
              ddl.table("t_pgGeneratedColumns_T")
                  .column("generated_virtual")
                  .generationExpression(),
              equalTo("(id / '1'::bigint)"));
        });
  }

  private SharedTestCase defaultColumns() {
    String statement =
        "CREATE TABLE `t_defaultColumns_T` ("
            + " `id`                                     INT64 NOT NULL,"
            + " `generated`                              INT64 NOT NULL DEFAULT (10), "
            + " ) PRIMARY KEY (`id` ASC)";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          Table table = ddl.table("t_defaultColumns_T");
          assertThat(table.column("generated").defaultExpression(), equalTo("10"));
        });
  }

  private SharedTestCase pgDefaultColumns() {
    String statement =
        "CREATE TABLE \"t_pgDefaultColumns_T\" ( \"id\"                       bigint NOT NULL,"
            + " \"generated\"                              bigint NOT NULL DEFAULT '10'::bigint,"
            + " PRIMARY KEY (\"id\") )";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          Table table = ddl.table("t_pgDefaultColumns_T");
          assertThat(table.column("generated").defaultExpression(), equalTo("'10'::bigint"));
        });
  }

  private SharedTestCase onUpdateColumns() {
    String statement =
        "CREATE TABLE `t_onUpdateColumns_T` ("
            + " `id` INT64 NOT NULL,"
            + " `on_update` TIMESTAMP DEFAULT (PENDING_COMMIT_TIMESTAMP()) "
            + "    ON UPDATE (PENDING_COMMIT_TIMESTAMP()) "
            + "    OPTIONS (allow_commit_timestamp=TRUE),"
            + " ) PRIMARY KEY (`id` ASC)";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          Table table = ddl.table("t_onUpdateColumns_T");
          assertThat(
              table.column("on_update").onUpdateExpression(),
              equalTo("PENDING_COMMIT_TIMESTAMP()"));
        });
  }

  private SharedTestCase pgOnUpdateColumns() {
    String statement =
        "CREATE TABLE \"t_pgOnUpdateColumns_T\" ( \"id\"                       bigint NOT NULL,"
            + " \"on_update\" SPANNER.COMMIT_TIMESTAMP "
            + "    DEFAULT (SPANNER.PENDING_COMMIT_TIMESTAMP()) "
            + "    ON UPDATE (SPANNER.PENDING_COMMIT_TIMESTAMP()),"
            + " PRIMARY KEY (\"id\") )";

    return new SharedTestCase(
        Collections.singletonList(statement),
        ddl -> {
          Table table = ddl.table("t_pgOnUpdateColumns_T");
          assertThat(
              table.column("on_update").onUpdateExpression(),
              equalTo("spanner.pending_commit_timestamp()"));
        });
  }

  private SharedTestCase identityColumns() {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE `"
                + sharedSpannerResourceManager.getDatabaseId()
                + "` SET OPTIONS ( default_sequence_kind = \"bit_reversed_positive\" )",
            "CREATE TABLE `t_identityColumns_T` ("
                + " `id` INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY,"
                + " `non_key_col` INT64 NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),"
                + " ) PRIMARY KEY (`id` ASC)");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_identityColumns_T");
          assertThat(table.column("non_key_col").sequenceKind(), equalTo("bit_reversed_positive"));
        });
  }

  private SharedTestCase pgIdentityColumns() {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE \""
                + sharedPgSpannerResourceManager.getDatabaseId()
                + "\" SET spanner.default_sequence_kind = 'bit_reversed_positive'",
            "CREATE TABLE \"t_pgIdentityColumns_T\" ("
                + " \"id\" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY,"
                + " \"non_key_col\" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE),"
                + " PRIMARY KEY (\"id\") )");

    return new SharedTestCase(
        statements,
        ddl -> {
          Table table = ddl.table("t_pgIdentityColumns_T");
          assertThat(table.column("non_key_col").sequenceKind(), equalTo("bit_reversed_positive"));
        });
  }

  private SharedTestCase databaseOptions() {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE `"
                + sharedSpannerResourceManager.getDatabaseId()
                + "` SET OPTIONS ( version_retention_period = \"5d\" )\n",
            "CREATE TABLE `t_databaseOptions_Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE UNIQUE NULL_FILTERED INDEX `i_databaseOptions_a_last_name_idx` ON "
                + " `t_databaseOptions_Users`(`last_name` ASC) STORING (`first_name`)",
            " CREATE INDEX `i_databaseOptions_b_age_idx` ON `t_databaseOptions_Users`(`age` DESC)",
            " CREATE UNIQUE INDEX `i_databaseOptions_c_first_name_idx` ON `t_databaseOptions_Users`(`first_name` ASC)");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(ddl.databaseOptions().isEmpty(), is(false));
          assertThat(
              ddl.databaseOptions().stream().map(Object::toString).collect(Collectors.toList()),
              hasItem(
                  "option_name: \"version_retention_period\"\noption_type: \"STRING\"\noption_value: \"5d\"\n"));
        });
  }

  private SharedTestCase pgDatabaseOptions() {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE \""
                + sharedPgSpannerResourceManager.getDatabaseId()
                + "\" SET spanner.version_retention_period = '5d'\n",
            "CREATE TABLE \"t_pgDatabaseOptions_Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"age\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " ) ",
            " CREATE INDEX \"i_pgDatabaseOptions_a_last_name_idx\" ON "
                + " \"t_pgDatabaseOptions_Users\"(\"last_name\" ASC) INCLUDE (\"first_name\")",
            " CREATE INDEX \"i_pgDatabaseOptions_b_age_idx\" ON \"t_pgDatabaseOptions_Users\"(\"age\" DESC)",
            " CREATE INDEX \"i_pgDatabaseOptions_c_first_name_idx\" ON \"t_pgDatabaseOptions_Users\"(\"first_name\" ASC)");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(ddl.databaseOptions().isEmpty(), is(false));
          assertThat(
              ddl.databaseOptions().stream().map(Object::toString).collect(Collectors.toList()),
              hasItem(
                  "option_name: \"version_retention_period\"\noption_type: \"character varying\"\noption_value: \"5d\"\n"));
        });
  }

  private SharedTestCase changeStreams() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE `t_changeStreams_Account` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `balanceId`                             INT64 NOT NULL,"
                + " `balance`                               FLOAT64 NOT NULL,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE TABLE `t_changeStreams_Users` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `first_name`                            STRING(10),"
                + " `last_name`                             STRING(MAX),"
                + " `age`                                   INT64,"
                + " ) PRIMARY KEY (`id` ASC)",
            " CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamAll` FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")",
            " CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamEmpty` OPTIONS (retention_period=\"24h\")",
            " CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamKeyColumns` FOR `t_changeStreams_Account`(), `t_changeStreams_Users`()",
            " CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamTableColumns`"
                + " FOR `t_changeStreams_Account`, `t_changeStreams_Users`(`first_name`, `last_name`)");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(ddl.changeStreams().isEmpty(), is(false));
          List<String> changeStreamStrings =
              ddl.changeStreams().stream().map(Object::toString).collect(Collectors.toList());
          assertThat(
              changeStreamStrings,
              hasItems(
                  "CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamAll`\n\tFOR ALL\n\tOPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")",
                  "CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamEmpty`\n\tOPTIONS (retention_period=\"24h\")",
                  "CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamKeyColumns`\n\tFOR `t_changeStreams_Account`(), `t_changeStreams_Users`()",
                  "CREATE CHANGE STREAM `cs_changeStreams_ChangeStreamTableColumns`\n\tFOR `t_changeStreams_Account`, `t_changeStreams_Users`(`first_name`, `last_name`)"));
        });
  }

  private SharedTestCase pgChangeStreams() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE \"t_pgChangeStreams_Account\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"balanceId\"                             bigint NOT NULL,"
                + " \"balance\"                               double precision NOT NULL,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE TABLE \"t_pgChangeStreams_Users\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"first_name\"                            character varying(10),"
                + " \"last_name\"                             character varying,"
                + " \"age\"                                   bigint,"
                + " PRIMARY KEY (\"id\")"
                + " )",
            " CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamAll\" FOR ALL"
                + " WITH (retention_period='7d', value_capture_type='OLD_AND_NEW_VALUES')",
            " CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamEmpty\" WITH (retention_period='24h')",
            " CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamKeyColumns\" FOR \"t_pgChangeStreams_Account\"(), \"t_pgChangeStreams_Users\"()",
            " CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamTableColumns\""
                + " FOR \"t_pgChangeStreams_Account\", \"t_pgChangeStreams_Users\"(\"first_name\", \"last_name\")");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(ddl.changeStreams().isEmpty(), is(false));
          List<String> pgChangeStreamStrings =
              ddl.changeStreams().stream().map(Object::toString).collect(Collectors.toList());
          assertThat(
              pgChangeStreamStrings,
              hasItems(
                  "CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamAll\"\n\tFOR ALL\n\tWITH (retention_period='7d', value_capture_type='OLD_AND_NEW_VALUES')",
                  "CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamEmpty\"\n\tWITH (retention_period='24h')",
                  "CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamKeyColumns\"\n\tFOR \"t_pgChangeStreams_Account\"(), \"t_pgChangeStreams_Users\"()",
                  "CREATE CHANGE STREAM \"cs_pgChangeStreams_ChangeStreamTableColumns\"\n\tFOR \"t_pgChangeStreams_Account\", \"t_pgChangeStreams_Users\"(\"first_name\", \"last_name\")"));
        });
  }

  private SharedTestCase sequences() {
    DdlToAvroSchemaConverter converter =
        new DdlToAvroSchemaConverter("spannertest", "booleans", true);
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE `"
                + sharedSpannerResourceManager.getDatabaseId()
                + "` SET OPTIONS ( default_sequence_kind = \"bit_reversed_positive\" )",
            "CREATE SEQUENCE `s_sequences_mySequence` OPTIONS ("
                + "sequence_kind = \"bit_reversed_positive\")",
            "CREATE SEQUENCE `s_sequences_mySequence2` OPTIONS ("
                + "sequence_kind = \"bit_reversed_positive\","
                + "skip_range_min = 1,"
                + "skip_range_max = 1000,"
                + "start_with_counter = 100)",
            "CREATE SEQUENCE `s_sequences_mySequence3` OPTIONS ("
                + "skip_range_min = 1,"
                + "skip_range_max = 1000,"
                + "start_with_counter = 100)",
            "CREATE SEQUENCE `s_sequences_mySequence4`",
            "CREATE SEQUENCE `s_sequences_mySequence5` BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000 START COUNTER WITH 100",
            "CREATE TABLE `t_sequences_account` ("
                + " `id`        INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE s_sequences_mySequence)),"
                + " `balanceId` INT64 NOT NULL,"
                + " ) PRIMARY KEY (`id` ASC)");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.databaseOptions().stream().map(Object::toString).collect(Collectors.toList()),
              hasItem(
                  "option_name: \"default_sequence_kind\"\noption_type: \"STRING\"\noption_value: \"bit_reversed_positive\"\n"));

          assertThat(
              ddl.sequence("s_sequences_mySequence").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE `s_sequences_mySequence` OPTIONS (sequence_kind=\"bit_reversed_positive\")"));

          assertThat(
              ddl.sequence("s_sequences_mySequence2").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE `s_sequences_mySequence2` OPTIONS (sequence_kind=\"bit_reversed_positive\", skip_range_max=1000, skip_range_min=1, start_with_counter=100)"));

          assertThat(
              ddl.sequence("s_sequences_mySequence3").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE `s_sequences_mySequence3` OPTIONS (skip_range_max=1000, skip_range_min=1, start_with_counter=100)"));

          assertThat(
              ddl.sequence("s_sequences_mySequence4").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE `s_sequences_mySequence4` OPTIONS (sequence_kind=\"default\")"));

          assertThat(
              ddl.sequence("s_sequences_mySequence5").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE `s_sequences_mySequence5` OPTIONS (sequence_kind=\"bit_reversed_positive\", skip_range_max=1000, skip_range_min=1, start_with_counter=100)"));

          assertThat(
              ddl.table("t_sequences_account").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE TABLE `t_sequences_account` ( `id` INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE s_sequences_mySequence)), `balanceId` INT64 NOT NULL, ) PRIMARY KEY (`id` ASC)"));

          Collection<Schema> result = converter.convert(ddl);

          // Convert to a map to avoid iterator order flakiness
          Map<String, Schema> schemas =
              result.stream().collect(Collectors.toMap(Schema::getName, s -> s));

          Schema avroSchema1 = schemas.get("s_sequences_mySequence");
          assertThat(avroSchema1.getName(), equalTo("s_sequences_mySequence"));
          assertThat(
              avroSchema1.getProp("sequenceOption_0"),
              equalTo("sequence_kind=\"bit_reversed_positive\""));
          assertThat(avroSchema1.getProp("sequenceOption_1"), equalTo(null));

          Schema avroSchema2 = schemas.get("s_sequences_mySequence2");
          assertThat(avroSchema2.getName(), equalTo("s_sequences_mySequence2"));
          assertThat(
              avroSchema2.getProp("sequenceOption_0"),
              equalTo("sequence_kind=\"bit_reversed_positive\""));
          assertThat(avroSchema2.getProp("sequenceOption_1"), equalTo("skip_range_max=1000"));
          assertThat(avroSchema2.getProp("sequenceOption_2"), equalTo("skip_range_min=1"));
          assertThat(avroSchema2.getProp("sequenceOption_3"), equalTo("start_with_counter=100"));
          assertThat(avroSchema2.getProp("sequenceOption_4"), equalTo(null));

          Schema avroSchema3 = schemas.get("s_sequences_mySequence3");
          assertThat(avroSchema3.getName(), equalTo("s_sequences_mySequence3"));
          assertThat(avroSchema3.getProp("sequenceOption_0"), equalTo("skip_range_max=1000"));
          assertThat(avroSchema3.getProp("sequenceOption_1"), equalTo("skip_range_min=1"));
          assertThat(avroSchema3.getProp("sequenceOption_2"), equalTo("start_with_counter=100"));
          assertThat(avroSchema3.getProp("sequenceOption_3"), equalTo(null));

          Schema avroSchema4 = schemas.get("s_sequences_mySequence4");
          assertThat(avroSchema4.getName(), equalTo("s_sequences_mySequence4"));
          assertThat(avroSchema4.getProp("sequenceOption_0"), equalTo("sequence_kind=\"default\""));

          Schema avroSchema5 = schemas.get("s_sequences_mySequence5");
          assertThat(
              avroSchema5.getProp("sequenceOption_0"),
              equalTo("sequence_kind=\"bit_reversed_positive\""));
          assertThat(avroSchema5.getProp("sequenceOption_1"), equalTo("skip_range_max=1000"));
          assertThat(avroSchema5.getProp("sequenceOption_2"), equalTo("skip_range_min=1"));
          assertThat(avroSchema5.getProp("sequenceOption_3"), equalTo("start_with_counter=100"));
        });
  }

  private SharedTestCase pgSequences() {
    List<String> statements =
        Arrays.asList(
            "ALTER DATABASE \""
                + sharedPgSpannerResourceManager.getDatabaseId()
                + "\" SET spanner.default_sequence_kind = 'bit_reversed_positive'",
            "CREATE SEQUENCE \"s_pgSequences_myPGSequence\" BIT_REVERSED_POSITIVE",
            "CREATE SEQUENCE \"s_pgSequences_myPGSequence2\" BIT_REVERSED_POSITIVE"
                + " SKIP RANGE 1 1000 START COUNTER WITH 100",
            "CREATE SEQUENCE \"s_pgSequences_myPGSequence3\""
                + " SKIP RANGE 1 1000 START COUNTER WITH 100",
            "CREATE TABLE \"t_pgSequences_Account\" ("
                + " \"id\"        bigint DEFAULT nextval('\"s_pgSequences_myPGSequence\"'),"
                + " \"balanceId\" bigint NOT NULL,"
                + " PRIMARY KEY (\"id\"))");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.databaseOptions().stream().map(Object::toString).collect(Collectors.toList()),
              hasItem(
                  "option_name: \"default_sequence_kind\"\noption_type: \"character varying\"\noption_value: \"bit_reversed_positive\"\n"));

          assertThat(
              ddl.sequence("s_pgSequences_myPGSequence").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE \"s_pgSequences_myPGSequence\" BIT_REVERSED_POSITIVE START COUNTER WITH 1"));

          assertThat(
              ddl.sequence("s_pgSequences_myPGSequence2").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE \"s_pgSequences_myPGSequence2\" BIT_REVERSED_POSITIVE SKIP RANGE 1 1000 START COUNTER WITH 100"));

          assertThat(
              ddl.sequence("s_pgSequences_myPGSequence3").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE SEQUENCE \"s_pgSequences_myPGSequence3\" SKIP RANGE 1 1000 START COUNTER WITH 100"));

          assertThat(
              ddl.table("t_pgSequences_Account").prettyPrint(),
              equalToCompressingWhiteSpace(
                  "CREATE TABLE \"t_pgSequences_Account\" ( \"id\" bigint NOT NULL DEFAULT nextval('\"s_pgSequences_myPGSequence\"'::text), \"balanceId\" bigint NOT NULL, PRIMARY KEY (\"id\") )"));
        });
  }

  private SharedTestCase propertyGraphOnViewGroupBy() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE t_propertyGraphOnViewGroupBy_GraphTablePerson(loc_id INT64, pid INT64) PRIMARY KEY(loc_id, pid)",
            "CREATE TABLE t_propertyGraphOnViewGroupBy_GraphTableAccount(loc_id INT64, aid INT64, owner_id INT64, name"
                + " STRING(MAX), account_kind INT64, ProtoColumn BYTES(MAX), generated_enum_field"
                + " INT64, another_enum_field INT64) PRIMARY KEY(loc_id, aid)",
            "CREATE VIEW v_propertyGraphOnViewGroupBy_V_GroupByPerson SQL SECURITY INVOKER AS SELECT t.loc_id, t.pid, COUNT(*)"
                + " AS cnt FROM t_propertyGraphOnViewGroupBy_GraphTablePerson AS t GROUP BY t.loc_id, t.pid ORDER BY cnt DESC",
            "CREATE VIEW v_propertyGraphOnViewGroupBy_V_FilteredPerson SQL SECURITY INVOKER AS SELECT t.loc_id, t.pid FROM"
                + " t_propertyGraphOnViewGroupBy_GraphTablePerson AS t WHERE t.loc_id = 1",
            "CREATE PROPERTY GRAPH g_propertyGraphOnViewGroupBy_aml_view_complex\n"
                + "  NODE TABLES (\n"
                + "    v_propertyGraphOnViewGroupBy_V_GroupByPerson KEY (loc_id, pid) PROPERTIES(loc_id, pid, cnt),\n"
                + "    v_propertyGraphOnViewGroupBy_V_FilteredPerson KEY (loc_id, pid) PROPERTIES(loc_id, pid),\n"
                + "    t_propertyGraphOnViewGroupBy_GraphTableAccount KEY(loc_id, aid) PROPERTIES(loc_id, aid, owner_id, name,"
                + " account_kind, ProtoColumn, generated_enum_field, another_enum_field)\n"
                + "  )\n"
                + "  EDGE TABLES (\n"
                + "    t_propertyGraphOnViewGroupBy_GraphTableAccount AS Owns KEY(loc_id, aid)\n"
                + "      SOURCE KEY(loc_id, owner_id) REFERENCES v_propertyGraphOnViewGroupBy_V_FilteredPerson(loc_id, pid)\n"
                + "      DESTINATION KEY(loc_id, aid) REFERENCES t_propertyGraphOnViewGroupBy_GraphTableAccount(loc_id, aid)"
                + " PROPERTIES(loc_id, aid, owner_id)\n"
                + "  )");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.propertyGraphs().toString(),
              containsString(
                  "CREATE PROPERTY GRAPH g_propertyGraphOnViewGroupBy_aml_view_complex"));
        });
  }

  private SharedTestCase propertyGraphOnViewSimple() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE t_propertyGraphOnViewSimple_TableA(id INT64) PRIMARY KEY(id)",
            "CREATE VIEW v_propertyGraphOnViewSimple_ViewA SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewSimple_TableA.id FROM t_propertyGraphOnViewSimple_TableA",
            "CREATE TABLE t_propertyGraphOnViewSimple_TableB(id INT64) PRIMARY KEY(id)",
            "CREATE VIEW v_propertyGraphOnViewSimple_ViewB SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewSimple_TableB.id FROM t_propertyGraphOnViewSimple_TableB",
            "CREATE PROPERTY GRAPH g_propertyGraphOnViewSimple_aml_view_complex\n"
                + "  NODE TABLES (\n"
                + "    v_propertyGraphOnViewSimple_ViewA KEY (id),\n"
                + "    v_propertyGraphOnViewSimple_ViewB KEY (id)\n"
                + "  )\n");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.propertyGraphs().toString(),
              containsString("CREATE PROPERTY GRAPH g_propertyGraphOnViewSimple_aml_view_complex"));
        });
  }

  private SharedTestCase propertyGraphOnViewMixedOrder() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE t_propertyGraphOnViewMixedOrder_Parts(part_id INT64, part_name STRING(MAX)) PRIMARY KEY(part_id)",
            "CREATE VIEW v_propertyGraphOnViewMixedOrder_PartView SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewMixedOrder_Parts.part_id, t_propertyGraphOnViewMixedOrder_Parts.part_name FROM t_propertyGraphOnViewMixedOrder_Parts",
            "CREATE TABLE t_propertyGraphOnViewMixedOrder_Suppliers(supplier_id INT64, supplier_name STRING(MAX)) PRIMARY"
                + " KEY(supplier_id)",
            "CREATE VIEW v_propertyGraphOnViewMixedOrder_SupplierView SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewMixedOrder_Suppliers.supplier_id, t_propertyGraphOnViewMixedOrder_Suppliers.supplier_name"
                + " FROM t_propertyGraphOnViewMixedOrder_Suppliers",
            "CREATE TABLE t_propertyGraphOnViewMixedOrder_PartSuppliers(part_id INT64, supplier_id INT64) PRIMARY KEY(part_id,"
                + " supplier_id)",
            "CREATE VIEW v_propertyGraphOnViewMixedOrder_PartSuppliersView SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewMixedOrder_PartSuppliers.part_id, t_propertyGraphOnViewMixedOrder_PartSuppliers.supplier_id"
                + " FROM t_propertyGraphOnViewMixedOrder_PartSuppliers",
            "CREATE PROPERTY GRAPH g_propertyGraphOnViewMixedOrder_SupplyChainGraph\n"
                + "  NODE TABLES (\n"
                + "    v_propertyGraphOnViewMixedOrder_PartView KEY (part_id),\n"
                + "    v_propertyGraphOnViewMixedOrder_SupplierView KEY (supplier_id)\n"
                + "  )\n"
                + "  EDGE TABLES (\n"
                + "    v_propertyGraphOnViewMixedOrder_PartSuppliersView KEY (part_id, supplier_id)\n"
                + "      SOURCE KEY (part_id) REFERENCES v_propertyGraphOnViewMixedOrder_PartView(part_id)\n"
                + "      DESTINATION KEY (supplier_id) REFERENCES v_propertyGraphOnViewMixedOrder_SupplierView(supplier_id)\n"
                + "  )");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.propertyGraphs().toString(),
              containsString(
                  "CREATE PROPERTY GRAPH g_propertyGraphOnViewMixedOrder_SupplyChainGraph"));
        });
  }

  private SharedTestCase propertyGraphOnViewTablesFirst() {
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE t_propertyGraphOnViewTablesFirst_Parts2(part_id INT64, part_name STRING(MAX)) PRIMARY KEY(part_id)",
            "CREATE TABLE t_propertyGraphOnViewTablesFirst_Suppliers2(supplier_id INT64, supplier_name STRING(MAX)) PRIMARY"
                + " KEY(supplier_id)",
            "CREATE TABLE t_propertyGraphOnViewTablesFirst_PartSuppliers2(part_id INT64, supplier_id INT64) PRIMARY KEY(part_id,"
                + " supplier_id)",
            "CREATE VIEW v_propertyGraphOnViewTablesFirst_PartView2 SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewTablesFirst_Parts2.part_id, t_propertyGraphOnViewTablesFirst_Parts2.part_name FROM"
                + " t_propertyGraphOnViewTablesFirst_Parts2",
            "CREATE VIEW v_propertyGraphOnViewTablesFirst_SupplierView2 SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewTablesFirst_Suppliers2.supplier_id, t_propertyGraphOnViewTablesFirst_Suppliers2.supplier_name"
                + " FROM t_propertyGraphOnViewTablesFirst_Suppliers2",
            "CREATE VIEW v_propertyGraphOnViewTablesFirst_PartSuppliersView2 SQL SECURITY INVOKER AS SELECT t_propertyGraphOnViewTablesFirst_PartSuppliers2.part_id, t_propertyGraphOnViewTablesFirst_PartSuppliers2.supplier_id"
                + " FROM t_propertyGraphOnViewTablesFirst_PartSuppliers2",
            "CREATE PROPERTY GRAPH g_propertyGraphOnViewTablesFirst_SupplyChainGraph2\n"
                + "  NODE TABLES (\n"
                + "    v_propertyGraphOnViewTablesFirst_PartView2 KEY (part_id),\n"
                + "    v_propertyGraphOnViewTablesFirst_SupplierView2 KEY (supplier_id)\n"
                + "  )\n"
                + "  EDGE TABLES (\n"
                + "    v_propertyGraphOnViewTablesFirst_PartSuppliersView2 KEY (part_id, supplier_id)\n"
                + "      SOURCE KEY (part_id) REFERENCES v_propertyGraphOnViewTablesFirst_PartView2(part_id)\n"
                + "      DESTINATION KEY (supplier_id) REFERENCES v_propertyGraphOnViewTablesFirst_SupplierView2(supplier_id)\n"
                + "  )");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.propertyGraphs().toString(),
              containsString(
                  "CREATE PROPERTY GRAPH g_propertyGraphOnViewTablesFirst_SupplyChainGraph2"));
        });
  }

  private SharedTestCase propertyGraphOnViewWithNamedSchema() {
    List<String> statements =
        Arrays.asList(
            "CREATE SCHEMA s_propertyGraphOnViewWithNamedSchema_Sch1",
            "CREATE SCHEMA s_propertyGraphOnViewWithNamedSchema_Sch2",
            "CREATE TABLE s_propertyGraphOnViewWithNamedSchema_Sch1.Account("
                + "    AccountID INT64 NOT NULL,"
                + "    Money FLOAT64,"
                + "    AnotherMoney FLOAT64"
                + "  ) PRIMARY KEY(AccountID)",
            "CREATE VIEW v_propertyGraphOnViewWithNamedSchema_V0 SQL SECURITY INVOKER AS SELECT Account.AccountID, Account.Money FROM s_propertyGraphOnViewWithNamedSchema_Sch1.Account",
            "CREATE VIEW s_propertyGraphOnViewWithNamedSchema_Sch1.V1 SQL SECURITY INVOKER AS SELECT Account.AccountID, Account.Money FROM s_propertyGraphOnViewWithNamedSchema_Sch1.Account",
            "CREATE VIEW s_propertyGraphOnViewWithNamedSchema_Sch2.V2 SQL SECURITY INVOKER AS SELECT Account.AccountID, Account.Money FROM s_propertyGraphOnViewWithNamedSchema_Sch1.Account",
            "CREATE PROPERTY GRAPH g_propertyGraphOnViewWithNamedSchema_aml "
                + "    NODE TABLES ("
                + "      v_propertyGraphOnViewWithNamedSchema_V0 KEY(AccountID) PROPERTIES(AccountID, Money),"
                + "      s_propertyGraphOnViewWithNamedSchema_Sch1.V1 KEY(AccountID) PROPERTIES(AccountID, Money),"
                + "      s_propertyGraphOnViewWithNamedSchema_Sch2.V2 KEY(AccountID) PROPERTIES(AccountID, Money)"
                + "    )");

    return new SharedTestCase(
        statements,
        ddl -> {
          assertThat(
              ddl.propertyGraphs().toString(),
              containsString("CREATE PROPERTY GRAPH g_propertyGraphOnViewWithNamedSchema_aml"));
        });
  }

  @Test
  public void sharedInformationSchemaScannerTestGsql() throws Exception {
    List<SharedTestCase> testCases =
        Arrays.asList(
            tableWithAllTypes(),
            simpleModel(),
            simplePropertyGraph(),
            dynamicPropertyGraph(),
            simpleView(),
            simpleUdf(),
            interleavedIn(),
            reserved(),
            indexes(),
            searchIndexes(),
            vectorIndexes(),
            foreignKeys(),
            checkConstraints(),
            commitTimestamp(),
            generatedColumns(),
            defaultColumns(),
            onUpdateColumns(),
            identityColumns(),
            databaseOptions(),
            changeStreams(),
            sequences(),
            propertyGraphOnViewGroupBy(),
            propertyGraphOnViewSimple(),
            propertyGraphOnViewMixedOrder(),
            propertyGraphOnViewTablesFirst(),
            propertyGraphOnViewWithNamedSchema());

    List<String> allStatements = new ArrayList<>();
    for (SharedTestCase testCase : testCases) {
      allStatements.addAll(testCase.getDdlStatements());
    }
    sharedSpannerResourceManager.executeDdlStatements(allStatements);
    Ddl ddl = getDatabaseDdl(sharedSpannerResourceManager);

    for (SharedTestCase testCase : testCases) {
      testCase.getAssertions().accept(ddl);
    }
  }

  @Test
  public void sharedInformationSchemaScannerTestPg() throws Exception {
    List<SharedTestCase> testCases =
        Arrays.asList(
            tableWithAllPgTypes(),
            pgSimpleView(),
            pgInterleavedIn(),
            pgReserved(),
            pgSearchIndexes(),
            pgVectorIndexes(),
            pgIndexes(),
            pgForeignKeys(),
            pgCheckConstraints(),
            pgCommitTimestamp(),
            pgGeneratedColumns(),
            pgDefaultColumns(),
            pgOnUpdateColumns(),
            pgIdentityColumns(),
            pgDatabaseOptions(),
            pgChangeStreams(),
            pgSequences());

    List<String> allStatements = new ArrayList<>();
    for (SharedTestCase testCase : testCases) {
      allStatements.addAll(testCase.getDdlStatements());
    }
    sharedPgSpannerResourceManager.executeDdlStatements(allStatements);
    Ddl ddl = getPgDatabaseDdl(sharedPgSpannerResourceManager);

    for (SharedTestCase testCase : testCases) {
      testCase.getAssertions().accept(ddl);
    }
  }

  @Test
  public void placementsAndPlacementTables() throws Exception {
    setupResourceManager(Dialect.GOOGLE_STANDARD_SQL);
    try {
      List<String> statements =
          Arrays.asList(
              "ALTER DATABASE `"
                  + spannerResourceManager.getDatabaseId()
                  + "` SET OPTIONS ( opt_in_dataplacement_preview = TRUE )\n\n",
              "CREATE PLACEMENT `pl1_placements`\n\tOPTIONS (instance_partition=\""
                  + INSTANCE_PARTITION_ID
                  + "\")\n",
              "CREATE PLACEMENT `pl2_placements`\n\tOPTIONS (default_leader=\"us-east1\", instance_partition=\""
                  + INSTANCE_PARTITION_ID
                  + "\")\n",
              "CREATE PLACEMENT `pl3_placements`\n\tOPTIONS (default_leader=\"us-east4\", instance_partition=\""
                  + INSTANCE_PARTITION_ID
                  + "\")",
              "CREATE TABLE `t_placementTables_PlacementKeyAsPrimaryKey` (\n\t"
                  + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n\t"
                  + "`val`                                   STRING(MAX),\n"
                  + ") PRIMARY KEY (`location` ASC)\n\n\n",
              "CREATE TABLE `t_placementTables_PlacedUsers` (\n\t"
                  + "`location`                              STRING(MAX) NOT NULL,\n\t"
                  + "`user_id`                               INT64 NOT NULL,\n"
                  + ") PRIMARY KEY (`location` ASC, `user_id` ASC),\n"
                  + "INTERLEAVE IN PARENT `t_placementTables_PlacementKeyAsPrimaryKey`\n\n\n",
              "CREATE TABLE `t_placementTables_UsersByPlacement` (\n\t"
                  + "`user_id`                               INT64 NOT NULL,\n\t"
                  + "`location`                              STRING(MAX) NOT NULL PLACEMENT KEY,\n"
                  + ") PRIMARY KEY (`user_id` ASC)\n\n");

      spannerResourceManager.executeDdlStatements(statements);
      Ddl ddl = getDatabaseDdl(spannerResourceManager);
      assertThat(
          ddl.placement("pl1_placements").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(1)));
      assertThat(
          ddl.placement("pl2_placements").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(2)));
      assertThat(
          ddl.placement("pl3_placements").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(3)));
      assertThat(
          ddl.table("t_placementTables_PlacementKeyAsPrimaryKey").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(4)));
      assertThat(
          ddl.table("t_placementTables_PlacedUsers").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(5)));
      assertThat(
          ddl.table("t_placementTables_UsersByPlacement").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(6)));
    } finally {
      if (spannerResourceManager != null) {
        spannerResourceManager.cleanupAll();
      }
    }
  }

  @Test
  public void pgPlacementTables() throws Exception {
    setupResourceManager(Dialect.POSTGRESQL);
    try {
      List<String> statements =
          Arrays.asList(
              "ALTER DATABASE \""
                  + pgSpannerResourceManager.getDatabaseId()
                  + "\" SET spanner.opt_in_dataplacement_preview = TRUE\n",
              " CREATE TABLE \"t_pgPlacementTables_PlacementKeyAsPrimaryKey\" ("
                  + " \"location\"                              character varying NOT NULL PLACEMENT KEY,"
                  + " \"val\"                                   character varying,"
                  + " PRIMARY KEY (\"location\")"
                  + " )",
              " CREATE TABLE \"t_pgPlacementTables_PlacedUsers\" ("
                  + " \"location\"                              character varying NOT NULL,"
                  + " \"user_id\"                               character varying NOT NULL,"
                  + " PRIMARY KEY (\"location\", \"user_id\") "
                  + ") INTERLEAVE IN PARENT \"t_pgPlacementTables_PlacementKeyAsPrimaryKey\"",
              " CREATE TABLE \"t_pgPlacementTables_UsersWithPlacement\" ("
                  + " \"user_id\"                               bigint NOT NULL,"
                  + " \"location\"                              character varying NOT NULL PLACEMENT KEY,"
                  + " PRIMARY KEY (\"user_id\")"
                  + " )");
      // Validate the PLACEMENT KEY constraint is available in placement tables.
      pgSpannerResourceManager.executeDdlStatements(statements);
      Ddl ddl = getPgDatabaseDdl(pgSpannerResourceManager);
      assertThat(
          ddl.table("t_pgPlacementTables_PlacementKeyAsPrimaryKey").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(1)));
      assertThat(
          ddl.table("t_pgPlacementTables_PlacedUsers").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(2)));
      assertThat(
          ddl.table("t_pgPlacementTables_UsersWithPlacement").prettyPrint(),
          equalToCompressingWhiteSpace(statements.get(3)));
    } finally {
      if (pgSpannerResourceManager != null) {
        pgSpannerResourceManager.cleanupAll();
      }
    }
  }
}
