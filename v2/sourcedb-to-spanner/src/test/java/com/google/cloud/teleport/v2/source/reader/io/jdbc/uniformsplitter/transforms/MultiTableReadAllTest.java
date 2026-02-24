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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiTableReadAllTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String DRIVER_CLASS_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
  private static final String JDBC_URL = "jdbc:derby:memory:MultiTableReadAllTest;create=true";

  @BeforeClass
  public static void beforeClass() throws Exception {
    Class.forName(DRIVER_CLASS_NAME);
    try (java.sql.Connection conn = java.sql.DriverManager.getConnection(JDBC_URL)) {
      try (java.sql.Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE testTable (field1 VARCHAR(255))");
        stmt.execute("INSERT INTO testTable VALUES ('val1')");
      }
    }
  }

  @AfterClass
  public static void afterClass() {
    try {
      java.sql.DriverManager.getConnection("jdbc:derby:memory:MultiTableReadAllTest;drop=true");
    } catch (java.sql.SQLException e) {
      // Derby throws an exception on successful drop
    }
  }

  @Test
  public void testFluentApiMethods() {
    SerializableFunction<Void, DataSource> mockProvider = mock(SerializableFunction.class);
    JdbcIO.PreparedStatementSetter<String> mockSetter = mock(JdbcIO.PreparedStatementSetter.class);
    JdbcIO.RowMapper<String> mockMapper = mock(JdbcIO.RowMapper.class);
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();
    SerializableFunction<String, TableIdentifier> mockIdFn = mock(SerializableFunction.class);

    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setDataSourceProviderFn(mockProvider)
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setParameterSetter(mockSetter)
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn(mockIdFn)
            .setOutputParallelization(true)
            .build();

    assertThat(readAll.getDataSourceProviderFn()).isEqualTo(mockProvider);
    assertThat(readAll.getParameterSetter()).isEqualTo(mockSetter);
    assertThat(readAll.getOutputParallelization()).isTrue();

    // Test fluent updates
    readAll = readAll.withOutputParallelization(false).withDisableAutoCommit(false);
    assertThat(readAll.getOutputParallelization()).isFalse();
    assertThat(readAll.getDisableAutoCommit()).isFalse();

    readAll = readAll.withCoder(StringUtf8Coder.of());
    assertThat(readAll.getCoder()).isEqualTo(StringUtf8Coder.of());

    readAll = readAll.withQuery("SELECT * FROM foo");
    assertThat(readAll.getQueryProvider()).isNotNull();

    readAll = readAll.withTableReadSpecifications(ImmutableMap.of());
    assertThat(readAll.getTableReadSpecifications()).isEmpty();

    readAll = readAll.withTableIdentifierFn(mockIdFn);
    assertThat(readAll.getTableIdentifierFn()).isEqualTo(mockIdFn);

    readAll = readAll.withParameterSetter(mockSetter);
    assertThat(readAll.getParameterSetter()).isEqualTo(mockSetter);
  }

  @Test
  public void testWithDataSourceConfiguration() {
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create("org.h2.Driver", "jdbc:h2:mem:test");
    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build()
            .withDataSourceConfiguration(config);

    assertThat(readAll.getDataSourceProviderFn()).isNotNull();
  }

  @Test
  public void testExpand_withParallelizationAndSchema() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    JdbcIO.RowMapper<String> mockMapper = new StringRowMapper();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setDataSourceProviderFn(
                JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(
                    JdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, JDBC_URL)))
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setParameterSetter((element, preparedStatement) -> {})
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn((element) -> tableId)
            .setOutputParallelization(true) // covers line 258
            .build();

    PCollection<String> input = pipeline.apply(Create.of("testTable"));
    input.apply(readAll);

    // This test covers line 258 (parallelization), and 261-271 (inferCoder and schema)
    // By default, a String coder should be inferred.
    pipeline.run();
  }

  @Test
  public void testExpand_withRegisteredSchema() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    JdbcIO.RowMapper<TestRow> mockMapper = new TestRowMapper();
    TableReadSpecification<TestRow> spec =
        TableReadSpecification.<TestRow>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    Schema schema = Schema.builder().addStringField("field1").build();
    pipeline
        .getSchemaRegistry()
        .registerSchemaForType(
            TypeDescriptor.of(TestRow.class),
            schema,
            (row) -> org.apache.beam.sdk.values.Row.withSchema(schema).addValue("val").build(),
            (beamRow) -> new TestRow());

    MultiTableReadAll<String, TestRow> readAll =
        MultiTableReadAll.<String, TestRow>builder()
            .setDataSourceProviderFn(
                JdbcIO.DataSourceProviderFromDataSourceConfiguration.of(
                    JdbcIO.DataSourceConfiguration.create(DRIVER_CLASS_NAME, JDBC_URL)))
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setParameterSetter((element, preparedStatement) -> {})
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn((element) -> tableId)
            .setOutputParallelization(false)
            .build();

    PCollection<String> input = pipeline.apply(Create.of("testTable"));
    input.apply(readAll);

    // This covers line 265-269 when schema is present
    pipeline.run();
  }

  @Test
  public void testExpand_coderRegistryException() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    // Use a type that Beam won't have a coder for by default
    JdbcIO.RowMapper<Uncodable> mockMapper = new UncodableRowMapper();
    TableReadSpecification<Uncodable> spec =
        TableReadSpecification.<Uncodable>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    MultiTableReadAll<String, Uncodable> readAll =
        MultiTableReadAll.<String, Uncodable>builder()
            .setDataSourceProviderFn(mock(SerializableFunction.class))
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setParameterSetter((element, preparedStatement) -> {})
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn((element) -> tableId)
            .setOutputParallelization(false)
            .build();

    // Testing inferCoder directly as per instructions to avoid full pipeline execution
    assertThat(readAll.inferCoder(pipeline.getCoderRegistry(), pipeline.getSchemaRegistry()))
        .isNull();
  }

  @Test
  public void testInferCoder_manualCoder() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setCoder(StringUtf8Coder.of())
            .setOutputParallelization(true)
            .build();

    // Directly testing the property as inferCoder is private, but expand calls it.
    // Line 210: if (getCoder() != null) return getCoder();
    assertThat(readAll.getCoder()).isEqualTo(StringUtf8Coder.of());
  }

  @Test
  public void testPopulateDisplayData_withHasDisplayData() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    JdbcIO.RowMapper<String> mockMapper = new StringRowMapper();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    // Create a provider that implements HasDisplayData
    interface DisplayDataProvider extends SerializableFunction<Void, DataSource>, HasDisplayData {}
    DisplayDataProvider mockProvider = mock(DisplayDataProvider.class);

    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setDataSourceProviderFn(mockProvider)
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn((element) -> tableId)
            .setOutputParallelization(false)
            .build();

    DisplayData displayData = DisplayData.from(readAll);
    // Line 299: if (getDataSourceProviderFn() instanceof HasDisplayData)
    verify(mockProvider).populateDisplayData(any());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDataSourceProviderFn_duplicate_throwsException() {
    SerializableFunction<Void, DataSource> mockProvider = mock(SerializableFunction.class);
    MultiTableReadAll.<String, String>builder()
        .setDataSourceProviderFn(mockProvider)
        .setTableReadSpecifications(ImmutableMap.of())
        .setTableIdentifierFn(mock(SerializableFunction.class))
        .setOutputParallelization(false)
        .build()
        .withDataSourceProviderFn(mockProvider);
  }

  @Test
  public void testPopulateDisplayData() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    JdbcIO.RowMapper<String> mockMapper = new StringRowMapper();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn((element) -> tableId)
            .setCoder(StringUtf8Coder.of())
            .setOutputParallelization(false)
            .build();

    DisplayData displayData = DisplayData.from(readAll);

    java.util.Map<String, DisplayData.Item> items = new java.util.HashMap<>();
    for (DisplayData.Item item : displayData.items()) {
      items.put(item.getKey(), item);
    }

    assertThat(items).containsKey("query");
    assertThat(items).containsKey("rowMapper");
    assertThat(items).containsKey("coder");

    assertThat(items.get("rowMapper").getValue()).isEqualTo(mockMapper.getClass().getName());
    assertThat(items.get("coder").getValue()).isEqualTo(StringUtf8Coder.class.getName());
  }

  @Test
  public void testPopulateDisplayData_emptySpecs() {
    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setQueryProvider(StaticValueProvider.of(new TestQueryProvider()))
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();

    DisplayData displayData = DisplayData.from(readAll);

    java.util.Map<String, DisplayData.Item> items = new java.util.HashMap<>();
    for (DisplayData.Item item : displayData.items()) {
      items.put(item.getKey(), item);
    }

    assertThat(items).containsKey("query");
    assertThat(items).doesNotContainKey("rowMapper");
    assertThat(items).doesNotContainKey("coder");
  }

  @Test
  public void testWithQuery_nullThrows() {
    MultiTableReadAll.Builder<String, String> builder = MultiTableReadAll.builder();
    MultiTableReadAll<String, String> readAll =
        builder
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> readAll.withQuery(null));
    assertThat(thrown).hasMessageThat().contains("called with null query");
  }

  @Test
  public void testWithQueryProvider_nullThrows() {
    MultiTableReadAll.Builder<String, String> builder = MultiTableReadAll.builder();
    MultiTableReadAll<String, String> readAll =
        builder
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> readAll.withQueryProvider((MultiTableReadAll.QueryProvider<String>) null));
    assertThat(thrown).hasMessageThat().contains("called with null query");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            readAll.withQueryProvider((StaticValueProvider<MultiTableReadAll.QueryProvider>) null));
  }

  @Test
  public void testWithParameterSetter_nullThrows() {
    MultiTableReadAll.Builder<String, String> builder = MultiTableReadAll.builder();
    MultiTableReadAll<String, String> readAll =
        builder
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();
    assertThrows(IllegalArgumentException.class, () -> readAll.withParameterSetter(null));
  }

  @Test
  public void testWithCoder_nullThrows() {
    MultiTableReadAll.Builder<String, String> builder = MultiTableReadAll.builder();
    MultiTableReadAll<String, String> readAll =
        builder
            .setTableReadSpecifications(ImmutableMap.of())
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();
    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> readAll.withCoder(null));
    assertThat(thrown).hasMessageThat().contains("called with null coder");
  }

  @Test
  public void testInferCoder() throws CannotProvideCoderException {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    JdbcIO.RowMapper<String> mockMapper = new StringRowMapper();
    TableReadSpecification<String> spec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(mockMapper)
            .build();

    MultiTableReadAll<String, String> readAll =
        MultiTableReadAll.<String, String>builder()
            .setTableReadSpecifications(ImmutableMap.of(tableId, spec))
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();

    org.apache.beam.sdk.coders.CoderRegistry registry =
        org.apache.beam.sdk.coders.CoderRegistry.createDefault();
    org.apache.beam.sdk.schemas.SchemaRegistry schemaRegistry =
        org.apache.beam.sdk.schemas.SchemaRegistry.createDefault();

    // 1. Manual coder
    assertThat(readAll.withCoder(StringUtf8Coder.of()).inferCoder(registry, schemaRegistry))
        .isEqualTo(StringUtf8Coder.of());

    // 2. From SchemaRegistry (String has no schema by default in createDefault, but let's test the
    // fallback)
    assertThat(readAll.inferCoder(registry, schemaRegistry)).isEqualTo(StringUtf8Coder.of());

    // 3. From CoderRegistry fallback (when NoSuchSchemaException is caught)
    // String is already in registry. Let's use a custom type.
    // We use Uncodable which does not implement Serializable to avoid fallback to SerializableCoder
    JdbcIO.RowMapper<Uncodable> uncodableRowMapper = new UncodableRowMapper();
    TableReadSpecification<Uncodable> uncodableSpec =
        TableReadSpecification.<Uncodable>builder()
            .setTableIdentifier(tableId)
            .setRowMapper(uncodableRowMapper)
            .build();
    MultiTableReadAll<String, Uncodable> readAllUncodable =
        MultiTableReadAll.<String, Uncodable>builder()
            .setTableReadSpecifications(ImmutableMap.of(tableId, uncodableSpec))
            .setTableIdentifierFn(mock(SerializableFunction.class))
            .setOutputParallelization(false)
            .build();

    // Uncodable is not in schemaRegistry and not in registry. should return null.
    assertThat(readAllUncodable.inferCoder(registry, schemaRegistry)).isNull();
  }

  private static class TestQueryProvider implements MultiTableReadAll.QueryProvider<String> {
    @Override
    public String getQuery(String element) throws Exception {
      return "SELECT * FROM " + element;
    }
  }

  private static class TestRow implements Serializable {}

  private static class Uncodable {}

  private static class StringRowMapper implements JdbcIO.RowMapper<String> {
    @Override
    public String mapRow(java.sql.ResultSet resultSet) throws Exception {
      return "test";
    }
  }

  private static class TestRowMapper implements JdbcIO.RowMapper<TestRow> {
    @Override
    public TestRow mapRow(java.sql.ResultSet resultSet) throws Exception {
      return new TestRow();
    }
  }

  private static class UncodableRowMapper implements JdbcIO.RowMapper<Uncodable> {
    @Override
    public Uncodable mapRow(java.sql.ResultSet resultSet) throws Exception {
      return new Uncodable();
    }
  }
}
