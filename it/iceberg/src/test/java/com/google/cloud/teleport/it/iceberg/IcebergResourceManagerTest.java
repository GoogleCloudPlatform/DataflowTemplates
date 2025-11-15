/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.it.iceberg;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link IcebergResourceManager}. */
@RunWith(JUnit4.class)
public class IcebergResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String TEST_ID = "test-id";
  private static final String CATALOG_NAME = "test-catalog";
  private static final String NAMESPACE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final TableIdentifier TABLE_IDENTIFIER =
      TableIdentifier.of(NAMESPACE_NAME, TABLE_NAME);
  private static final Schema TABLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Mock(extraInterfaces = SupportsNamespaces.class)
  private Catalog catalog;

  @Mock private Table table;
  @Mock private FileIO fileIO;

  private MockedStatic<CatalogUtil> catalogUtilMock;

  private IcebergResourceManager testManager;

  @Before
  public void setUp() {
    catalogUtilMock = Mockito.mockStatic(CatalogUtil.class);
    catalogUtilMock
        .when(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()))
        .thenReturn(catalog);

    testManager = IcebergResourceManager.builder(TEST_ID).setCatalogName(CATALOG_NAME).build();
  }

  @After
  public void tearDown() {
    catalogUtilMock.close();
  }

  @Test
  public void testBuilderWithDefaultParameters() {
    IcebergResourceManager manager = IcebergResourceManager.builder(TEST_ID).build();
    assertThat(manager).isNotNull();
  }

  @Test
  public void testBuilderWithAllParameters() {
    IcebergResourceManager manager =
        IcebergResourceManager.builder(TEST_ID)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(Map.of("key", "value"))
            .setConfigProperties(Map.of("config-key", "config-value"))
            .build();
    assertThat(manager).isNotNull();
  }

  @Test
  public void testCatalogIsCached() {
    testManager.catalog();
    testManager.catalog();

    catalogUtilMock.verify(() -> CatalogUtil.buildIcebergCatalog(any(), any(), any()), times(1));
  }

  @Test
  public void testCreateNamespace() {
    boolean result = testManager.createNamespace(NAMESPACE_NAME);

    assertThat(result).isTrue();
    verify((SupportsNamespaces) catalog).createNamespace(Namespace.of(NAMESPACE_NAME));
  }

  @Test
  public void testCreateNamespaceThatAlreadyExists() {
    Mockito.doThrow(AlreadyExistsException.class)
        .when((SupportsNamespaces) catalog)
        .createNamespace(any(Namespace.class));

    boolean result = testManager.createNamespace(NAMESPACE_NAME);
    assertThat(result).isFalse();
  }

  @Test
  public void testCreateTable() {
    when(catalog.createTable(TABLE_IDENTIFIER, TABLE_SCHEMA)).thenReturn(table);

    Table result = testManager.createTable(TABLE_IDENTIFIER.toString(), TABLE_SCHEMA);

    assertThat(result).isEqualTo(table);
    verify(catalog).createTable(TABLE_IDENTIFIER, TABLE_SCHEMA);
  }

  @Test
  public void testCreateTableThatAlreadyExists() {
    when(catalog.createTable(TABLE_IDENTIFIER, TABLE_SCHEMA))
        .thenThrow(AlreadyExistsException.class);

    assertThrows(
        IcebergResourceManagerException.class,
        () -> testManager.createTable(TABLE_IDENTIFIER.toString(), TABLE_SCHEMA));
  }

  @Test
  public void testLoadTable() {
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);

    Table result = testManager.loadTable(TABLE_IDENTIFIER.toString());

    assertThat(result).isEqualTo(table);
    verify(catalog).loadTable(TABLE_IDENTIFIER);
  }

  @Test
  public void testLoadTableThatDoesNotExist() {
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenThrow(NoSuchTableException.class);

    assertThrows(
        IcebergResourceManagerException.class,
        () -> testManager.loadTable(TABLE_IDENTIFIER.toString()));
  }

  @Test
  public void testReadTable() {
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
    Record mockRecord = mock(Record.class);
    CloseableIterable<Record> records = CloseableIterable.withNoopClose(List.of(mockRecord));

    try (MockedStatic<org.apache.iceberg.data.IcebergGenerics> genericsMock =
        Mockito.mockStatic(org.apache.iceberg.data.IcebergGenerics.class)) {
      org.apache.iceberg.data.IcebergGenerics.ScanBuilder scanBuilder =
          mock(org.apache.iceberg.data.IcebergGenerics.ScanBuilder.class);
      genericsMock
          .when(() -> org.apache.iceberg.data.IcebergGenerics.read(table))
          .thenReturn(scanBuilder);
      when(scanBuilder.build()).thenReturn(records);

      List<Record> result = testManager.read(TABLE_IDENTIFIER.toString());

      assertThat(result).containsExactly(mockRecord);
    }
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testWriteTableSucceeds() {
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
    when(table.io()).thenReturn(fileIO);
    when(table.location()).thenReturn("/tmp/warehouse");
    when(table.schema()).thenReturn(TABLE_SCHEMA);
    OutputFile mockOutputFile = mock(OutputFile.class);
    when(fileIO.newOutputFile(any(String.class))).thenReturn(mockOutputFile);
    InputFile mockInputFile = mock(InputFile.class);
    when(fileIO.newInputFile(any(String.class))).thenReturn(mockInputFile);

    FileAppender mockAppender = mock(FileAppender.class);
    AppendFiles mockAppendFiles = mock(AppendFiles.class);
    when(table.newFastAppend()).thenReturn(mockAppendFiles);

    try (MockedStatic<Parquet> parquetMock =
            Mockito.mockStatic(Parquet.class, Mockito.RETURNS_DEEP_STUBS);
        MockedStatic<DataFiles> dataFilesMock =
            Mockito.mockStatic(DataFiles.class, Mockito.RETURNS_DEEP_STUBS)) {
      Parquet.WriteBuilder mockWriterBuilder = mock(Parquet.WriteBuilder.class);
      parquetMock.when(() -> Parquet.write(any(OutputFile.class))).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.createWriterFunc(any())).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.schema(any(Schema.class))).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.overwrite()).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.build()).thenReturn(mockAppender);
      DataFile mockDataFile = mock(DataFile.class);
      when(DataFiles.builder(any()).build()).thenReturn(mockDataFile);

      when(mockAppendFiles.appendFile(any(DataFile.class))).thenReturn(mockAppendFiles);

      testManager.write(TABLE_IDENTIFIER.toString(), List.of(Map.of("id", 1L)));

      verify(mockAppendFiles).appendFile(any(DataFile.class));
      verify(mockAppendFiles).commit();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testWriteTableFails() throws IOException {
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
    when(table.io()).thenReturn(fileIO);
    when(table.location()).thenReturn("/tmp/warehouse");
    when(table.schema()).thenReturn(TABLE_SCHEMA);
    OutputFile mockOutputFile = mock(OutputFile.class);
    when(fileIO.newOutputFile(any(String.class))).thenReturn(mockOutputFile);

    FileAppender mockAppender = mock(FileAppender.class);
    doThrow(new IOException("Could not close appender")).when(mockAppender).close();

    try (MockedStatic<Parquet> parquetMock =
        Mockito.mockStatic(Parquet.class, Mockito.RETURNS_DEEP_STUBS)) {
      Parquet.WriteBuilder mockWriterBuilder = mock(Parquet.WriteBuilder.class);
      parquetMock.when(() -> Parquet.write(any(OutputFile.class))).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.createWriterFunc(any())).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.schema(any(Schema.class))).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.overwrite()).thenReturn(mockWriterBuilder);
      when(mockWriterBuilder.build()).thenReturn(mockAppender);

      assertThrows(
          IcebergResourceManagerException.class,
          () -> testManager.write(TABLE_IDENTIFIER.toString(), List.of(Map.of("id", 1L))));
    }
  }

  @Test
  public void testCleanupAll() {
    doReturn(List.of(Namespace.of(NAMESPACE_NAME)))
        .when((SupportsNamespaces) catalog)
        .listNamespaces();
    doReturn(List.of(TABLE_IDENTIFIER)).when(catalog).listTables(any(Namespace.class));
    doReturn(true).when((SupportsNamespaces) catalog).dropNamespace(any(Namespace.class));
    doReturn(true).when((SupportsNamespaces) catalog).namespaceExists(any(Namespace.class));

    testManager.cleanupAll();

    verify(catalog).listTables(Namespace.of(NAMESPACE_NAME));
    verify(catalog).dropTable(TABLE_IDENTIFIER);
    verify((SupportsNamespaces) catalog).dropNamespace(Namespace.of(NAMESPACE_NAME));
  }

  @Test
  public void testCleanupAllWhenNoNamespaces() {
    doReturn(Collections.emptyList()).when((SupportsNamespaces) catalog).listNamespaces();

    testManager.cleanupAll();

    verify((SupportsNamespaces) catalog).listNamespaces();
    verify(catalog, never()).listTables(any(Namespace.class));
    verify((SupportsNamespaces) catalog, never()).dropNamespace(any(Namespace.class));
  }
}
