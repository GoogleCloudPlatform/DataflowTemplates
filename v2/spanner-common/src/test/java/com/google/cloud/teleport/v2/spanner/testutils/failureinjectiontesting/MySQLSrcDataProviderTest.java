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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.AUTHORS_TABLE;
import static com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting.MySQLSrcDataProvider.BOOKS_TABLE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager.JDBCSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link MySQLSrcDataProvider} using JUnit 4. */
@RunWith(MockitoJUnitRunner.class)
public class MySQLSrcDataProviderTest {

  @Mock private CloudMySQLResourceManager mockResourceManager;

  @Test
  public void testCreateSourceResourceManagerWithSchema() {
    try (MockedStatic<CloudMySQLResourceManager> mockedStatic =
        mockStatic(CloudMySQLResourceManager.class)) {
      CloudMySQLResourceManager.Builder mockBuilder = mock(CloudMySQLResourceManager.Builder.class);
      mockedStatic
          .when(() -> CloudMySQLResourceManager.builder(anyString()))
          .thenReturn(mockBuilder);
      when(mockBuilder.build()).thenReturn(mockResourceManager);

      CloudSqlResourceManager result =
          MySQLSrcDataProvider.createSourceResourceManagerWithSchema("test-project");

      assertThat(result).isEqualTo(mockResourceManager);

      ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<JDBCSchema> schemaCaptor = ArgumentCaptor.forClass(JDBCSchema.class);
      verify(mockResourceManager, times(2))
          .createTable(tableNameCaptor.capture(), schemaCaptor.capture());

      int authorIndex = tableNameCaptor.getAllValues().indexOf(AUTHORS_TABLE);
      assertThat(authorIndex).isNotEqualTo(-1);
      JDBCSchema authorSchema = schemaCaptor.getAllValues().get(authorIndex);
      assertThat(authorSchema.toSqlStatement())
          .isEqualTo("author_id INT NOT NULL, name VARCHAR(200), PRIMARY KEY ( author_id )");

      // Verify books table creation
      int bookIndex = tableNameCaptor.getAllValues().indexOf(BOOKS_TABLE);
      assertThat(bookIndex).isNotEqualTo(-1);
      JDBCSchema bookSchema = schemaCaptor.getAllValues().get(bookIndex);
      assertThat(bookSchema.toSqlStatement())
          .isEqualTo(
              "book_id INT NOT NULL, name VARCHAR(200), author_id INT NOT NULL, PRIMARY KEY ( book_id )");
    }
  }

  @Test
  public void testWriteRowsInSourceDB_Success() {
    when(mockResourceManager.write(anyString(), anyList())).thenReturn(true);
    int startId = 1;
    int endId = 5;

    boolean success = MySQLSrcDataProvider.writeRowsInSourceDB(startId, endId, mockResourceManager);

    assertThat(success).isTrue();
    ArgumentCaptor<List<Map<String, Object>>> rowsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockResourceManager, times(2)).write(anyString(), rowsCaptor.capture());

    List<Map<String, Object>> authorRows = rowsCaptor.getAllValues().get(0);
    assertThat(authorRows).hasSize(5);
    assertThat(authorRows.get(0)).containsEntry("author_id", 1);
    assertThat(authorRows.get(0)).containsEntry("name", "author_name_1");
    assertThat(authorRows.get(4)).containsEntry("author_id", 5);
    assertThat(authorRows.get(4)).containsEntry("name", "author_name_5");

    // Verify Books rows
    List<Map<String, Object>> bookRows = rowsCaptor.getAllValues().get(1);
    assertThat(bookRows).hasSize(5);
    assertThat(bookRows.get(0)).containsEntry("author_id", 1);
    assertThat(bookRows.get(0)).containsEntry("book_id", 1);
    assertThat(bookRows.get(0)).containsEntry("name", "book_name_1");
    assertThat(bookRows.get(4)).containsEntry("author_id", 5);
    assertThat(bookRows.get(4)).containsEntry("book_id", 5);
    assertThat(bookRows.get(4)).containsEntry("name", "book_name_5");
  }

  @Test
  public void testWriteRowsInSourceDB_FailsOnFirstWrite() {
    when(mockResourceManager.write(eq(AUTHORS_TABLE), anyList())).thenReturn(false);
    int startId = 1;
    int endId = 5;

    boolean success = MySQLSrcDataProvider.writeRowsInSourceDB(startId, endId, mockResourceManager);

    assertThat(success).isFalse();

    verify(mockResourceManager, times(1)).write(eq(AUTHORS_TABLE), anyList());
    verify(mockResourceManager, never()).write(eq(BOOKS_TABLE), anyList());
  }

  @Test
  public void testWriteAuthorRowsInSourceDB() {
    when(mockResourceManager.write(eq(AUTHORS_TABLE), anyList())).thenReturn(true);
    int startId = 10;
    int endId = 12;

    boolean success =
        MySQLSrcDataProvider.writeAuthorRowsInSourceDB(startId, endId, mockResourceManager);

    assertThat(success).isTrue();
    ArgumentCaptor<List<Map<String, Object>>> rowsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockResourceManager, times(1)).write(eq(AUTHORS_TABLE), rowsCaptor.capture());
    verify(mockResourceManager, never()).write(eq(BOOKS_TABLE), anyList());

    List<Map<String, Object>> capturedRows = rowsCaptor.getValue();
    assertThat(capturedRows).hasSize(3);
    assertThat(capturedRows.get(0)).containsEntry("author_id", 10);
    assertThat(capturedRows.get(0)).containsEntry("name", "author_name_10");
    assertThat(capturedRows.get(2)).containsEntry("author_id", 12);
    assertThat(capturedRows.get(2)).containsEntry("name", "author_name_12");
  }

  @Test
  public void testWriteBookRowsInSourceDB() {
    when(mockResourceManager.write(eq(BOOKS_TABLE), anyList())).thenReturn(true);
    int startId = 100;
    int endId = 101;
    int authorId = 999;

    boolean success =
        MySQLSrcDataProvider.writeBookRowsInSourceDB(startId, endId, authorId, mockResourceManager);

    assertThat(success).isTrue();
    ArgumentCaptor<List<Map<String, Object>>> rowsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockResourceManager, times(1)).write(eq(BOOKS_TABLE), rowsCaptor.capture());
    verify(mockResourceManager, never()).write(eq(AUTHORS_TABLE), anyList());

    List<Map<String, Object>> capturedRows = rowsCaptor.getValue();
    assertThat(capturedRows).hasSize(2);
    assertThat(capturedRows.get(0)).containsEntry("author_id", authorId);
    assertThat(capturedRows.get(0)).containsEntry("book_id", 100);
    assertThat(capturedRows.get(0)).containsEntry("name", "book_name_100");
    assertThat(capturedRows.get(1)).containsEntry("author_id", authorId);
    assertThat(capturedRows.get(1)).containsEntry("book_id", 101);
    assertThat(capturedRows.get(1)).containsEntry("name", "book_name_101");
  }

  @Test
  public void testCreateForeignKeyConstraint() {
    MySQLSrcDataProvider.createForeignKeyConstraint(mockResourceManager);
    verify(mockResourceManager)
        .runSQLUpdate(
            "ALTER TABLE Books\n"
                + "ADD CONSTRAINT fk_Books_Authors\n"
                + "FOREIGN KEY (author_id)\n"
                + "REFERENCES Authors(author_id);");
  }
}
