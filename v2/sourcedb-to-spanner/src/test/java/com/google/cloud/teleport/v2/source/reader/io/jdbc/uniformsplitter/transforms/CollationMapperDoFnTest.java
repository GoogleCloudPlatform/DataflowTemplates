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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.CODEPOINT_RANK_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_EMPTY_COL;
import static com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationOrderRow.CollationsOrderQueryColumns.IS_SPACE_COL;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CollationMapperDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class CollationMapperDoFnTest {
  SerializableFunction<Void, DataSource> mockDataSourceProviderFn =
      Mockito.mock(SerializableFunction.class, withSettings().serializable());
  DataSource mockDataSource = Mockito.mock(DataSource.class, withSettings().serializable());

  Connection mockConnection = Mockito.mock(Connection.class, withSettings().serializable());

  Statement mockStatement = Mockito.mock(Statement.class, withSettings().serializable());

  ResultSet mockResultSet = Mockito.mock(ResultSet.class, withSettings().serializable());

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<KV<CollationReference, CollationMapper>> collationMapperCaptor;

  @Test
  public void testCollationMapperDoFnBasic() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(any())).thenReturn(false);
    when(mockStatement.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatement.getUpdateCount()).thenReturn(0);
    when(mockStatement.getResultSet()).thenReturn(mockResultSet);

    when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSet.getString(CHARSET_CHAR_COL)).thenReturn("a").thenReturn("A");
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_COL)).thenReturn("A").thenReturn("A");
    when(mockResultSet.getLong(CODEPOINT_RANK_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSet.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL))
        .thenReturn("A")
        .thenReturn("A");
    when(mockResultSet.getLong(CODEPOINT_RANK_PAD_SPACE_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSet.getBoolean(IS_EMPTY_COL)).thenReturn(false).thenReturn(false);
    when(mockResultSet.getBoolean(IS_SPACE_COL)).thenReturn(false).thenReturn(false);

    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();

    CollationMapperDoFn collationMapperDoFn =
        new CollationMapperDoFn(
            mockDataSourceProviderFn, new MysqlDialectAdapter(MySqlVersion.DEFAULT));
    collationMapperDoFn.setup();
    collationMapperDoFn.processElement(testCollationReference, mockOut);
    verify(mockOut).output(collationMapperCaptor.capture());
    KV<CollationReference, CollationMapper> mapperKV = collationMapperCaptor.getValue();
    assertThat(mapperKV.getKey()).isEqualTo(testCollationReference);
    assertThat(mapperKV.getValue().allPositionsIndex().getCharsetSize()).isEqualTo(1);
    assertThat(mapperKV.getValue().unMapString(mapperKV.getValue().mapString("a", 1)))
        .isEqualTo("A");
  }

  @Test
  public void testCollationMapperDoFnException() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection())
        .thenThrow(new SQLException("test"))
        .thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenThrow(new SQLException("test"));

    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(false)
            .build();

    CollationMapperDoFn collationMapperDoFn =
        new CollationMapperDoFn(
            mockDataSourceProviderFn, new MysqlDialectAdapter(MySqlVersion.DEFAULT));
    collationMapperDoFn.setup();
    assertThrows(
        SQLException.class,
        () -> collationMapperDoFn.processElement(testCollationReference, mockOut));
    assertThrows(
        SQLException.class,
        () -> collationMapperDoFn.processElement(testCollationReference, mockOut));
  }
}
