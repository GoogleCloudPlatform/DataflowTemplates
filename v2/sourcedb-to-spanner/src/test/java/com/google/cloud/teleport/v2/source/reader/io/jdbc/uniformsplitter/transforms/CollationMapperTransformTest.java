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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.quality.Strictness;

/** Test class for {@link CollationMapperTransform}. */
@RunWith(MockitoJUnitRunner.class)
public class CollationMapperTransformTest implements Serializable {

  SerializableFunction<Void, DataSource> mockDataSourceProviderFn =
      Mockito.mock(
          SerializableFunction.class, withSettings().serializable().strictness(Strictness.LENIENT));

  DataSource mockDataSource =
      Mockito.mock(DataSource.class, withSettings().serializable().strictness(Strictness.LENIENT));

  Connection mockConnection =
      Mockito.mock(Connection.class, withSettings().serializable().strictness(Strictness.LENIENT));

  Statement mockStatementFirst =
      Mockito.mock(Statement.class, withSettings().serializable().strictness(Strictness.LENIENT));

  Statement mockStatementSecond =
      Mockito.mock(Statement.class, withSettings().serializable().strictness(Strictness.LENIENT));

  ResultSet mockResultSetFirst =
      Mockito.mock(ResultSet.class, withSettings().serializable().strictness(Strictness.LENIENT));
  ResultSet mockResultSetSecond =
      Mockito.mock(ResultSet.class, withSettings().serializable().strictness(Strictness.LENIENT));

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<KV<CollationReference, CollationMapper>> collationMapperCaptor;

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testCollationMapperTransform() throws SQLException {
    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement())
        .thenReturn(mockStatementFirst)
        .thenReturn(mockStatementSecond);

    when(mockStatementFirst.execute(any())).thenReturn(false);
    when(mockStatementFirst.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatementFirst.getUpdateCount()).thenReturn(0);
    when(mockStatementFirst.getResultSet()).thenReturn(mockResultSetFirst);

    when(mockStatementSecond.execute(any())).thenReturn(false);
    when(mockStatementSecond.getMoreResults()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(mockStatementSecond.getUpdateCount()).thenReturn(0);
    when(mockStatementSecond.getResultSet()).thenReturn(mockResultSetSecond);

    when(mockResultSetFirst.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSetFirst.getString(CHARSET_CHAR_COL)).thenReturn("A").thenReturn("a");
    when(mockResultSetFirst.getString(EQUIVALENT_CHARSET_CHAR_COL)).thenReturn("a").thenReturn("a");
    when(mockResultSetFirst.getLong(CODEPOINT_RANK_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSetFirst.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL))
        .thenReturn("a")
        .thenReturn("a");
    when(mockResultSetFirst.getLong(CODEPOINT_RANK_PAD_SPACE_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSetFirst.getBoolean(IS_EMPTY_COL)).thenReturn(false).thenReturn(false);
    when(mockResultSetFirst.getBoolean(IS_SPACE_COL)).thenReturn(false).thenReturn(false);

    when(mockResultSetSecond.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockResultSetSecond.getString(CHARSET_CHAR_COL)).thenReturn("a").thenReturn("A");
    when(mockResultSetSecond.getString(EQUIVALENT_CHARSET_CHAR_COL))
        .thenReturn("A")
        .thenReturn("A");
    when(mockResultSetSecond.getLong(CODEPOINT_RANK_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSetSecond.getString(EQUIVALENT_CHARSET_CHAR_PAD_SPACE_COL))
        .thenReturn("A")
        .thenReturn("A");
    when(mockResultSetSecond.getLong(CODEPOINT_RANK_PAD_SPACE_COL)).thenReturn(0L).thenReturn(0L);
    when(mockResultSetSecond.getBoolean(IS_EMPTY_COL)).thenReturn(false).thenReturn(false);
    when(mockResultSetSecond.getBoolean(IS_SPACE_COL)).thenReturn(false).thenReturn(false);

    CollationReference testCollationReferenceFirst =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollationFirst")
            .setPadSpace(false)
            .build();

    CollationReference testCollationReferenceSecond =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollationSecond")
            .setPadSpace(false)
            .build();
    CollationMapperTransform collationMapperTransform =
        CollationMapperTransform.builder()
            .setCollationReferences(
                ImmutableList.of(
                    testCollationReferenceFirst,
                    testCollationReferenceSecond,
                    /* test that pick distinct collationReferences to avoid un-necessary collation discovery work. */ testCollationReferenceFirst))
            .setDataSourceProviderFn(mockDataSourceProviderFn)
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .build();
    PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView =
        testPipeline.apply(collationMapperTransform);
    PCollection<Void> output =
        testPipeline
            .apply(Create.of("test"))
            .apply(
                "testTransform",
                ParDo.of(
                        new VerifyMapper(
                            collationMapperView,
                            testCollationReferenceFirst,
                            testCollationReferenceSecond))
                    .withSideInputs(collationMapperView));
    testPipeline.run().waitUntilFinish();
  }

  static class VerifyMapper extends DoFn<String, Void> implements Serializable {
    private PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView;
    private CollationReference testCollationReferenceFirst;
    private CollationReference testCollationReferenceSecond;

    VerifyMapper(
        PCollectionView<Map<CollationReference, CollationMapper>> collationMapperView,
        CollationReference testCollationReferenceFirst,
        CollationReference testCollationReferenceSecond) {
      this.collationMapperView = collationMapperView;
      this.testCollationReferenceFirst = testCollationReferenceFirst;
      this.testCollationReferenceSecond = testCollationReferenceSecond;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Map<CollationReference, CollationMapper> collationMap = c.sideInput(collationMapperView);
      assertThat(collationMap.size()).isEqualTo(2);
      assertThat(collationMap.get(testCollationReferenceFirst).collationReference())
          .isEqualTo(testCollationReferenceFirst);
      assertThat(collationMap.get(testCollationReferenceSecond).collationReference())
          .isEqualTo(testCollationReferenceSecond);
    }
  }
}
