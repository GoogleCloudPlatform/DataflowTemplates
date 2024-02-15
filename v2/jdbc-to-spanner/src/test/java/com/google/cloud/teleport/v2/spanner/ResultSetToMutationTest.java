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
package com.google.cloud.teleport.v2.spanner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Mutation;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ResultSetToMutation}. */
@RunWith(MockitoJUnitRunner.class)
public class ResultSetToMutationTest {

  @Mock private ResultSet resultSet;
  @Mock private ResultSetMetaData resultSetMetaData;

  @Test
  public void testMutationMapper() throws Exception {
    when(resultSet.getObject(1)).thenReturn(1);
    when(resultSet.getObject(2)).thenReturn("name1");
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    when(resultSetMetaData.getColumnCount()).thenReturn(2);

    when(resultSetMetaData.getColumnName(1)).thenReturn("id");
    when(resultSetMetaData.getColumnType(1)).thenReturn(Types.INTEGER);
    when(resultSetMetaData.getColumnName(2)).thenReturn("name");
    when(resultSetMetaData.getColumnType(2)).thenReturn(Types.VARCHAR);

    Mutation.WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder("singers");
    mutationBuilder.set("id").to(1);
    mutationBuilder.set("name").to("name1");
    Mutation expectedMutation = mutationBuilder.build();

    JdbcIO.RowMapper<Mutation> resultSetConverters = ResultSetToMutation.create("singers", null);
    Mutation actualMutation = resultSetConverters.mapRow(resultSet);

    assertThat(actualMutation, equalTo(expectedMutation));
  }
}
