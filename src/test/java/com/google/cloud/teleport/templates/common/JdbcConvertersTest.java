/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link JdbcConverters}. */
@RunWith(MockitoJUnitRunner.class)
public class JdbcConvertersTest {

    private static final String NAME_KEY = "name";
    private static final String NAME_VALUE = "John";
    private static final String AGE_KEY = "age";
    private static final int AGE_VALUE = 24;
    private static TableRow expectedTableRow;

    @Mock private ResultSet resultSet;

    @Mock private ResultSetMetaData resultSetMetaData;

    @Before
    public void setUp() throws Exception {

        Mockito.when(resultSet.getObject(1)).thenReturn(NAME_VALUE);
        Mockito.when(resultSet.getObject(2)).thenReturn(AGE_VALUE);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);

        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);

        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn(AGE_KEY);

        expectedTableRow = new TableRow();
        expectedTableRow.set(NAME_KEY, NAME_VALUE);
        expectedTableRow.set(AGE_KEY, AGE_VALUE);
    }

    @Test
    public void testRowMapper() throws Exception {

        JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow();
        TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

        assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
        assertThat(actualTableRow, equalTo(expectedTableRow));
    }
}
