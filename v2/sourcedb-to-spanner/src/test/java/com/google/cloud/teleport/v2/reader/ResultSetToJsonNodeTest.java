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
package com.google.cloud.teleport.v2.reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class ResultSetToJsonNodeTest {

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    @Test
    public void convertToChangeEventJsonBasic() throws Exception {

        ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
        when(rsMetaMock.getColumnCount()).thenReturn(6);
        when(rsMetaMock.getColumnName(1)).thenReturn("col1");
        when(rsMetaMock.getColumnType(1)).thenReturn(Types.VARCHAR);
        when(rsMetaMock.getColumnName(2)).thenReturn("col2");
        when(rsMetaMock.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(rsMetaMock.getColumnName(3)).thenReturn("bytes_column");
        when(rsMetaMock.getColumnType(3)).thenReturn(Types.VARBINARY);
        when(rsMetaMock.getColumnName(4)).thenReturn("timestamp_column");
        when(rsMetaMock.getColumnType(4)).thenReturn(Types.TIMESTAMP);
        when(rsMetaMock.getColumnName(5)).thenReturn("bool_column");
        when(rsMetaMock.getColumnType(5)).thenReturn(Types.BIT);
        when(rsMetaMock.getColumnName(6)).thenReturn("date_column");
        when(rsMetaMock.getColumnType(6)).thenReturn(Types.DATE);

        ResultSet rs = Mockito.mock(ResultSet.class);
        when(rs.getMetaData()).thenReturn(rsMetaMock);
        when(rs.getObject(1)).thenReturn(1);
        when(rs.getObject(2)).thenReturn("value");
        when(rs.getBytes(3)).thenReturn(new byte[]{120, 53, 56, 48, 48});
        when(rs.getObject(4)).thenReturn("2022-08-05 08:23:11.0");
        when(rs.getObject(5)).thenReturn(true);
        when(rs.getObject(6)).thenReturn("2022-03-05");

        JsonNode actual = ResultSetToJsonNode.create("test_table").mapRow(rs);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode expected =
                mapper.readTree(
                        String.format("{\"%s\":\"test_table\", \"col1\":1, \"col2\": \"value\", \"bytes_column\": \"7835383030\", \"timestamp_column\": \"2022-08-05T08:23:11.0\", \"bool_column\": true, \"date_column\": \"2022-03-05\"}", Constants.EVENT_TABLE_NAME_KEY));

        assertEquals(expected, actual);
    }
}