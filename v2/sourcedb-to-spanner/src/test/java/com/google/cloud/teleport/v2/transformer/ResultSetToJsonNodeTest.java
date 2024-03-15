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
package com.google.cloud.teleport.v2.transformer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ResultSetToJsonNodeTest {

  @Mock private ResultSet resultSet;

  @Mock private ResultSetMetaData resultSetMetaData;

  @Test
  public void convertToChangeEventJsonBasic() throws Exception {

    ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
    when(rsMetaMock.getColumnCount()).thenReturn(2);
    when(rsMetaMock.getColumnName(1)).thenReturn("col1");
    when(rsMetaMock.getColumnName(2)).thenReturn("col2");

    // prepare main mock for result set and define when
    // main mock to return dependant mock
    ResultSet rs = Mockito.mock(ResultSet.class);
    when(rs.getMetaData()).thenReturn(rsMetaMock);
    when(rs.getObject(1)).thenReturn(1);
    when(rs.getObject(2)).thenReturn("value");

    JsonNode actual = ResultSetToJsonNode.create("test_table").mapRow(rs);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode expected =
        mapper.readTree(
            "{\"_metadata_table_name\":\"test_table\", \"col1\":1, \"col2\": \"value\"}");

    assertEquals(actual, expected);
  }
}
