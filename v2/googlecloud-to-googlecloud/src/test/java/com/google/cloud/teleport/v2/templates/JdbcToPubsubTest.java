/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.JdbcToPubsub.ResultSetToJSONString;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link JdbcToPubsub}. */
@RunWith(JUnit4.class)
public final class JdbcToPubsubTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private ResultSet rs;
  @Mock private ResultSetMetaData rsMetaData;
  @Mock private Clob clobColumn;

  @Test
  public void testResultSetToJSONString() throws Exception {
    when(rsMetaData.getColumnCount()).thenReturn(3);
    when(rsMetaData.getColumnLabel(1)).thenReturn("Name");
    when(rsMetaData.getColumnLabel(2)).thenReturn("Points");
    when(rsMetaData.getColumnLabel(3)).thenReturn("Clob_Column");
    when(rsMetaData.getColumnTypeName(1)).thenReturn("VARCHAR");
    when(rsMetaData.getColumnTypeName(2)).thenReturn("INTEGER");
    when(rsMetaData.getColumnTypeName(3)).thenReturn("CLOB");
    when(rs.getMetaData()).thenReturn(rsMetaData);
    when(rs.getObject(1)).thenReturn("testName");
    when(rs.getObject(2)).thenReturn(123);
    when(rs.getObject(3)).thenReturn(clobColumn);
    when(rs.getClob(3)).thenReturn(clobColumn);
    when(clobColumn.length()).thenReturn((long) 20);
    when(clobColumn.getSubString(1, 20)).thenReturn("This is a long text.");

    ResultSetToJSONString mapper = new ResultSetToJSONString();
    String jsonString = mapper.mapRow(rs);
    JSONObject jsonObject = new JSONObject(jsonString);

    assertEquals(jsonObject.getInt("Points"), 123);
    assertEquals(jsonObject.getString("Name"), "testName");
    assertEquals(jsonObject.getString("Clob_Column"), "This is a long text.");
  }
}
