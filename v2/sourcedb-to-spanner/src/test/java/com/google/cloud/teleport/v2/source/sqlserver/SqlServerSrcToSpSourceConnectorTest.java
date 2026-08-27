/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SqlServerSrcToSpSourceConnectorTest {

  @Test
  public void testGetTypeMapping() {
    SqlServerSrcToSpSourceConnector connector = new SqlServerSrcToSpSourceConnector();
    assertTrue(connector.getTypeMapping().containsKey("INT"));
  }

  @Test
  public void testGetJdbcUrl() {
    SqlServerSrcToSpSourceConnector connector = new SqlServerSrcToSpSourceConnector();
    String url = connector.getJdbcUrl("localhost", 1433, "mydb", "prop1=val1", null, null);
    assertEquals("jdbc:sqlserver://localhost:1433;databaseName=mydb;encrypt=false;prop1=val1", url);
  }
}
