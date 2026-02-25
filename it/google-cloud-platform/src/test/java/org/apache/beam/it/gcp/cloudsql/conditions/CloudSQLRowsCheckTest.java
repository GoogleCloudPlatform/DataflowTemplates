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
package org.apache.beam.it.gcp.cloudsql.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link CloudSQLRowsCheck}. */
@RunWith(JUnit4.class)
public class CloudSQLRowsCheckTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private CloudSqlResourceManager resourceManager;
  private static final String TABLE_ID = "test-table";

  @Before
  public void setUp() {
    when(resourceManager.getRowCount(TABLE_ID)).thenReturn(10L);
  }

  @Test
  public void testCheck_minRowsMet() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(5).build();
    ConditionCheck.CheckResult result = check.check();
    assertTrue(result.isSuccess());
    assertEquals("Expected at least 5 rows and found 10", result.getMessage());
  }

  @Test
  public void testCheck_minRowsNotMet() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(15).build();
    ConditionCheck.CheckResult result = check.check();
    assertFalse(result.isSuccess());
    assertEquals("Expected 15 rows but has only 10", result.getMessage());
  }

  @Test
  public void testCheck_maxRowsMet() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(5).setMaxRows(15).build();
    ConditionCheck.CheckResult result = check.check();
    assertTrue(result.isSuccess());
    assertEquals("Expected between 5 and 15 rows and found 10", result.getMessage());
  }

  @Test
  public void testCheck_maxRowsExceeded() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(5).setMaxRows(8).build();
    ConditionCheck.CheckResult result = check.check();
    assertFalse(result.isSuccess());
    assertEquals("Expected up to 8 rows but found 10", result.getMessage());
  }

  @Test
  public void testGetDescription_minRowsOnly() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(10).build();
    assertEquals("CloudSQL check if table test-table has 10 rows", check.getDescription());
  }

  @Test
  public void testGetDescription_minAndMaxRows() {
    CloudSQLRowsCheck check =
        CloudSQLRowsCheck.builder(resourceManager, TABLE_ID).setMinRows(5).setMaxRows(15).build();
    assertEquals(
        "CloudSQL check if table test-table has between 5 and 15 rows", check.getDescription());
  }
}
