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
package org.apache.beam.it.gcp.cloudsql;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Integration tests for {@link CloudOracleResourceManager}. */
@RunWith(JUnit4.class)
public class CloudOracleResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private CloudOracleResourceManager testManager;

  private static final String TEST_ID = "test_id";

  @Before
  public void setUp() {
    testManager =
        (CloudOracleResourceManager)
            CloudOracleResourceManager.builder(TEST_ID)
                .setUsername("username")
                .setPassword("password")
                .setHost("127.0.0.1")
                .build();
  }

  @Test
  public void testGetJDBCPrefixReturnsCorrectValue() {
    assertThat(testManager.getJDBCPrefix()).isEqualTo("oracle");
  }

  @Test
  public void testGetDatabaseReturnsCorrectValue() {
    assertThat(testManager.getDatabaseName()).isEqualTo("xe");
  }

  @Test
  public void testGeUriReturnsCorrectValue() {
    assertThat(testManager.getUri()).isEqualTo("jdbc:oracle:thin:@127.0.0.1:1521:xe");
  }
}
