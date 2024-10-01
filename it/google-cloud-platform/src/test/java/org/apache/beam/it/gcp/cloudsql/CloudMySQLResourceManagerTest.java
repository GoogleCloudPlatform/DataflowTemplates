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

/** Integration tests for {@link CloudMySQLResourceManager}. */
@RunWith(JUnit4.class)
public class CloudMySQLResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private CloudMySQLResourceManager testManager;

  private static final String TEST_ID = "test_id";

  @Before
  public void setUp() {
    testManager =
        (CloudMySQLResourceManager)
            CloudMySQLResourceManager.builder(TEST_ID)
                .setDatabaseName("mockDatabase")
                .setUsername("username")
                .setPassword("password")
                .setHost("127.0.0.1")
                .setPort(1234)
                .build();
  }

  @Test
  public void testGetJDBCPrefixReturnsCorrectValue() {
    assertThat(testManager.getJDBCPrefix()).isEqualTo("mysql");
  }
}
