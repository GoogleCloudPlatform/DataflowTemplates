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
package com.google.cloud.teleport.v2.templates.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.mysql.MySQLDMLGenerator;
import com.google.cloud.teleport.v2.templates.mysql.MySqlDao;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SourceProcessorFactoryTest {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testGetDMLGenerator_MySQLSource() throws Exception {
    IDMLGenerator dmlGenerator = SourceProcessorFactory.getDMLGenerator(Constants.SOURCE_MYSQL);
    assertNotNull(dmlGenerator);
    assertTrue(dmlGenerator instanceof MySQLDMLGenerator);
  }

  @Test
  public void testGetDMLGenerator_InvalidSource() throws Exception {
    expectedEx.expect(Exception.class);
    expectedEx.expectMessage("Invalid source type: invalidSource");
    SourceProcessorFactory.getDMLGenerator("invalidSource");
  }

  @Test
  public void testGetSourceDaoMap_MySQLSource() throws Exception {
    Shard shard1 = mock(Shard.class);
    when(shard1.getHost()).thenReturn("localhost");
    when(shard1.getPort()).thenReturn("3306");
    when(shard1.getDbName()).thenReturn("test_db");
    when(shard1.getLogicalShardId()).thenReturn("shard1");
    when(shard1.getUserName()).thenReturn("root");
    when(shard1.getPassword()).thenReturn("password");

    Shard shard2 = mock(Shard.class);
    when(shard2.getHost()).thenReturn("localhost");
    when(shard2.getPort()).thenReturn("3307");
    when(shard2.getDbName()).thenReturn("test_db_2");
    when(shard2.getLogicalShardId()).thenReturn("shard2");
    when(shard2.getUserName()).thenReturn("root");
    when(shard2.getPassword()).thenReturn("password");

    List<Shard> shards = Arrays.asList(shard1, shard2);

    Map<String, ISourceDao> sourceDaoMap =
        SourceProcessorFactory.getSourceDaoMap(Constants.SOURCE_MYSQL, shards, 10);

    assertNotNull(sourceDaoMap);
    assertEquals(2, sourceDaoMap.size());

    ISourceDao mySqlDao1 = sourceDaoMap.get("shard1");
    assertTrue(mySqlDao1 instanceof MySqlDao);
    assertEquals(
        "jdbc:mysql://localhost:3306/test_db", ((MySqlDao) mySqlDao1).getSourceConnectionUrl());

    ISourceDao mySqlDao2 = sourceDaoMap.get("shard2");
    assertTrue(mySqlDao2 instanceof MySqlDao);
    assertEquals(
        "jdbc:mysql://localhost:3307/test_db_2", ((MySqlDao) mySqlDao2).getSourceConnectionUrl());
  }

  @Test
  public void testGetSourceDaoMap_InvalidSource() throws Exception {
    expectedEx.expect(Exception.class);
    expectedEx.expectMessage("Invalid source type: invalidSource");
    List<Shard> shards = Arrays.asList(mock(Shard.class), mock(Shard.class));
    SourceProcessorFactory.getSourceDaoMap("invalidSource", shards, 10);
  }

  @Test
  public void testGetSourceDaoMap_NullShards() throws Exception {
    expectedEx.expect(NullPointerException.class);
    SourceProcessorFactory.getSourceDaoMap(Constants.SOURCE_MYSQL, null, 10);
  }
}
