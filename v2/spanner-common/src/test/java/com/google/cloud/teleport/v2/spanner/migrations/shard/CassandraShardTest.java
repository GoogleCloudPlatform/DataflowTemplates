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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CassandraShardTest {

  private OptionsMap optionsMap;
  private List<String> contactPoints;

  @BeforeEach
  public void setUp() {
    contactPoints = List.of("127.0.0.1:9042");
    optionsMap = Mockito.mock(OptionsMap.class);
    Mockito.when(optionsMap.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints);
    Mockito.when(optionsMap.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn("test_keyspace");
  }

  @Test
  public void testConstructor_Valid() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertNotNull(shard);
    assertEquals("test_keyspace", shard.getKeySpaceName());
    assertEquals(contactPoints, shard.getContactPoints());
  }

  @Test
  public void testConstructor_InvalidContactPoints() {
    Mockito.when(optionsMap.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> new CassandraShard(optionsMap));
  }

  @Test
  public void testConstructor_InvalidKeySpace() {
    Mockito.when(optionsMap.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(null);
    assertThrows(IllegalArgumentException.class, () -> new CassandraShard(optionsMap));
  }

  @Test
  public void testExtractAndSetHostAndPort_Valid() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertEquals("127.0.0.1", shard.getHost());
    assertEquals("9042", shard.getPort());
  }

  @Test
  public void testGetters() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertEquals(contactPoints, shard.getContactPoints());
    assertEquals("test_keyspace", shard.getKeySpaceName());
  }

  @Test
  public void testToString() {
    CassandraShard shard = new CassandraShard(optionsMap);
    String expected =
        String.format(
            "CassandraShard{logicalShardId='%s', contactPoints=%s, keyspace='%s', host='%s', port='%s'}",
            shard.getLogicalShardId(), contactPoints, "test_keyspace", "127.0.0.1", "9042");
    assertEquals(expected, shard.toString());
  }

  @Test
  public void testEqualsAndHashCode_Equal() {
    CassandraShard shard1 = new CassandraShard(optionsMap);
    CassandraShard shard2 = new CassandraShard(optionsMap);
    assertEquals(shard1, shard2);
    assertEquals(shard1.hashCode(), shard2.hashCode());
  }
}
