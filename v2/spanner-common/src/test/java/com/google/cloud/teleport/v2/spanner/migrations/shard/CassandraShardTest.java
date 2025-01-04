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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CassandraShardTest {

  private OptionsMap optionsMap;
  private List<String> contactPoints;

  @Before
  public void setUp() {
    optionsMap = mock(OptionsMap.class);
    contactPoints = List.of("127.0.0.1:9042");

    when(optionsMap.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints);
    when(optionsMap.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn("test_keyspace");
  }

  @Test
  public void testConstructor_Valid() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertNotNull("CassandraShard should be created successfully", shard);
    assertEquals("Keyspace name should match", "test_keyspace", shard.getKeySpaceName());
    assertEquals("Contact points should match", contactPoints, shard.getContactPoints());
  }

  @Test
  public void testConstructor_InvalidContactPoints() {
    when(optionsMap.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(null);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new CassandraShard(optionsMap));
    assertEquals("CONTACT_POINTS cannot be null or empty.", exception.getMessage());
  }

  @Test
  public void testConstructor_InvalidKeySpace() {
    when(optionsMap.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(null);
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> new CassandraShard(optionsMap));
    assertEquals("SESSION_KEYSPACE cannot be null or empty.", exception.getMessage());
  }

  @Test
  public void testExtractAndSetHostAndPort_Valid() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertEquals("Host should be extracted correctly", "127.0.0.1", shard.getHost());
    assertEquals("Port should be extracted correctly", "9042", shard.getPort());
  }

  @Test
  public void testGetters() {
    CassandraShard shard = new CassandraShard(optionsMap);
    assertEquals(
        "Contact points getter should return correct value",
        contactPoints,
        shard.getContactPoints());
    assertEquals(
        "Keyspace getter should return correct value", "test_keyspace", shard.getKeySpaceName());
  }

  @Test
  public void testToString() {
    CassandraShard shard = new CassandraShard(optionsMap);
    String expected =
        String.format(
            "CassandraShard{logicalShardId='%s', contactPoints=%s, keyspace='%s', host='%s', port='%s'}",
            shard.getLogicalShardId(), contactPoints, "test_keyspace", "127.0.0.1", "9042");
    assertEquals("toString should return the expected representation", expected, shard.toString());
  }

  @Test
  public void testEqualsAndHashCode_Equal() {
    CassandraShard shard1 = new CassandraShard(optionsMap);
    CassandraShard shard2 = new CassandraShard(optionsMap);
    assertEquals("Equal shards should be considered equal", shard1, shard2);
    assertEquals(
        "Equal shards should have the same hash code", shard1.hashCode(), shard2.hashCode());
  }

  @Test
  public void testGetUsername() {
    CassandraShard shard = new CassandraShard(optionsMap);
    String expectedUsername = "user123";
    when(optionsMap.get(TypedDriverOption.AUTH_PROVIDER_USER_NAME)).thenReturn(expectedUsername);
    String actualUsername = shard.getUsername();
    assertEquals(
        "Username should match the one in the options map", expectedUsername, actualUsername);
  }

  @Test
  public void testGetOptionsMap() {
    CassandraShard shard = new CassandraShard(optionsMap);
    OptionsMap returnedOptionsMap = shard.getOptionsMap();
    assertSame(
        "The options map returned should be the same as the one passed to the constructor",
        optionsMap,
        returnedOptionsMap);
  }

  @Test
  public void testEquals_ShouldReturnTrue_WhenContactPointsAndKeyspaceAreSame() {
    String keyspace = "keyspace1";
    List<String> contactPoints = List.of("localhost:9042");
    OptionsMap optionsMap1 = mock(OptionsMap.class);
    when(optionsMap1.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(keyspace);
    when(optionsMap1.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints);

    CassandraShard shard1 = new CassandraShard(optionsMap1);
    CassandraShard shard2 = new CassandraShard(optionsMap1);

    assertEquals(
        "CassandraShard objects with the same contactPoints and keyspace should be equal",
        shard1,
        shard2);
  }

  @Test
  public void testEquals_ShouldReturnFalse_WhenContactPointsAreDifferent() {
    String keyspace = "keyspace1";
    List<String> contactPoints1 = List.of("localhost:9042");
    List<String> contactPoints2 = List.of("localhost:9043");
    OptionsMap optionsMap1 = mock(OptionsMap.class);
    when(optionsMap1.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(keyspace);
    when(optionsMap1.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints1);

    OptionsMap optionsMap2 = mock(OptionsMap.class);
    when(optionsMap2.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(keyspace);
    when(optionsMap2.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints2);

    CassandraShard shard1 = new CassandraShard(optionsMap1);
    CassandraShard shard2 = new CassandraShard(optionsMap2);

    assertNotEquals(
        "CassandraShard objects with different contactPoints should not be equal", shard1, shard2);
  }

  @Test
  public void testEquals_ShouldReturnFalse_WhenKeyspaceIsDifferent() {
    String keyspace1 = "keyspace1";
    String keyspace2 = "keyspace2";
    List<String> contactPoints = List.of("localhost:9042");
    OptionsMap optionsMap1 = mock(OptionsMap.class);
    when(optionsMap1.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(keyspace1);
    when(optionsMap1.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints);

    OptionsMap optionsMap2 = mock(OptionsMap.class);
    when(optionsMap2.get(TypedDriverOption.SESSION_KEYSPACE)).thenReturn(keyspace2);
    when(optionsMap2.get(TypedDriverOption.CONTACT_POINTS)).thenReturn(contactPoints);

    CassandraShard shard1 = new CassandraShard(optionsMap1);
    CassandraShard shard2 = new CassandraShard(optionsMap2);

    assertNotEquals(
        "CassandraShard objects with different keyspace should not be equal", shard1, shard2);
  }
}
