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
package com.google.cloud.teleport.v2.spanner.testutils.failureinjectiontesting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class UserTest {

  @Test
  public void testEqualsAndHashCode() {
    User u1 = new User();
    u1.id = 1;
    u1.firstName = "John";
    u1.lastName = "Doe";
    u1.age = 30;
    u1.status = true;
    u1.col1 = 100L;
    u1.col2 = 200L;

    User u2 = new User();
    u2.id = 1;
    u2.firstName = "John";
    u2.lastName = "Doe";
    u2.age = 30;
    u2.status = true;
    u2.col1 = 100L;
    u2.col2 = 200L;

    User u3 = new User();
    u3.id = 2;

    assertTrue(u1.equals(u1));
    assertTrue(u1.equals(u2));
    assertFalse(u1.equals(u3));
    assertFalse(u1.equals(null));
    assertFalse(u1.equals("string"));

    assertEquals(u1.hashCode(), u2.hashCode());

    User u4 = new User();
    u4.id = 1;
    u4.age = 31; // Different age
    assertFalse(u1.equals(u4));

    User u5 = new User();
    u5.id = 1;
    u5.status = false; // Different status
    assertFalse(u1.equals(u5));

    User u6 = new User();
    u6.id = 1;
    u6.col1 = 101L; // Different col1
    assertFalse(u1.equals(u6));

    User u7 = new User();
    u7.id = 1;
    u7.col2 = 201L; // Different col2
    assertFalse(u1.equals(u7));

    User u8 = new User();
    u8.id = 1;
    u8.firstName = "Jane"; // Different firstName
    assertFalse(u1.equals(u8));

    User u9 = new User();
    u9.id = 1;
    u9.lastName = "Smith"; // Different lastName
    assertFalse(u1.equals(u9));
  }

  @Test
  public void testToString() {
    User u = new User();
    u.id = 1;
    u.firstName = "John";
    String str = u.toString();
    assertTrue(str.contains("User"));
    assertTrue(str.contains("John"));
  }

  @Test
  public void testUpdate_JDBC() throws SQLException {
    User u = new User();
    u.id = 1;
    u.firstName = "John";
    u.lastName = "Doe";
    u.age = 30;
    u.status = true;

    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);

    Random random = mock(Random.class);
    // Test all columns in switch
    for (int i = 0; i < User.UPDATABLE_COLUMNS.size(); i++) {
      when(random.nextInt(User.UPDATABLE_COLUMNS.size())).thenReturn(i);
      u.update(conn, random);
    }
    // Verify that executeUpdate was called for each
    verify(ps, times(User.UPDATABLE_COLUMNS.size())).executeUpdate();
  }

  @Test
  public void testInsert_Spanner() {
    User u = new User();
    u.id = 1;
    u.firstName = "John";

    DatabaseClient client = mock(DatabaseClient.class);
    u.insert(client);

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(client).write(captor.capture());
    assertEquals(1, captor.getValue().size());
  }

  @Test
  public void testFetchAll_Spanner() {
    SpannerResourceManager manager = mock(SpannerResourceManager.class);
    Struct struct = mock(Struct.class);
    when(struct.getLong(User.ID)).thenReturn(1L);
    when(struct.isNull(User.FIRST_NAME)).thenReturn(false);
    when(struct.getString(User.FIRST_NAME)).thenReturn("John");
    when(struct.isNull(User.LAST_NAME)).thenReturn(true);
    when(struct.isNull(User.AGE)).thenReturn(false);
    when(struct.getLong(User.AGE)).thenReturn(30L);
    when(struct.isNull(User.STATUS)).thenReturn(false);
    when(struct.getBoolean(User.STATUS)).thenReturn(true);
    when(struct.isNull(User.COL1)).thenReturn(true);
    when(struct.isNull(User.COL2)).thenReturn(true);

    when(manager.runQuery(anyString())).thenReturn(ImmutableList.of(struct));

    Map<Integer, User> users = User.fetchAll(manager);
    assertEquals(1, users.size());
    User u = users.get(1);
    assertNotNull(u);
    assertEquals("John", u.firstName);
    assertEquals(null, u.lastName);
    assertEquals(30, u.age);
    assertTrue(u.status);
    assertEquals(0L, u.col1);
    assertEquals(0L, u.col2);
  }

  @Test
  public void testFetchAll_Spanner_NullValues() {
    SpannerResourceManager manager = mock(SpannerResourceManager.class);
    Struct struct = mock(Struct.class);
    when(struct.getLong(User.ID)).thenReturn(1L);
    when(struct.isNull(User.FIRST_NAME)).thenReturn(true);
    when(struct.isNull(User.LAST_NAME)).thenReturn(false);
    when(struct.getString(User.LAST_NAME)).thenReturn("Doe");
    when(struct.isNull(User.AGE)).thenReturn(true);
    when(struct.isNull(User.STATUS)).thenReturn(true);
    when(struct.isNull(User.COL1)).thenReturn(false);
    when(struct.getLong(User.COL1)).thenReturn(100L);
    when(struct.isNull(User.COL2)).thenReturn(false);
    when(struct.getLong(User.COL2)).thenReturn(200L);

    when(manager.runQuery(anyString())).thenReturn(ImmutableList.of(struct));

    Map<Integer, User> users = User.fetchAll(manager);
    assertEquals(1, users.size());
    User u = users.get(1);
    assertNotNull(u);
    assertEquals(null, u.firstName);
    assertEquals("Doe", u.lastName);
    assertEquals(0, u.age);
    assertFalse(u.status);
    assertEquals(100L, u.col1);
    assertEquals(200L, u.col2);
  }
}
