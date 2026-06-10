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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SecretManagerAccessorImplTest {

  private SecretManagerAccessorImpl accessor;

  @Before
  public void setUp() {
    // Spy on the accessor so we can mock out the actual call to SecretManagerUtils
    accessor = spy(new SecretManagerAccessorImpl());
  }

  @Test
  public void testResolvePassword_NullUri() {
    String password = accessor.resolvePassword(null, "shard1", "plain_password");
    assertEquals("plain_password", password);
  }

  @Test
  public void testResolvePassword_EmptyUri() {
    String password = accessor.resolvePassword("", "shard1", "plain_password");
    assertEquals("plain_password", password);
  }

  @Test
  public void testResolvePassword_FullUri() {
    doReturn("secret_password").when(accessor).getSecret("projects/p1/secrets/s1/versions/v1");
    String password =
        accessor.resolvePassword("projects/p1/secrets/s1/versions/v1", "shard1", "plain_password");
    assertEquals("secret_password", password);
    verify(accessor).getSecret("projects/p1/secrets/s1/versions/v1");
  }

  @Test
  public void testResolvePassword_PartialUriWithoutSlash() {
    doReturn("secret_password").when(accessor).getSecret("projects/p1/secrets/s1/versions/latest");
    String password =
        accessor.resolvePassword("projects/p1/secrets/s1", "shard1", "plain_password");
    assertEquals("secret_password", password);
    verify(accessor).getSecret("projects/p1/secrets/s1/versions/latest");
  }

  @Test
  public void testResolvePassword_PartialUriWithSlash() {
    doReturn("secret_password").when(accessor).getSecret("projects/p1/secrets/s1/versions/latest");
    String password =
        accessor.resolvePassword("projects/p1/secrets/s1/", "shard1", "plain_password");
    assertEquals("secret_password", password);
    verify(accessor).getSecret("projects/p1/secrets/s1/versions/latest");
  }

  @Test
  public void testResolvePassword_InvalidUri() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> accessor.resolvePassword("invalid_uri", "shard1", "plain_password"));
    assertEquals(
        "The secretManagerUri field with value invalid_uri for shard shard1, "
            + "specified in source config file does not adhere to expected pattern "
            + "projects/{project}/secrets/{secret}/versions/{version}.",
        exception.getMessage());
  }
}
