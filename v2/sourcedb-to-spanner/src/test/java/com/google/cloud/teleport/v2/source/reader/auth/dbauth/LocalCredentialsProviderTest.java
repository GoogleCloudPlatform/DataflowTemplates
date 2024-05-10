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
package com.google.cloud.teleport.v2.source.reader.auth.dbauth;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link GuardedStringValueProvider}. */
@RunWith(MockitoJUnitRunner.class)
public class LocalCredentialsProviderTest {
  @Test
  public void testLocalCredentialsProvider() {
    final String testUserName = "testUserName";
    final String testPassword = "testPassword";
    LocalCredentialsProvider provider =
        LocalCredentialsProvider.builder()
            .setUserName(testUserName)
            .setPassword(testPassword)
            .build();
    assertThat(provider.getUserName().get()).isEqualTo(testUserName);
    assertThat(provider.getPassword().get()).isEqualTo(testPassword);
  }
}
