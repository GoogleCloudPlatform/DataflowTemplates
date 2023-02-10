/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test for SecretManagerUtils. */
public class SecretManagerUtilsTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetSecretInvalidSecret() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Provided Secret must be in the form"
            + " projects/{project}/secrets/{secret}/versions/{secret_version}");

    SecretManagerUtils.getSecret("invalid");
  }
}
