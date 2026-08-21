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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link UriSanitizer}. */
@RunWith(JUnit4.class)
public class UriSanitizerTest {

  @Test
  public void testSanitize_standardUriWithPassword() {
    String uri = "mongodb://user:secretPassword@localhost:27017/db";
    String sanitized = UriSanitizer.sanitize(uri);
    assertEquals("mongodb://user:****@localhost:27017/db", sanitized);
  }

  @Test
  public void testSanitize_srvUriWithPassword() {
    String uri = "mongodb+srv://admin:pass123!@cluster0.example.com/test?retryWrites=true&tls=true";
    String sanitized = UriSanitizer.sanitize(uri);
    assertEquals(
        "mongodb+srv://admin:****@cluster0.example.com/test?retryWrites=true&tls=true", sanitized);
  }

  @Test
  public void testSanitize_uriWithoutPassword() {
    String uri = "mongodb://localhost:27017/db";
    String sanitized = UriSanitizer.sanitize(uri);
    assertEquals("mongodb://localhost:27017/db", sanitized);
  }

  @Test
  public void testSanitize_nullOrEmpty() {
    assertNull(UriSanitizer.sanitize(null));
    assertEquals("", UriSanitizer.sanitize(""));
  }
}
