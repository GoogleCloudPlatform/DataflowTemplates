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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class DMLGeneratorUtilsTest {

  @Test
  public void testConvertBase64ToRawHex_Null() {
    assertNull(DMLGeneratorUtils.convertBase64ToRawHex(null));
  }

  @Test
  public void testConvertBase64ToRawHex_Empty() {
    assertEquals("", DMLGeneratorUtils.convertBase64ToRawHex(""));
  }

  @Test
  public void testConvertBase64ToRawHex_Invalid() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DMLGeneratorUtils.convertBase64ToRawHex("invalid base64!"));
  }

  @Test
  public void testConvertBase64ToRawHex_Valid() {
    // "Hello" in base64 is "SGVsbG8="
    // "Hello" in hex is "48656c6c6f"
    assertEquals("48656c6c6f", DMLGeneratorUtils.convertBase64ToRawHex("SGVsbG8="));
  }
}
