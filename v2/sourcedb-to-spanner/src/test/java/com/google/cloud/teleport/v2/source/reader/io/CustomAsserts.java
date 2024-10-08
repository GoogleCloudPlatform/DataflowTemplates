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
package com.google.cloud.teleport.v2.source.reader.io;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

public class CustomAsserts {

  private static final double DELTA = 0.0001f;

  private CustomAsserts() {}

  public static void assertColumnEquals(String message, Object expected, Object actual) {
    if (expected instanceof byte[] && actual instanceof byte[]) {
      assertArrayEquals(message, (byte[]) expected, (byte[]) actual);
    } else if (expected instanceof byte[] && actual instanceof ByteBuffer) {
      assertArrayEquals(message, (byte[]) expected, ((ByteBuffer) actual).array());
    } else if (expected instanceof Float && actual instanceof Float) {
      assertEquals(message, (float) expected, (float) actual, DELTA);
    } else if (expected instanceof Double && actual instanceof Double) {
      assertEquals(message, (double) expected, (double) actual, DELTA);
    } else {
      assertEquals(message, expected, actual);
    }
  }
}
