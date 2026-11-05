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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimestampSortKeyCoder}. */
@RunWith(JUnit4.class)
public class TimestampSortKeyCoderTest {

  @Test
  public void testEncodeDecode_roundTrip() throws IOException {
    TimestampSortKey key = TimestampSortKey.cdc(1724000000L, 42L);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TimestampSortKeyCoder.of().encode(key, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    TimestampSortKey decoded = TimestampSortKeyCoder.of().decode(in);

    assertEquals(key, decoded);
    assertEquals(1724000000L, decoded.getSeconds());
    assertEquals(42L, decoded.getSubSeconds());
    assertEquals(true, decoded.isCdc());
  }

  @Test
  public void testEncodeDecode_nullHandling() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TimestampSortKeyCoder.of().encode(null, out);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    TimestampSortKey decoded = TimestampSortKeyCoder.of().decode(in);

    assertNull(decoded);
  }

  @Test
  public void testCoderProperties_deterministic() throws Exception {
    CoderProperties.coderDeterministic(
        TimestampSortKeyCoder.of(),
        TimestampSortKey.cdc(123456L, 1L),
        TimestampSortKey.cdc(123456L, 1L));
  }
}
