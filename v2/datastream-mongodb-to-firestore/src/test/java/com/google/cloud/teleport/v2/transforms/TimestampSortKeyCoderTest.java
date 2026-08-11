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
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimestampSortKeyCoder}. */
@RunWith(JUnit4.class)
public class TimestampSortKeyCoderTest {

  private final TimestampSortKeyCoder coder = TimestampSortKeyCoder.of();

  @Test
  public void testEncodeDecodeRoundTrip_cdcEvent() throws Exception {
    TimestampSortKey key = TimestampSortKey.of(1786382543L, 4752, true);
    TimestampSortKey decoded = CoderUtils.clone(coder, key);

    assertEquals(key, decoded);
    assertEquals(1786382543L, decoded.getSeconds());
    assertEquals(4752, decoded.getSubSeconds());
    assertEquals(true, decoded.isCdc());
    CoderProperties.coderDecodeEncodeEqual(coder, key);
  }

  @Test
  public void testEncodeDecodeRoundTrip_backfillEvent() throws Exception {
    TimestampSortKey key = TimestampSortKey.of(1786382543L, 967669000, false);
    TimestampSortKey decoded = CoderUtils.clone(coder, key);

    assertEquals(key, decoded);
    assertEquals(1786382543L, decoded.getSeconds());
    assertEquals(967669000, decoded.getSubSeconds());
    assertEquals(false, decoded.isCdc());
    CoderProperties.coderDecodeEncodeEqual(coder, key);
  }

  @Test
  public void testEncodeDecodeRoundTrip_boundaryValues() throws Exception {
    TimestampSortKey minKey = TimestampSortKey.of(0L, 0, false);
    CoderProperties.coderDecodeEncodeEqual(coder, minKey);

    TimestampSortKey maxKey = TimestampSortKey.of(Long.MAX_VALUE, Integer.MAX_VALUE, true);
    CoderProperties.coderDecodeEncodeEqual(coder, maxKey);
  }

  @Test
  public void testNullValueEncoding() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(null, out);
    byte[] bytes = out.toByteArray();

    assertEquals(1, bytes.length);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    TimestampSortKey decoded = coder.decode(in);
    assertNull(decoded);
  }

  @Test
  public void testVerifyDeterministic() throws Exception {
    coder.verifyDeterministic();

    TimestampSortKey key1 = TimestampSortKey.of(1000L, 50, true);
    TimestampSortKey key2 = TimestampSortKey.of(1000L, 50, true);
    CoderProperties.coderDeterministic(coder, key1, key2);
  }

  @Test
  public void testBinaryEncodingExactSize() throws Exception {
    TimestampSortKey key = TimestampSortKey.of(1786382543L, 4752, true);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(key, out);

    // 1 byte presence + 8 bytes long + 4 bytes int + 1 byte boolean = 14 bytes
    assertEquals(14, out.toByteArray().length);
  }
}
