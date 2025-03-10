/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class DurationCodecTest {

  private DurationCodec durationCodec;

  @Before
  public void setUp() {
    durationCodec = new DurationCodec();
  }

  @Test
  public void testGetJavaType() {
    assertEquals(GenericType.DURATION, durationCodec.getJavaType());
  }

  @Test
  public void testGetCqlType() {
    assertEquals(DataTypes.DURATION, durationCodec.getCqlType());
  }

  @Test
  public void testEncode() {
    Duration duration = Duration.ofDays(60).plusNanos(123456789);
    ByteBuffer encoded = durationCodec.encode(duration, ProtocolVersion.DEFAULT);

    assertNotNull(encoded);
    assertTrue(encoded.remaining() > 0);
  }

  @Test
  public void testEncodeNull() {
    assertNull(durationCodec.encode(null, ProtocolVersion.DEFAULT));
  }

  @Test
  public void testDecode() {
    Duration originalDuration = Duration.ofDays(60).plusNanos(123456789);
    ByteBuffer encoded = durationCodec.encode(originalDuration, ProtocolVersion.DEFAULT);
    Duration decoded = durationCodec.decode(encoded, ProtocolVersion.DEFAULT);

    assertNotNull(decoded);
    assertEquals(originalDuration, decoded);
  }

  @Test
  public void testDecodeNull() {
    assertNull(durationCodec.decode(null, ProtocolVersion.DEFAULT));
    assertNull(durationCodec.decode(ByteBuffer.allocate(0), ProtocolVersion.DEFAULT));
  }

  @Test
  public void testParse() {
    String durationString =
        "2mo15d";
    Duration parsed = durationCodec.parse(durationString);

    assertNotNull(parsed);
    assertEquals(
        Duration.ofDays(75),
        parsed);
  }

  @Test
  public void testParseNull() {
    assertNull(durationCodec.parse(null));
    assertNull(durationCodec.parse("NULL"));
  }

  @Test
  public void testFormat() {
    Duration duration = Duration.ofDays(60).plusNanos(123456789);
    String formatted = durationCodec.format(duration);

    assertNotNull(formatted);
    assertFalse("NULL".equals(formatted));
  }

  @Test
  public void testFormatNull() {
    assertEquals("NULL", durationCodec.format(null));
  }
}
