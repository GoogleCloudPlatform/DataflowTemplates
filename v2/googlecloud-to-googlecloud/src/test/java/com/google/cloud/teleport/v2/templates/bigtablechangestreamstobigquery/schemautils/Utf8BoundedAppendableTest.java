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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Utf8BoundedAppendable}. */
@RunWith(JUnit4.class)
public class Utf8BoundedAppendableTest {

  @Test
  public void asciiCountedOneBytePerChar() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    sb.append("hello world");
    assertEquals(11L, sb.byteCount());
    assertEquals("hello world", sb.toJson());
    assertEquals(11, "hello world".getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  public void twoByteCharCountedAsTwo() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    sb.append("\u00e9"); // "é" -> 0xC3 0xA9
    assertEquals(2L, sb.byteCount());
    assertEquals(2, "\u00e9".getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  public void threeByteCharCountedAsThree() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    sb.append("\u4e2d"); // "中" -> 3 bytes
    assertEquals(3L, sb.byteCount());
    assertEquals(3, "\u4e2d".getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  public void supplementaryCodePointCountedAsFour() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    // U+1F600 GRINNING FACE, encoded as a surrogate pair in Java, 4 UTF-8 bytes.
    String emoji = new String(Character.toChars(0x1F600));
    sb.append(emoji);
    assertEquals(4L, sb.byteCount());
    assertEquals(4, emoji.getBytes(StandardCharsets.UTF_8).length);
  }

  @Test
  public void mixedCharactersAccumulateExactly() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    String text = "a\u00e9\u4e2d" + new String(Character.toChars(0x1F600));
    sb.append(text);
    // 1 + 2 + 3 + 4 = 10
    assertEquals(10L, sb.byteCount());
    assertEquals(text.getBytes(StandardCharsets.UTF_8).length, sb.byteCount());
  }

  @Test
  public void boundaryExactlyAtMaxBytesPasses() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(5);
    sb.append("hello"); // exactly 5 bytes
    assertEquals(5L, sb.byteCount());
  }

  @Test
  public void oneByteOverThrows() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(5);
    assertThrows(OversizedJsonException.class, () -> sb.append("hello!"));
  }

  @Test
  public void multibyteOverflowThrowsAtFirstExceedingChar() {
    // max = 3 bytes; "é" = 2 bytes, a second "é" would be 4 bytes total -> throws on second append.
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(3);
    sb.append("\u00e9");
    assertEquals(2L, sb.byteCount());
    assertThrows(OversizedJsonException.class, () -> sb.append("\u00e9"));
  }

  @Test
  public void appendCharByCharTracksBytesCorrectly() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    sb.append('a');
    sb.append('b');
    sb.append('c');
    assertEquals(3L, sb.byteCount());
    assertEquals("abc", sb.toJson());
  }

  @Test
  public void zeroOrNegativeMaxBytesDisablesBound() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(0);
    String bigger = "a".repeat(10_000);
    sb.append(bigger);
    assertEquals(10_000L, sb.byteCount());
  }

  @Test
  public void appendSubSequenceRespectsOffsetAndCountsBytes() {
    Utf8BoundedAppendable sb = new Utf8BoundedAppendable(100);
    sb.append("hello world", 6, 11); // "world"
    assertEquals(5L, sb.byteCount());
    assertEquals("world", sb.toJson());
  }
}
