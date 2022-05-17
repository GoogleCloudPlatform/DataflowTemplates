/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.coders;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

/** Unit tests for {@link SplunkEventCoder} class. */
public class SplunkEventCoderTest {

  /**
   * Test whether {@link SplunkEventCoder} is able to encode/decode a {@link SplunkEvent} correctly.
   *
   * @throws IOException
   */
  @Test
  public void testEncodeDecode() throws IOException {

    String event = "test-event";
    String host = "test-host";
    String index = "test-index";
    String source = "test-source";
    String sourceType = "test-source-type";
    Long time = 123456789L;

    SplunkEvent actualEvent =
        SplunkEvent.newBuilder()
            .withEvent(event)
            .withHost(host)
            .withIndex(index)
            .withSource(source)
            .withSourceType(sourceType)
            .withTime(time)
            .create();

    SplunkEventCoder coder = SplunkEventCoder.of();
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      coder.encode(actualEvent, bos);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())) {
        SplunkEvent decodedEvent = coder.decode(bin);
        assertThat(decodedEvent, is(equalTo(actualEvent)));
      }
    }
  }

  /**
   * Tests whether {@link SplunkEventCoder} is able to decode a {@link SplunkEvent} encoded using
   * the older coder version 1 (commit f0ff6cc).
   */
  @Test
  public void testBackwardsCompatibility_canDecodeVersion1() throws IOException, DecoderException {

    SplunkEvent expectedEvent =
        SplunkEvent.newBuilder()
            .withEvent("e")
            .withHost("h")
            .withIndex("i")
            .withSource("s")
            .withSourceType("st")
            .withTime(1234L)
            .create();

    String hex = "0100000000000004d2010168010173010273740101690165";
    SplunkEvent actualEvent = SplunkEventCoder.of().decode(fromHex(hex));

    assertThat(actualEvent, is(equalTo(expectedEvent)));
  }

  /**
   * Tests whether {@link SplunkEventCoder} is able to decode a {@link SplunkEvent} encoded using
   * the older coder version 1 (commit f0ff6cc) and having an empty "event" field.
   *
   * <p>An empty field is encoded as <code>00</code>, which may look like the present/not present
   * marker for the "fields" field in V2.
   */
  @Test
  public void testBackwardsCompatibility_canDecodeVersion1withEmptyEvent()
      throws IOException, DecoderException {

    SplunkEvent expectedEvent =
        SplunkEvent.newBuilder()
            .withEvent("")
            .withHost("h")
            .withIndex("i")
            .withSource("s")
            .withSourceType("st")
            .withTime(1234L)
            .create();

    String hex = "0100000000000004d20101680101730102737401016900";
    SplunkEvent actualEvent = SplunkEventCoder.of().decode(fromHex(hex));

    assertThat(actualEvent, is(equalTo(expectedEvent)));
  }

  /**
   * Tests whether {@link SplunkEventCoder} is able to decode a {@link SplunkEvent} encoded using
   * the older coder version 1 (commit f0ff6cc) and having the "event" field of length 1.
   *
   * <p>This is a special case when "event" is of length 1 and the first character code is 00. This
   * is encoded as byte sequence 01 00 by V1 coder, which can be treated as an empty "fields" field
   * by V2 decoder.
   */
  @Test
  public void testBackwardsCompatibility_canDecodeVersion1withEventLength1()
      throws IOException, DecoderException {

    SplunkEvent expectedEvent =
        SplunkEvent.newBuilder()
            .withEvent(new String(new byte[] {0}, StandardCharsets.UTF_8))
            .withHost("h")
            .withIndex("i")
            .withSource("s")
            .withSourceType("st")
            .withTime(1234L)
            .create();

    String hex = "0100000000000004d2010168010173010273740101690100";
    SplunkEvent actualEvent = SplunkEventCoder.of().decode(fromHex(hex));

    assertThat(actualEvent, is(equalTo(expectedEvent)));
  }

  /**
   * Tests whether {@link SplunkEventCoder} is able to decode a {@link SplunkEvent} encoded using
   * the older coder version 2 (commit 5e53040), without the newly added "fields" field.
   */
  @Test
  public void testBackwardsCompatibility_canDecodeVersion2() throws IOException, DecoderException {

    SplunkEvent expectedEvent =
        SplunkEvent.newBuilder()
            .withEvent("e")
            .withHost("h")
            .withIndex("i")
            .withSource("s")
            .withSourceType("st")
            .withTime(1234L)
            .create();

    String hex = "0100000000000004d201016801017301027374010169000165";
    SplunkEvent actualEvent = SplunkEventCoder.of().decode(fromHex(hex));

    assertThat(actualEvent, is(equalTo(expectedEvent)));
  }

  private static InputStream fromHex(String hex) throws DecoderException {
    byte[] b = Hex.decodeHex(hex);
    return new ByteArrayInputStream(b);
  }
}
