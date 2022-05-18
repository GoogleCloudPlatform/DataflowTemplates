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
import org.apache.beam.sdk.io.splunk.SplunkWriteError;
import org.junit.Test;

/** Unit tests for {@link SplunkWriteErrorCoder} class. */
public final class SplunkWriteErrorCoderTest {

  @Test
  public void testEncodeDecode() throws IOException {
    String payload = "test-payload";
    String message = "test-message";
    Integer statusCode = 123;

    SplunkWriteError actualError =
        SplunkWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage(message)
            .create();

    SplunkWriteErrorCoder coder = SplunkWriteErrorCoder.of();
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      coder.encode(actualError, bos);
      try (ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())) {
        SplunkWriteError decodedWriteError = coder.decode(bin);
        assertThat(decodedWriteError, is(equalTo(actualError)));
      }
    }
  }
}
