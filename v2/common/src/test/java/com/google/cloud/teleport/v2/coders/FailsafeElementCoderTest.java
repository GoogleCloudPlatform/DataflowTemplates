/*
 * Copyright (C) 2019 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link FailsafeElementCoder} class. */
@RunWith(JUnit4.class)
public class FailsafeElementCoderTest {

  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static String originalPayload = "{\"agent\":\"007\"}";
  private FailsafeElement<String, String> failsafeElement =
      FailsafeElement.of(originalPayload, originalPayload);

  @Test
  public void testEncoding() {
    OutputStream out = mock(OutputStream.class);
    FailsafeElementCoder failsafeElementCoder = FailsafeElementCoder.of(STRING_CODER, STRING_CODER);
    try {
      failsafeElementCoder.encode(failsafeElement, out);
    } catch (IOException ignored) {
    }

    assertEquals(failsafeElement.getOriginalPayload(), originalPayload);
  }
}
