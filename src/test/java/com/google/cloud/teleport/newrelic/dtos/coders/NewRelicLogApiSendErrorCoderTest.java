/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic.dtos.coders;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogApiSendError;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.junit.jupiter.api.Test;

class NewRelicLogApiSendErrorCoderTest {

  private static final String PAYLOAD = "{\"message\": \"MY MESSAGE\"}";
  private static final String STATUS_MESSAGE = "Internal Server Error";
  private static final int STATUS_CODE = 500;

  @Test
  public void shouldCodeAndDecodeIntoAnEqualObject() throws IOException {
    final NewRelicLogApiSendError original =
        new NewRelicLogApiSendError(PAYLOAD, STATUS_MESSAGE, STATUS_CODE);

    final PipedInputStream input = new PipedInputStream();
    // Whatever is written to this output stream will be readable from the input one.
    final PipedOutputStream output = new PipedOutputStream(input);

    NewRelicLogApiSendErrorCoder.getInstance().encode(original, output);
    final NewRelicLogApiSendError decoded =
        NewRelicLogApiSendErrorCoder.getInstance().decode(input);

    assertThat(decoded).isEqualTo(original);
  }

  @Test
  public void shouldWorkWellWithNullValues() throws IOException {
    final NewRelicLogApiSendError original = new NewRelicLogApiSendError(null, null, null);

    final PipedInputStream input = new PipedInputStream();
    // Whatever is written to this output stream will be readable from the input one.
    final PipedOutputStream output = new PipedOutputStream(input);

    NewRelicLogApiSendErrorCoder.getInstance().encode(original, output);
    final NewRelicLogApiSendError decoded =
        NewRelicLogApiSendErrorCoder.getInstance().decode(input);

    assertThat(decoded).isEqualTo(original);
  }
}
