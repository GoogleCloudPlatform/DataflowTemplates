/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.splunk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import org.junit.Test;

/** Unit tests for {@link SplunkWriteError} class. */
public class SplunkWriteErrorTest {

  /** Test whether a {@link SplunkWriteError} created via its builder can be compared correctly. */
  @Test
  public void testEquals() {

    String payload = "test-payload";
    String message = "test-message";
    Integer statusCode = 123;

    SplunkWriteError actualError =
        SplunkWriteError.newBuilder()
            .withPayload(payload)
            .withStatusCode(statusCode)
            .withStatusMessage(message)
            .build();

    assertThat(
        actualError,
        is(
            equalTo(
                SplunkWriteError.newBuilder()
                    .withPayload(payload)
                    .withStatusCode(statusCode)
                    .withStatusMessage(message)
                    .build())));

    assertThat(
        actualError,
        is(
            not(
                equalTo(
                    SplunkWriteError.newBuilder()
                        .withPayload(payload)
                        .withStatusCode(statusCode)
                        .withStatusMessage("a-different-message")
                        .build()))));
  }
}
