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
package com.google.cloud.teleport.datadog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import com.google.gson.JsonObject;
import org.junit.Test;

/** Unit tests for {@link DatadogEvent} class. */
public class DatadogEventTest {

  /** Test whether a {@link DatadogEvent} created via its builder can be compared correctly. */
  @Test
  public void testEquals() {

    String event = "test-event";
    String host = "test-host";
    String index = "test-index";
    String source = "test-source";
    String sourceType = "test-source-type";
    Long time = 123456789L;
    JsonObject fields = new JsonObject();
    fields.addProperty("test-key", "test-value");

    DatadogEvent actualEvent =
        DatadogEvent.newBuilder()
            .withEvent(event)
            .withHost(host)
            .withIndex(index)
            .withSource(source)
            .withSourceType(sourceType)
            .withTime(time)
            .withFields(fields)
            .build();

    assertThat(
        actualEvent,
        is(
            equalTo(
                DatadogEvent.newBuilder()
                    .withEvent(event)
                    .withHost(host)
                    .withIndex(index)
                    .withSource(source)
                    .withSourceType(sourceType)
                    .withTime(time)
                    .withFields(fields)
                    .build())));

    assertThat(
        actualEvent,
        is(
            not(
                equalTo(
                    DatadogEvent.newBuilder()
                        .withEvent("a-different-test-event")
                        .withHost(host)
                        .withIndex(index)
                        .withSource(source)
                        .withSourceType(sourceType)
                        .withTime(time)
                        .withFields(fields)
                        .build()))));
  }
}
