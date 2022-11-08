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
package com.google.cloud.teleport.v2.logging;

import static com.google.common.truth.Truth.assertThat;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for structured Json logging. */
public class JsonLoggingAppenderTest {

  private static final String LOGGER_NAME = "TestLogger";
  private static final String TEST_MESSAGE = "test message";
  public static final String EXPECTED_OUTPUT_MESSAGE = LOGGER_NAME + " - " + TEST_MESSAGE;
  public static final String TEST_EXCEPTION_MESSAGE = "Test Error!!!";
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);

  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalErr = System.err;

  @Before
  public void setUpStreams() {
    System.setErr(new PrintStream(errContent));
  }

  @After
  public void restoreStreams() {
    System.setErr(originalErr);
  }

  @Test
  public void testLoggingMessage() {
    LOG.info(TEST_MESSAGE);
    assertThat(GSON.fromJson(errContent.toString(), JsonObject.class).get("message").getAsString())
        .isEqualTo(EXPECTED_OUTPUT_MESSAGE);
  }

  @Test
  public void testLoggingException() {
    LOG.info(TEST_MESSAGE, new Exception(TEST_EXCEPTION_MESSAGE));
    String resultOutputMessage =
        GSON.fromJson(errContent.toString(), JsonObject.class).get("message").getAsString();
    assertThat(resultOutputMessage).contains(EXPECTED_OUTPUT_MESSAGE);
    assertThat(resultOutputMessage).contains(TEST_EXCEPTION_MESSAGE);
    assertThat(resultOutputMessage).contains(Exception.class.getName());
  }

  @Test
  public void testLoggingSeverityInfo() {
    LOG.info("test message");
    assertThat(GSON.fromJson(errContent.toString(), JsonObject.class).get("severity").getAsString())
        .isEqualTo("INFO");
  }

  @Test
  public void testLoggingSeverityError() {
    LOG.error("test message");
    assertThat(GSON.fromJson(errContent.toString(), JsonObject.class).get("severity").getAsString())
        .isEqualTo("ERROR");
  }

  @Test
  public void testLoggingSeverityWarn() {
    LOG.warn("test message");
    assertThat(GSON.fromJson(errContent.toString(), JsonObject.class).get("severity").getAsString())
        .isEqualTo("WARN");
  }

  @Test
  public void testLoggingDebugIsDisabledByDefault() {
    assertThat(LOG.isDebugEnabled()).isFalse();
  }
}
