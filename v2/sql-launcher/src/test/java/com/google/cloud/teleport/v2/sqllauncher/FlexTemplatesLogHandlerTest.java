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
package com.google.cloud.teleport.v2.sqllauncher;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.annotation.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FlexTemplatesLogHandler}. */
@RunWith(JUnit4.class)
public class FlexTemplatesLogHandlerTest {

  /** Returns the json-escaped string for the platform specific line separator. */
  private static String escapeNewline() {
    try {
      String quoted = new ObjectMapper().writeValueAsString(System.lineSeparator());
      int len = quoted.length();
      if (len < 3 || quoted.charAt(0) != '\"' || quoted.charAt(len - 1) != '\"') {
        return "Failed to escape newline; expected quoted intermediate value";
      }
      // Strip the quotes.
      return quoted.substring(1, len - 1);

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  // Typically \n or \r\n
  private static String escapedNewline = escapeNewline();

  private static class FixedOutputStreamFactory implements Supplier<OutputStream> {
    private OutputStream[] streams;
    private int next = 0;

    public FixedOutputStreamFactory(OutputStream... streams) {
      this.streams = streams;
    }

    @Override
    public OutputStream get() {
      return streams[next++];
    }
  }

  /** Encodes a LogRecord into a Json string using the {@link FlexTemplatesLogHandler}. */
  private static String createJson(LogRecord record) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(output);
    FlexTemplatesLogHandler handler = FlexTemplatesLogHandler.forOutputStreamSupplier(factory, 0);
    // Format the record as JSON.
    handler.publish(record);
    // Decode the binary output as UTF-8 and return the generated string.
    return new String(output.toByteArray(), StandardCharsets.UTF_8);
  }

  @Before
  public void setup() {
    FlexTemplatesLogging.setJobId("testJobId");
  }

  @Test
  public void testOutputStreamRollover() throws IOException {
    ByteArrayOutputStream first = new ByteArrayOutputStream();
    ByteArrayOutputStream second = new ByteArrayOutputStream();

    LogRecord record = createLogRecord("test.message", null /* throwable */);
    String expected =
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator();

    FixedOutputStreamFactory factory = new FixedOutputStreamFactory(first, second);
    FlexTemplatesLogHandler handler =
        FlexTemplatesLogHandler.forOutputStreamSupplier(
            factory, expected.length() + 1 /* sizelimit */);

    // Using |expected|+1 for size limit means that we will rollover after writing 2 log messages.
    // We thus expect to see 2 messsages written to 'first' and 1 message to 'second',

    handler.publish(record);
    handler.publish(record);
    handler.publish(record);

    assertEquals(expected + expected, new String(first.toByteArray(), StandardCharsets.UTF_8));
    assertEquals(expected, new String(second.toByteArray(), StandardCharsets.UTF_8));
  }

  @Test
  public void testWithMessageRequiringJulFormatting() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"message\":\"test.message myFormatString\",\"thread\":\"2\","
            + "\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message {0}", null /* throwable */, "myFormatString")));
  }

  @Test
  public void testWithMessageAndException() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"ERROR\","
            + "\"message\":\"test.message\",\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\","
            + "\"exception\":\"java.lang.Throwable: exception.test.message"
            + escapedNewline
            + "\\tat declaringClass1.method1(file1.java:1)"
            + escapedNewline
            + "\\tat declaringClass2.method2(file2.java:1)"
            + escapedNewline
            + "\\tat declaringClass3.method3(file3.java:1)"
            + escapedNewline
            + "\"}"
            + System.lineSeparator(),
        createJson(createLogRecord("test.message", fakeThrowable(), Level.SEVERE)));
  }

  @Test
  public void testWithException() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"ERROR\","
            + "\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\","
            + "\"exception\":\"java.lang.Throwable: exception.test.message"
            + escapedNewline
            + "\\tat declaringClass1.method1(file1.java:1)"
            + escapedNewline
            + "\\tat declaringClass2.method2(file2.java:1)"
            + escapedNewline
            + "\\tat declaringClass3.method3(file3.java:1)"
            + escapedNewline
            + "\"}"
            + System.lineSeparator(),
        createJson(createLogRecord(null /* message */, fakeThrowable(), Level.SEVERE)));
  }

  @Test
  public void testWithoutExceptionOrMessage() throws IOException {
    assertEquals(
        "{\"timestamp\":{\"seconds\":0,\"nanos\":1000000},\"severity\":\"INFO\","
            + "\"thread\":\"2\",\"job\":\"testJobId\","
            + "\"logger\":\"LoggerName\"}"
            + System.lineSeparator(),
        createJson(createLogRecord(null /* message */, null /* throwable */)));
  }

  /**
   * @return A throwable with a fixed stack trace. *
   */
  private Throwable fakeThrowable() {
    Throwable throwable = new Throwable("exception.test.message");
    throwable.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement("declaringClass1", "method1", "file1.java", 1),
          new StackTraceElement("declaringClass2", "method2", "file2.java", 1),
          new StackTraceElement("declaringClass3", "method3", "file3.java", 1),
        });
    return throwable;
  }

  /**
   * Creates and returns a LogRecord with a given message and throwable.
   *
   * @param message The message to place in the {@link LogRecord}
   * @param throwable The throwable to place in the {@link LogRecord}
   * @param params A list of parameters to place in the {@link LogRecord}
   * @return A {@link LogRecord} with the given message and throwable.
   */
  private LogRecord createLogRecord(
      @Nullable String message, @Nullable Throwable throwable, Object... params) {
    return createLogRecord(message, throwable, Level.INFO, params);
  }

  /**
   * Creates and returns a LogRecord with a given message at the specified {@link Level} and
   * throwable.
   *
   * @param message The message to place in the {@link LogRecord}
   * @param throwable The throwable to place in the {@link LogRecord}
   * @param params A list of parameters to place in the {@link LogRecord}
   * @return A {@link LogRecord} with the given message and throwable.
   */
  private LogRecord createLogRecord(
      @Nullable String message, @Nullable Throwable throwable, Level logLevel, Object... params) {
    LogRecord logRecord = new LogRecord(logLevel, message);
    logRecord.setLoggerName("LoggerName");
    logRecord.setMillis(1L);
    logRecord.setThreadID(2);
    logRecord.setThrown(throwable);
    logRecord.setParameters(params);
    return logRecord;
  }
}
