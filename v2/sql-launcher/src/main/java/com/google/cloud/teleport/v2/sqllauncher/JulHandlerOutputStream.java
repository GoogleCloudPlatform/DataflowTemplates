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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * {@link OutputStream} that writes all data to a specific {@link Handler} and {@link Logger}.
 *
 * <p>Internal buffers are flushed when the system-dependent newline character is seen.
 */
public class JulHandlerOutputStream extends OutputStream {

  public static JulHandlerOutputStream create(Handler handler, Logger logger, Level logLevel) {
    return new JulHandlerOutputStream(handler, logger, logLevel);
  }

  public static PrintStream createPrintStream(Handler handler, Logger logger, Level logLevel) {
    try {
      return new PrintStream(
          new JulHandlerOutputStream(handler, logger, logLevel),
          false,
          StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(String.format("Unsupported encoding: %s", StandardCharsets.UTF_8));
    }
  }

  // This limits the number of bytes which we buffer in case we don't see a newline character.
  private static final int BUFFER_LIMIT = 1 << 14; // 16384 bytes
  private static final byte[] NEW_LINE_BYTES =
      System.lineSeparator().getBytes(StandardCharsets.UTF_8);

  private Logger logger;
  private Handler handler;
  private Level messageLevel;

  private ByteArrayOutputStream buffer;

  /** How many bytes we have seen that match the newline character. */
  private int newlineBytesMatched = 0;

  /** Reference to {@code logger.getName()} to avoid accessor on the critical path. */
  private String loggerName;

  private JulHandlerOutputStream(Handler handler, Logger logger, Level logLevel) {
    this.handler = handler;
    this.logger = logger;
    this.loggerName = logger.getName();
    this.messageLevel = logLevel;
    this.buffer = new ByteArrayOutputStream(BUFFER_LIMIT);
  }

  @Override
  public void write(int b) {
    buffer.write(b);
    // Check to see if the next byte matches further into new line string.
    if (NEW_LINE_BYTES[newlineBytesMatched] == b) {
      newlineBytesMatched += 1;
      // If we have newlineBytesMatched the entire new line, output the contents of the buffer.
      if (newlineBytesMatched == NEW_LINE_BYTES.length) {
        output();
      }
    } else {
      // Reset the match
      newlineBytesMatched = 0;
    }
    if (buffer.size() == BUFFER_LIMIT) {
      output();
    }
  }

  @Override
  public void flush() throws IOException {
    output();
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  private void output() {
    // If nothing was output, do not log anything
    if (buffer.size() == 0) {
      return;
    }
    try {
      String message = buffer.toString(StandardCharsets.UTF_8.name());
      // Strip the new line if it exists
      if (message.endsWith(System.lineSeparator())) {
        message = message.substring(0, message.length() - System.lineSeparator().length());
      }

      publish(messageLevel, message);
    } catch (UnsupportedEncodingException e) {
      publish(Level.SEVERE, String.format("Unable to decode string output to stdout/stderr %s", e));
    }
    newlineBytesMatched = 0;
    buffer.reset();
  }

  private void publish(Level level, String message) {
    if (logger.isLoggable(level)) {
      LogRecord log = new LogRecord(level, message);
      log.setLoggerName(loggerName);
      handler.publish(log);
    }
  }
}
