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

import static com.google.cloud.teleport.v2.sqllauncher.LogRecordMatcher.hasLogItem;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JulHandlerOutputStream}. */
@RunWith(JUnit4.class)
public class JulHandlerOutputStreamTest {
  private static final String LOGGER_NAME = "test";

  private LogSaver handler;

  @Before
  public void setUp() {
    handler = new LogSaver();
  }

  @Test
  public void testLogOnNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.println("blah");
    assertThat(handler.getLogs(), hasLogItem(Level.INFO, "blah"));
  }

  @Test
  public void testLogRecordMetadata() {
    PrintStream printStream =
        JulHandlerOutputStream.createPrintStream(
            handler, Logger.getLogger("fooLogger"), Level.WARNING);
    printStream.println("anyMessage");

    assertThat(handler.getLogs(), not(empty()));
    LogRecord log = Iterables.get(handler.getLogs(), 0);

    assertThat(log.getLevel(), is(Level.WARNING));
    assertThat(log.getLoggerName(), is("fooLogger"));
  }

  @Test
  public void testLogOnlyUptoNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.println("blah");
    printStream.print("foo");
    assertThat(handler.getLogs(), hasLogItem("blah"));
    assertThat(handler.getLogs(), not(hasLogItem("foo")));
  }

  @Test
  public void testLogMultiLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.format("blah%nfoo%n");
    assertThat(handler.getLogs(), hasLogItem("blah"));
    assertThat(handler.getLogs(), hasLogItem("foo"));
  }

  @Test
  public void testDontLogIfNoNewLine() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.print("blah");
    assertThat(handler.getLogs(), not(hasLogItem("blah")));
  }

  @Test
  public void testLogOnFlush() {
    PrintStream printStream = createPrintStreamAdapter();
    printStream.print("blah");
    printStream.flush();
    assertThat(handler.getLogs(), hasLogItem("blah"));
  }

  @Test
  public void testLogOnClose() {
    try (PrintStream printStream = createPrintStreamAdapter()) {
      printStream.print("blah");
    }
    assertThat(handler.getLogs(), hasLogItem("blah"));
  }

  private PrintStream createPrintStreamAdapter() {
    return JulHandlerOutputStream.createPrintStream(
        handler, Logger.getLogger(LOGGER_NAME), Level.INFO);
  }
}
