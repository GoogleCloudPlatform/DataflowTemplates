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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link FlexTemplatesLogging}.
 *
 * <p>Tests which validate written log messages should assume that other background tasks may
 * concurrently be writing log messages, since registered log handlers are global. Therefore it is
 * not safe to assert on log counts or whether the retrieved log collection is empty.
 */
@RunWith(JUnit4.class)
public class FlexTemplatesLoggingTest {
  @Rule public TemporaryFolder logFolder = new TemporaryFolder();

  @Rule public RestoreSystemProperties restoreProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    Path logFileBasePath = Paths.get(logFolder.getRoot().getAbsolutePath(), "logfile.txt");
    System.setProperty(FlexTemplatesLogging.FILEPATH_PROPERTY, logFileBasePath.toString());
    LogManager.getLogManager().reset();
    FlexTemplatesLogging.reset();
  }

  @After
  public void tearDown() {
    LogManager.getLogManager().reset();
    FlexTemplatesLogging.reset();
  }

  @Test
  public void testWithDefaults() {
    FlexTemplatesLogging.initialize();

    Logger rootLogger = LogManager.getLogManager().getLogger("");
    assertEquals(1, rootLogger.getHandlers().length);
    assertEquals(Level.INFO, rootLogger.getLevel());
    Handler handler = rootLogger.getHandlers()[0];
    assertThat(handler, instanceOf(FlexTemplatesLogHandler.class));
    assertEquals(Level.INFO, handler.getLevel());
  }

  @Test
  public void testSystemOutToLogger() throws Throwable {
    FlexTemplatesLogging.initialize();
    System.out.println("afterInitialization");
    assertThat(retrieveLogLines(), hasItem(containsString("afterInitialization")));
  }

  @Test
  public void testSystemErrToLogger() throws Throwable {
    FlexTemplatesLogging.initialize();
    System.err.println("afterInitialization");
    assertThat(retrieveLogLines(), hasItem(containsString("afterInitialization")));
  }

  private List<String> retrieveLogLines() throws IOException {
    List<String> allLogLines = Lists.newArrayList();
    for (File logFile : logFolder.getRoot().listFiles()) {
      allLogLines.addAll(Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8));
    }

    return allLogLines;
  }
}
