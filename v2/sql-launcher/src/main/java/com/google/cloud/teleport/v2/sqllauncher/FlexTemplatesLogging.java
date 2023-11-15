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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Configuration for {@link java.util.logging} to interact with Flex Templates.
 *
 * <ul>
 *   <li>Destination: rotating files in a reserved location that the Flex Template launcher watches.
 *       Can be overridden by property {@code dataflow.sqllauncher.logging.basepath}.
 *   <li>Format: defined by {@link FlexTemplatesLogHandler}
 * </ul>
 *
 * <p>Note that this is forked from {@code DataflowWorkerLoggingInitializer} but deliberately does
 * not declare a dependency on the internal details of the Dataflow Java worker.
 */
public class FlexTemplatesLogging {

  /**
   * Base file path that Flex Templates tells us to log.
   *
   * <p>The Flex Templates framework owns the name of this property and may set it. Our Java code
   * must respect the instruction of the Flex Templates framework.
   */
  @VisibleForTesting
  static final String FILEPATH_PROPERTY = "dataflow.flextemplates.logging.filepath";

  /**
   * File size before starting a new file that Flex Templates tells us.
   *
   * <p>The Flex Templates framework owns the name of this property and may set it. Our Java code
   * must respect the instruction of the Flex Templates framework.
   */
  @VisibleForTesting
  static final String FILESIZE_MB_PROPERTY = "dataflow.flextemplates.logging.filesize_mb";

  /**
   * Log level that Flex Templates tells us.
   *
   * <p>The Flex Templates framework owns the name of this property and may set it. Our Java code
   * must respect the instruction of the Flex Templates framework.
   */
  @VisibleForTesting static final String LEVEL_PROPERTY = "dataflow.flextemplates.logging.level";

  /**
   * Default log location for Flex Templates.
   *
   * <p>The Flex Templates framework always expects logs in this directory, unless it uses the
   * properties above to choose a different place.
   */
  @VisibleForTesting
  static final String DEFAULT_FILEPATH = "/var/log/dataflow/template_launcher/java-sql.log";

  @VisibleForTesting static final String DEFAULT_FILESIZE_MB = "1024";

  @VisibleForTesting static final String DEFAULT_LEVEL = "INFO";

  // Misc magic strings
  private static final String ROOT_LOGGER_NAME = "";
  private static final String SYSTEM_OUT_LOG_NAME = "System.out";
  private static final String SYSTEM_ERR_LOG_NAME = "System.err";

  private static PrintStream originalStdOut;
  private static PrintStream originalStdErr = System.err;
  private static Handler handler;
  private static boolean initialized = false;

  public static String getJobId() {
    return FlexTemplatesLogHandler.getJobId();
  }

  public static void setJobId(String newJobId) {
    FlexTemplatesLogHandler.setJobId(newJobId);
  }

  /** Initialize logging configuration. */
  public static synchronized void initialize() {
    if (initialized) {
      return;
    }

    try {
      File filepath = new File(System.getProperty(FILEPATH_PROPERTY, DEFAULT_FILEPATH));
      int filesizeMb =
          Integer.parseInt(System.getProperty(FILESIZE_MB_PROPERTY, DEFAULT_FILESIZE_MB));
      Level level = Level.parse(System.getProperty(LEVEL_PROPERTY, DEFAULT_LEVEL));

      handler = FlexTemplatesLogHandler.forFile(filepath, filesizeMb * 1024L * 1024L);
      handler.setLevel(level);

      Logger rootLogger = LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME);
      for (Handler existingHandler : rootLogger.getHandlers()) {
        rootLogger.removeHandler(existingHandler);
      }

      rootLogger.setLevel(level);
      rootLogger.addHandler(handler);

      // Direct stdout to INFO log and stderr to SEVERE log. It isn't perfect, but it will
      // ensure that messages show up and that crash stack traces are shown as errors.
      originalStdOut = System.out;
      originalStdErr = System.err;
      System.setOut(
          JulHandlerOutputStream.createPrintStream(
              handler, Logger.getLogger(SYSTEM_OUT_LOG_NAME), Level.INFO));
      System.setErr(
          JulHandlerOutputStream.createPrintStream(
              handler, Logger.getLogger(SYSTEM_ERR_LOG_NAME), Level.SEVERE));

      initialized = true;
    } catch (SecurityException | IOException | NumberFormatException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static synchronized void consoleError(String err) {
    if (!initialized) {
      return;
    }
    originalStdErr.println(err);
  }

  public static synchronized void consoleStackTrace(Throwable e) {
    if (!initialized) {
      return;
    }
    e.printStackTrace(originalStdErr);
  }

  public static synchronized void flush() {
    handler.flush();
    System.out.flush();
    System.err.flush();
  }

  @VisibleForTesting
  public static synchronized void reset() {
    if (!initialized) {
      return;
    }
    System.setOut(originalStdOut);
    System.setErr(originalStdErr);
    initialized = false;
  }
}
