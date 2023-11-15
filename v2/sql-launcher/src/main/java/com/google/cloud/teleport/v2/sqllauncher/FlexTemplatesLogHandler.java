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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.io.CountingOutputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

/**
 * Formats {@link LogRecord} into JSON format for Cloud Logging. Any exception is represented using
 * {@link Throwable#printStackTrace()}.
 */
public class FlexTemplatesLogHandler extends Handler {

  // The diagnostic context consists of only a job id
  // The job id is known only after the point where we want to configure logging
  private static ThreadLocal<String> jobId = new ThreadLocal<>();

  static String getJobId() {
    return jobId.get();
  }

  static void setJobId(String newJobId) {
    jobId.set(newJobId);
  }

  private static String julLevelToCloudLogLevel(Level julLevel) {
    if (julLevel.intValue() <= Level.FINE.intValue()) {
      return "DEBUG";
    } else if (julLevel.intValue() <= Level.INFO.intValue()) {
      return "INFO";
    } else if (julLevel.intValue() <= Level.WARNING.intValue()) {
      return "WARNING";
    } else {
      return "ERROR";
    }
  }

  /**
   * Formats the throwable as per {@link Throwable#printStackTrace()}.
   *
   * @param thrown The throwable to format.
   * @return A string containing the contents of {@link Throwable#printStackTrace()}.
   */
  public static String formatException(Throwable thrown) {
    if (thrown == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      thrown.printStackTrace(pw);
    }
    return sw.toString();
  }

  /** Constructs a handler that writes to a rotating set of files. */
  public static FlexTemplatesLogHandler forFile(File path, long sizeLimit) throws IOException {
    return FlexTemplatesLogHandler.forOutputStreamSupplier(
        new FileOutputStreamFactory(path), sizeLimit);
  }

  /**
   * Constructs a handler that writes to arbitrary output streams. No rollover if sizeLimit is zero
   * or negative.
   */
  public static FlexTemplatesLogHandler forOutputStreamSupplier(
      Supplier<OutputStream> factory, long sizeLimit) throws IOException {
    return new FlexTemplatesLogHandler(factory, sizeLimit);
  }

  private FlexTemplatesLogHandler(Supplier<OutputStream> factory, long sizeLimit)
      throws IOException {
    this.outputStreamFactory = factory;
    this.generatorFactory = new ObjectMapper().getFactory();
    this.sizeLimit = sizeLimit < 1 ? Long.MAX_VALUE : sizeLimit;
    createOutputStream();
  }

  @Override
  public synchronized void publish(LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }

    rolloverOutputStreamIfNeeded();

    try {
      // Generating a JSON map like:
      // {"timestamp": {"seconds": 1435835832, "nanos": 123456789}, ...  "message": "hello"}
      generator.writeStartObject();
      // Write the timestamp.
      generator.writeFieldName("timestamp");
      generator.writeStartObject();
      generator.writeNumberField("seconds", record.getMillis() / 1000);
      generator.writeNumberField("nanos", (record.getMillis() % 1000) * 1000000);
      generator.writeEndObject();
      // Write the severity.
      generator.writeObjectField("severity", julLevelToCloudLogLevel(record.getLevel()));
      // Write the other labels.
      writeIfNotEmpty("message", formatter.formatMessage(record));
      writeIfNotEmpty("thread", String.valueOf(record.getThreadID()));
      writeIfNotEmpty("job", FlexTemplatesLogging.getJobId());
      writeIfNotEmpty("logger", record.getLoggerName());
      writeIfNotEmpty("exception", formatException(record.getThrown()));
      generator.writeEndObject();
      generator.writeRaw(System.lineSeparator());
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to publish", e, ErrorManager.WRITE_FAILURE);
    }

    // This implementation is based on that of java.util.logging.FileHandler, which flushes in a
    // synchronized context like this. Unfortunately the maximum throughput for generating log
    // entries will be the inverse of the flush latency. That could be as little as one hundred
    // log entries per second on some systems. For higher throughput this should be changed to
    // batch publish operations while writes and flushes are in flight on a different thread.
    flush();
  }

  /**
   * Check if a LogRecord will be logged.
   *
   * <p>This method checks if the <tt>LogRecord</tt> has an appropriate level and whether it
   * satisfies any <tt>Filter</tt>. It will also return false if the handler has been closed, or the
   * LogRecord is null.
   */
  @Override
  public boolean isLoggable(LogRecord record) {
    return generator != null && record != null && super.isLoggable(record);
  }

  @Override
  public synchronized void flush() {
    try {
      if (generator != null) {
        generator.flush();
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to flush", e, ErrorManager.FLUSH_FAILURE);
    }
  }

  @Override
  public synchronized void close() {
    // Flush any in-flight content, though there should not actually be any because
    // the generator is currently flushed in the synchronized publish() method.
    flush();
    // Close the generator and log file.
    try {
      if (generator != null) {
        generator.close();
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to close", e, ErrorManager.CLOSE_FAILURE);
    } finally {
      generator = null;
      counter = null;
    }
  }

  /** Unique file generator. Uses filenames with timestamp. */
  private static final class FileOutputStreamFactory implements Supplier<OutputStream> {
    private final File filepath;
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");

    public FileOutputStreamFactory(File filepath) {
      this.filepath = filepath;
    }

    @Override
    public OutputStream get() {
      try {
        String filename = filepath + "." + formatter.format(new Date()) + ".log";
        return new BufferedOutputStream(
            new FileOutputStream(new File(filename), true /* append */));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void createOutputStream() throws IOException {
    CountingOutputStream stream = new CountingOutputStream(outputStreamFactory.get());
    generator = generatorFactory.createGenerator(stream, JsonEncoding.UTF8);
    counter = stream;

    // Avoid 1 space indent for every line. We already add a newline after each log record.
    generator.setPrettyPrinter(new MinimalPrettyPrinter(""));
  }

  /**
   * Rollover to a new output stream (log file) if we have reached the size limit. Ensure that the
   * rollover fails or succeeds atomically.
   */
  private void rolloverOutputStreamIfNeeded() {
    if (counter.getCount() < sizeLimit) {
      return;
    }

    try {
      JsonGenerator old = generator;
      createOutputStream();

      try {
        // Rollover successful. Attempt to close old stream, but ignore on failure.
        old.close();
      } catch (IOException | RuntimeException e) {
        reportFailure("Unable to close old log file", e, ErrorManager.CLOSE_FAILURE);
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to create new log file", e, ErrorManager.OPEN_FAILURE);
    }
  }

  /** Appends a JSON key/value pair if the specified val is not null. */
  private void writeIfNotEmpty(String name, String val) throws IOException {
    if (val != null && !val.isEmpty()) {
      generator.writeStringField(name, val);
    }
  }

  /** Report logging failure to ErrorManager. Does not throw. */
  private void reportFailure(String message, Exception e, int code) {
    try {
      ErrorManager manager = getErrorManager();
      if (manager != null) {
        manager.error(message, e, code);
      }
    } catch (Throwable t) {
      // Failed to report logging failure. No meaningful action left.
    }
  }

  // Null after close().
  private JsonGenerator generator;
  private CountingOutputStream counter;

  private final long sizeLimit;
  private final Supplier<OutputStream> outputStreamFactory;
  private final JsonFactory generatorFactory;
  private final Formatter formatter = new SimpleFormatter();
}
