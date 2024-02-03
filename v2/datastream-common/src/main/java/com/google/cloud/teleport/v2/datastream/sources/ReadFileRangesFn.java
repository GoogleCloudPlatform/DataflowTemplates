/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.datastream.sources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Semaphore;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CompressedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads each file in the input {@link PCollection} of {@link ReadableFile} using given parameters
 * for splitting files into offset ranges and for creating a {@link FileBasedSource} for a file. The
 * input {@link PCollection} must not contain {@link ResourceId#isDirectory directories}.
 *
 * <p>To obtain the collection of {@link ReadableFile} from a filepattern, use {@link
 * FileIO#readMatches()}.
 */
public class ReadFileRangesFn<T> extends DoFn<ReadableFile, T> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ReadFileRangesFn.class);
  private final SerializableFunction<String, ? extends FileBasedSource<T>> createSource;
  private final ReadFileRangesFnExceptionHandler exceptionHandler;
  private boolean acquiredPermit = false;
  private static final Semaphore jvmThreads = new Semaphore(5, true);

  public ReadFileRangesFn(
      SerializableFunction<String, ? extends FileBasedSource<T>> createSource,
      ReadFileRangesFnExceptionHandler exceptionHandler) {
    this.createSource = createSource;
    this.exceptionHandler = exceptionHandler;
  }

  @StartBundle
  public void startBundle() throws Exception {
    try {
      jvmThreads.acquire();
    } catch (InterruptedException e) {
      throw e;
    }
    acquiredPermit = true;
    LOG.info(String.format("StartBundle: %d", jvmThreads.availablePermits()));
  }

  @FinishBundle
  public void finishBundle() throws Exception {
    jvmThreads.release();
    acquiredPermit = false;
    LOG.info(String.format("FinishBundle: %d", jvmThreads.availablePermits()));
  }

  @Teardown
  public void tearDown() throws Exception {
    if (acquiredPermit) {
      jvmThreads.release();
    }
  }

  @ProcessElement
  public void process(ProcessContext c) throws IOException {
    ReadableFile file = c.element();
    ResourceId resourceId = file.getMetadata().resourceId();
    try {
      FileBasedSource<T> source =
          CompressedSource.from(createSource.apply(resourceId.toString()))
              .withCompression(file.getCompression());
      try (BoundedSource.BoundedReader<T> reader = source.createReader(c.getPipelineOptions())) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(reader.getCurrent());
        }
      } catch (RuntimeException e) {
        if (exceptionHandler.apply(file, null, e)) {
          throw new RuntimeException(
              String.format(
                  "Encountered an error while reading from file %s:", resourceId.getFilename()),
              e);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.warn("Ignoring non-existent file {}", resourceId, e);
    }
  }

  /** A class to handle errors which occur during file reads. */
  public static class ReadFileRangesFnExceptionHandler implements Serializable {

    /*
     * Applies the desired handler logic to the given exception and returns
     * if the exception should be thrown.
     */
    public boolean apply(ReadableFile file, OffsetRange range, Exception e) {
      LOG.error("Avro File Read Failure {} {}", file.getMetadata().resourceId(), e);
      return false;
      // return true;
    }
  }
}
