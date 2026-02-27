/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link DoFn} that re-timestamps elements to the current system time (Processing Time).
 *
 * <p><b>Problem Statement:</b>
 *
 * <p>In the multi-table read pipeline, certain upstream transforms (like {@link Reparallelize} or
 * those within {@link ReadWithUniformPartitions}) consume side-inputs generated in the {@link
 * org.apache.beam.sdk.transforms.windowing.GlobalWindow}. In Apache Beam, side-inputs in the Global
 * Window are only considered "ready" at the end of the window ({@code TIMESTAMP_MAX_VALUE}).
 * Consequently, any element produced by a transform consuming such a side-input inherits this "End
 * of Time" timestamp (approx. year 294247).
 *
 * <p>This causes downstream failures, specifically in the Dead Letter Queue (DLQ) path. The DLQ
 * writer attempts to assign a "current" timestamp ({@link Instant#now()}) to failed records. Since
 * Beam prohibits moving a timestamp backwards (from the year 294247 to the present), it throws an
 * {@link IllegalArgumentException}.
 *
 * <p><b>The Fix:</b>
 *
 * <p>This {@link DoFn} explicitly resets the timestamp of each element to {@link Instant#now()}. To
 * bypass Beam's watermark constraints, it overrides {@link #getAllowedTimestampSkew()}.
 *
 * <p><b>Rationale for getAllowedTimestampSkew:</b>
 *
 * <p>While {@link #getAllowedTimestampSkew()} is deprecated and should generally be avoided in
 * streaming pipelines (as it can delay watermarks and cause data loss), it is considered safe and
 * effective for this **batch** template. In batch processing, the watermark is less sensitive to
 * backward movements within a single stage, and since this reset happens at the very end of the
 * extraction phase, it ensures that all subsequent metadata (specifically for DLQ and metrics) uses
 * realistic, present-day timestamps.
 *
 * @param <T> The type of the element being re-timestamped.
 */
public class TimeStampRow<T> extends DoFn<T, T> {

  /**
   * Overrides the allowed skew to permit moving timestamps from the "End of Time" (inherited from
   * side-inputs) back to the current wall-clock time.
   *
   * @return A duration representing infinite allowed skew.
   */
  @Override
  public Duration getAllowedTimestampSkew() {
    return Duration.millis(Long.MAX_VALUE);
  }

  /**
   * Re-timestamps the input element to the current system time.
   *
   * @param c The process context.
   */
  @ProcessElement
  public void process(ProcessContext c) {
    // Output the element with the current system time (Processing Time)
    c.outputWithTimestamp(c.element(), Instant.now());
  }
}
