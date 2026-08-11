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
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Composite monotonic sort key for Datastream MongoDB change events.
 *
 * <p>Disentangles cross-domain timestamp comparisons by ordering:
 *
 * <ol>
 *   <li>Epoch seconds (MongoDB oplog timestamp seconds / backfill extraction seconds).
 *   <li>Stream type precedence: Live CDC mutations (INSERT, UPDATE, DELETE) strictly supersede
 *       Backfill snapshot READ events within the same second.
 *   <li>Sub-second ordering within the same stream type:
 *       <ul>
 *         <li>For CDC: MongoDB oplog increment counter.
 *         <li>For Backfill: Snapshot extraction wall-clock nanoseconds.
 *       </ul>
 * </ol>
 */
@DefaultCoder(SerializableCoder.class)
public class TimestampSortKey implements Serializable, Comparable<TimestampSortKey> {

  private final long seconds;
  private final long subSeconds;
  private final boolean isCdc;

  public TimestampSortKey(long seconds, long subSeconds, boolean isCdc) {
    this.seconds = seconds;
    this.subSeconds = subSeconds;
    this.isCdc = isCdc;
  }

  public static TimestampSortKey of(MongoDbChangeEventContext event) {
    if (event == null) {
      return null;
    }
    return new TimestampSortKey(
        event.getTimestampSeconds(), event.getTimestampSubSeconds(), event.isCdcEvent());
  }

  public long getSeconds() {
    return seconds;
  }

  public long getSubSeconds() {
    return subSeconds;
  }

  public boolean isCdc() {
    return isCdc;
  }

  @Override
  public int compareTo(TimestampSortKey other) {
    if (other == null) {
      return 1;
    }
    // 1. Primary: Compare epoch seconds
    if (this.seconds != other.seconds) {
      return Long.compare(this.seconds, other.seconds);
    }
    // 2. Stream type precedence: Live CDC strictly supersedes Backfill snapshot within the same
    // second
    if (this.isCdc && !other.isCdc) {
      return 1;
    }
    if (!this.isCdc && other.isCdc) {
      return -1;
    }
    // 3. Sub-second ordering within the same stream type (nanos for backfill, oplog inc for CDC)
    return Long.compare(this.subSeconds, other.subSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampSortKey)) {
      return false;
    }
    TimestampSortKey that = (TimestampSortKey) o;
    return seconds == that.seconds && subSeconds == that.subSeconds && isCdc == that.isCdc;
  }

  @Override
  public int hashCode() {
    return Objects.hash(seconds, subSeconds, isCdc);
  }

  @Override
  public String toString() {
    return seconds + ":" + subSeconds + ":" + (isCdc ? "cdc" : "backfill");
  }
}
