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

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;

/**
 * Composite monotonic sort key for MongoDB change events and backfill reads.
 *
 * <p>Disentangles cross-domain timestamp comparisons by ordering:
 *
 * <ol>
 *   <li>Epoch seconds (MongoDB oplog/clusterTime timestamp seconds vs backfill start seconds).
 *   <li>Stream type precedence: Live CDC mutations (INSERT, UPDATE, REPLACE, DELETE) strictly
 *       supersede Backfill snapshot reads within the exact same second.
 *   <li>Sub-second ordering within the same stream type:
 *       <ul>
 *         <li>For CDC: MongoDB oplog increment counter (BsonTimestamp ordinal increment).
 *         <li>For Backfill: Sub-second / monotonic extraction order.
 *       </ul>
 * </ol>
 */
@DefaultCoder(TimestampSortKeyCoder.class)
public class TimestampSortKey implements Serializable, Comparable<TimestampSortKey> {
  private static final long serialVersionUID = 1L;

  private final long seconds;
  private final long subSeconds;
  private final boolean isCdc;

  public TimestampSortKey(long seconds, long subSeconds, boolean isCdc) {
    this.seconds = seconds;
    this.subSeconds = subSeconds;
    this.isCdc = isCdc;
  }

  public static TimestampSortKey of(long seconds, long subSeconds, boolean isCdc) {
    return new TimestampSortKey(seconds, subSeconds, isCdc);
  }

  public static TimestampSortKey cdc(long seconds, long inc) {
    return new TimestampSortKey(seconds, inc, true);
  }

  public static TimestampSortKey backfill(long seconds) {
    return new TimestampSortKey(seconds, 0L, false);
  }

  public static TimestampSortKey backfill(long seconds, long subSeconds) {
    return new TimestampSortKey(seconds, subSeconds, false);
  }

  public long getSeconds() {
    return seconds;
  }

  public long getTimestampSeconds() {
    return seconds;
  }

  public long getSubSeconds() {
    return subSeconds;
  }

  public long getTimestampSubSeconds() {
    return subSeconds;
  }

  public boolean isCdc() {
    return isCdc;
  }

  public boolean getIsCdc() {
    return isCdc;
  }

  @Override
  public int compareTo(TimestampSortKey other) {
    if (other == null) {
      throw new NullPointerException("Cannot compare TimestampSortKey with null");
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
    // 3. Sub-second ordering within the same stream type
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
