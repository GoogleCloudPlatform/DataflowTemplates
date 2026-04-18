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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

/**
 * An {@link Appendable} that buffers text into a {@link StringBuilder} while tracking the exact
 * UTF-8 byte length of the output. Once the running UTF-8 byte count would exceed {@code maxBytes},
 * the appendable throws {@link OversizedJsonException} so that in-progress serializers (notably
 * {@code JsonFormat.Printer#appendTo}) abort instead of inflating a huge JSON document in memory.
 *
 * <p>Per-code-point arithmetic is used so the count is exact; we do not use the pessimistic {@code
 * chars * 3} heuristic, which would reject payloads that actually fit.
 *
 * <p>Pass {@code maxBytes &lt;= 0} to disable bounding (the appendable accepts an unlimited amount
 * of text).
 *
 * <p>Package-private: only meant to be used from this package's transformers.
 */
final class Utf8BoundedAppendable implements Appendable {

  private final StringBuilder sb;
  private final long maxBytes;
  private long byteCount;

  Utf8BoundedAppendable(long maxBytes) {
    this.sb = new StringBuilder();
    this.maxBytes = maxBytes;
    this.byteCount = 0L;
  }

  @Override
  public Appendable append(CharSequence csq) {
    if (csq == null) {
      return appendInternal("null", 0, 4);
    }
    return appendInternal(csq, 0, csq.length());
  }

  @Override
  public Appendable append(CharSequence csq, int start, int end) {
    if (csq == null) {
      return appendInternal("null", 0, 4);
    }
    return appendInternal(csq, start, end);
  }

  @Override
  public Appendable append(char c) {
    long delta;
    if (Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
      // An isolated surrogate encodes as the 3-byte U+FFFD replacement when re-encoded.
      delta = 3;
    } else if (c <= 0x7F) {
      delta = 1;
    } else if (c <= 0x7FF) {
      delta = 2;
    } else {
      delta = 3;
    }
    long next = byteCount + delta;
    if (maxBytes > 0 && next > maxBytes) {
      throw new OversizedJsonException(next);
    }
    sb.append(c);
    byteCount = next;
    return this;
  }

  private Appendable appendInternal(CharSequence csq, int start, int end) {
    int i = start;
    while (i < end) {
      char c = csq.charAt(i);
      long delta;
      if (Character.isHighSurrogate(c) && i + 1 < end) {
        char next = csq.charAt(i + 1);
        if (Character.isLowSurrogate(next)) {
          delta = 4;
          long projected = byteCount + delta;
          if (maxBytes > 0 && projected > maxBytes) {
            throw new OversizedJsonException(projected);
          }
          sb.append(c).append(next);
          byteCount = projected;
          i += 2;
          continue;
        }
      }
      if (c <= 0x7F) {
        delta = 1;
      } else if (c <= 0x7FF) {
        delta = 2;
      } else {
        // Includes isolated surrogates, which round-trip as U+FFFD (3 bytes) when re-encoded.
        delta = 3;
      }
      long projected = byteCount + delta;
      if (maxBytes > 0 && projected > maxBytes) {
        throw new OversizedJsonException(projected);
      }
      sb.append(c);
      byteCount = projected;
      i += 1;
    }
    return this;
  }

  String toJson() {
    return sb.toString();
  }

  long byteCount() {
    return byteCount;
  }
}
