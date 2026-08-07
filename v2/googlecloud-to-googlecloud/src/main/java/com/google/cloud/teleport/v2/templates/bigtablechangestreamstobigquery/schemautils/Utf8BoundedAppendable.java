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

import com.google.common.base.Utf8;

/**
 * An {@link Appendable} that buffers text into a {@link StringBuilder} while tracking the exact
 * UTF-8 byte length of the output. Once the running UTF-8 byte count would exceed {@code maxBytes},
 * the appendable throws {@link OversizedJsonException} so that in-progress serializers (notably
 * {@code JsonFormat.Printer#appendTo}) abort instead of inflating a huge JSON document in memory.
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
    int cap = (int) Math.max(64, Math.min(maxBytes > 0 ? maxBytes : 1024L, 1L << 20));
    this.sb = new StringBuilder(cap);
    this.maxBytes = maxBytes;
    this.byteCount = 0L;
  }

  @Override
  public Appendable append(CharSequence csq) {
    CharSequence s = csq == null ? "null" : csq;
    return appendSegment(s, 0, s.length());
  }

  @Override
  public Appendable append(CharSequence csq, int start, int end) {
    CharSequence s = csq == null ? "null" : csq;
    return appendSegment(s, start, end);
  }

  @Override
  public Appendable append(char c) {
    return appendSegment(String.valueOf(c), 0, 1);
  }

  private Appendable appendSegment(CharSequence csq, int start, int end) {
    if (start >= end) {
      return this;
    }
    CharSequence segment = start == 0 && end == csq.length() ? csq : csq.subSequence(start, end);
    long delta = Utf8.encodedLength(segment);
    long projected = byteCount + delta;
    if (maxBytes > 0 && projected > maxBytes) {
      throw new OversizedJsonException(projected);
    }
    sb.append(segment);
    byteCount = projected;
    return this;
  }

  String toJson() {
    return sb.toString();
  }

  long byteCount() {
    return byteCount;
  }
}
