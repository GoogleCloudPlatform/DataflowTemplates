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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BooleanCoder;

/**
 * Deterministic binary coder for {@link TimestampSortKey}.
 *
 * <p>Encodes sort keys into a compact binary format:
 *
 * <ul>
 *   <li>Presence flag (1 byte boolean)
 *   <li>Epoch seconds (8 bytes via {@link BigEndianLongCoder})
 *   <li>Sub-second ordering / nanoseconds (4 bytes via {@link BigEndianIntegerCoder})
 *   <li>Stream type / isCdc flag (1 byte via {@link BooleanCoder})
 * </ul>
 */
public class TimestampSortKeyCoder extends AtomicCoder<TimestampSortKey> {

  private static final TimestampSortKeyCoder INSTANCE = new TimestampSortKeyCoder();
  private static final BigEndianLongCoder LONG_CODER = BigEndianLongCoder.of();
  private static final BigEndianIntegerCoder INT_CODER = BigEndianIntegerCoder.of();
  private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

  private TimestampSortKeyCoder() {}

  public static TimestampSortKeyCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TimestampSortKey value, OutputStream outStream) throws IOException {
    if (value == null) {
      BOOLEAN_CODER.encode(false, outStream);
      return;
    }
    BOOLEAN_CODER.encode(true, outStream);
    LONG_CODER.encode(value.getSeconds(), outStream);
    INT_CODER.encode((int) value.getSubSeconds(), outStream);
    BOOLEAN_CODER.encode(value.isCdc(), outStream);
  }

  @Override
  public TimestampSortKey decode(InputStream inStream) throws IOException {
    boolean isPresent = BOOLEAN_CODER.decode(inStream);
    if (!isPresent) {
      return null;
    }
    long seconds = LONG_CODER.decode(inStream);
    int subSeconds = INT_CODER.decode(inStream);
    boolean isCdc = BOOLEAN_CODER.decode(inStream);
    return TimestampSortKey.of(seconds, subSeconds, isCdc);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    LONG_CODER.verifyDeterministic();
    INT_CODER.verifyDeterministic();
    BOOLEAN_CODER.verifyDeterministic();
  }
}
