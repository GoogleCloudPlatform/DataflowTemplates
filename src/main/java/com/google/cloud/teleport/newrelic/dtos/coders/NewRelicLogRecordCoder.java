/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.newrelic.dtos.coders;

import com.google.cloud.teleport.newrelic.dtos.NewRelicLogRecord;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for {@link NewRelicLogRecord} objects. It allows serializing and
 * deserializing {@link NewRelicLogRecord} objects, which can then be transmitted through a Beam pipeline using
 * PCollection/PCollectionTuple objects.
 */
public class NewRelicLogRecordCoder extends AtomicCoder<NewRelicLogRecord> {

  private static final NewRelicLogRecordCoder SINGLETON = new NewRelicLogRecordCoder();
  private static final TypeDescriptor<NewRelicLogRecord> TYPE_DESCRIPTOR = new TypeDescriptor<NewRelicLogRecord>() {
  };
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<Long> LONG_NULLABLE_CODER = NullableCoder.of(BigEndianLongCoder.of());

  public static NewRelicLogRecordCoder getInstance() {
    return SINGLETON;
  }

  @Override
  public void encode(final NewRelicLogRecord newRelicLogRecord, final OutputStream out) throws IOException {
    LONG_NULLABLE_CODER.encode(newRelicLogRecord.getTimestamp(), out);
    STRING_UTF_8_CODER.encode(newRelicLogRecord.getMessage(), out);
  }

  @Override
  public NewRelicLogRecord decode(final InputStream in) throws IOException {
    final Long timestamp = LONG_NULLABLE_CODER.decode(in);
    final String message = STRING_UTF_8_CODER.decode(in);

    return new NewRelicLogRecord(message, timestamp);
  }

  @Override
  public TypeDescriptor<NewRelicLogRecord> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "NewRelicLogRecord can hold arbitrary instances, which may be non-deterministic.");
  }
}
