/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.datadog;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.io.IOUtils;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link DatadogEvent} objects. */
public class DatadogEventCoder extends AtomicCoder<DatadogEvent> {

  private static final DatadogEventCoder DATADOG_EVENT_CODER = new DatadogEventCoder();

  private static final TypeDescriptor<DatadogEvent> TYPE_DESCRIPTOR =
      new TypeDescriptor<DatadogEvent>() {};
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> STRING_NULLABLE_CODER =
      NullableCoder.of(STRING_UTF_8_CODER);
  private static final NullableCoder<Long> LONG_NULLABLE_CODER =
      NullableCoder.of(BigEndianLongCoder.of());

  private static final Gson GSON = new Gson();

  // Version markers must be >= 2.
  private static final int VERSION_3 = 3;

  public static DatadogEventCoder of() {
    return DATADOG_EVENT_CODER;
  }

  @Override
  public void encode(DatadogEvent value, OutputStream out) throws CoderException, IOException {
    out.write(VERSION_3);

    LONG_NULLABLE_CODER.encode(value.time(), out);
    STRING_NULLABLE_CODER.encode(value.host(), out);
    STRING_NULLABLE_CODER.encode(value.source(), out);
    STRING_NULLABLE_CODER.encode(value.sourceType(), out);
    STRING_NULLABLE_CODER.encode(value.index(), out);
    String fields = value.fields() == null ? null : value.fields().toString();
    STRING_NULLABLE_CODER.encode(fields, out);
    STRING_UTF_8_CODER.encode(value.event(), out);
  }

  @Override
  public DatadogEvent decode(InputStream in) throws CoderException, IOException {
    DatadogEvent.Builder builder = DatadogEvent.newBuilder();

    int v = in.read();

    // Versions 1 and 2 of this coder had no version marker field, but 1st byte in the serialized
    // data was always 0 or 1 (present/not present indicator for a nullable field).
    // So here we assume if the first byte is >= 2 then it's the version marker.

    if (v >= 2) {
      decodeWithVersion(v, in, builder);
    } else {
      // It's impossible to distinguish between V1 and V2 without re-reading portions of the input
      // stream twice (and without the version marker), so we must have a ByteArrayInputStream copy,
      // which is guaranteed to support mark()/reset().

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      os.write(v);
      IOUtils.copy(in, os);
      ByteArrayInputStream streamCopy = new ByteArrayInputStream(os.toByteArray());

      decodeVersion1or2(streamCopy, builder);
    }

    return builder.build();
  }

  private void decodeWithVersion(int version, InputStream in, DatadogEvent.Builder builder)
      throws IOException {

    decodeCommonFields(in, builder);

    if (version >= VERSION_3) {
      String fields = STRING_NULLABLE_CODER.decode(in);
      if (fields != null) {
        builder.withFields(GSON.fromJson(fields, JsonObject.class));
      }

      String event = STRING_UTF_8_CODER.decode(in);
      builder.withEvent(event);
    }
  }

  private void decodeVersion1or2(ByteArrayInputStream in, DatadogEvent.Builder builder)
      throws IOException {

    decodeCommonFields(in, builder);

    in.mark(Integer.MAX_VALUE);

    // The following fields may be different between V1 and V2.

    // V1 format: <... common fields...> <event length> <event string>
    // V2 format: <... common fields...> <fields present indicator byte 0/1>
    //                <fields length, if present> <fields string> <event length> <event string>

    // We try to read this as V2 first. If any exception, fall back to V1.

    // Note: it's impossible to incorrectly parse V1 data with V2 decoder (potentially causing
    // corrupted fields in the message). If we try that and the 1st byte is:
    //   - 2 or more: decoding fails because V2 expects it to be either 0 or 1 (present indicator).
    //   - 1: this means the "event" string length is 1, so we have only 1 more byte in the stream.
    //        V2 decoding fails with EOF assuming 1 is the "fields" string length and reading
    //        at least 1 more byte.
    //   - 0: this means the "event" string is empty, so we have no more bytes in the stream.
    //        V2 decoding fails with EOF assuming 0 is the "fields" string length and reading
    //        the next "event" field.

    JsonObject fields = null;
    String event;

    try {
      // Assume V2 first.
      String fieldsString = STRING_NULLABLE_CODER.decode(in);
      if (fieldsString != null) {
        fields = GSON.fromJson(fieldsString, JsonObject.class);
      }
      event = STRING_UTF_8_CODER.decode(in);
    } catch (CoderException e) {
      // If failed, reset the stream and parse as V1.
      in.reset();
      event = STRING_UTF_8_CODER.decode(in);
    }

    if (fields != null) {
      builder.withFields(fields);
    }
    builder.withEvent(event);
  }

  private void decodeCommonFields(InputStream in, DatadogEvent.Builder builder) throws IOException {
    Long time = LONG_NULLABLE_CODER.decode(in);
    if (time != null) {
      builder.withTime(time);
    }

    String host = STRING_NULLABLE_CODER.decode(in);
    if (host != null) {
      builder.withHost(host);
    }

    String source = STRING_NULLABLE_CODER.decode(in);
    if (source != null) {
      builder.withSource(source);
    }

    String sourceType = STRING_NULLABLE_CODER.decode(in);
    if (sourceType != null) {
      builder.withSourceType(sourceType);
    }

    String index = STRING_NULLABLE_CODER.decode(in);
    if (index != null) {
      builder.withIndex(index);
    }
  }

  @Override
  public TypeDescriptor<DatadogEvent> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "DatadogEvent can hold arbitrary instances, which may be non-deterministic.");
  }
}
