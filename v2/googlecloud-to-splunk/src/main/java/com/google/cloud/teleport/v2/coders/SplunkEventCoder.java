/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.coders;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link org.apache.beam.sdk.coders.Coder} for {@link SplunkEvent} objects. */
public class SplunkEventCoder extends AtomicCoder<SplunkEvent> {

  private static final SplunkEventCoder SPLUNK_EVENT_CODER = new SplunkEventCoder();

  private static final TypeDescriptor<SplunkEvent> TYPE_DESCRIPTOR =
      new TypeDescriptor<SplunkEvent>() {};
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();
  private static final NullableCoder<String> STRING_NULLABLE_CODER =
      NullableCoder.of(STRING_UTF_8_CODER);
  private static final NullableCoder<Long> LONG_NULLABLE_CODER =
      NullableCoder.of(BigEndianLongCoder.of());

  private static final Gson GSON = new Gson();

  private static final int VERSION = 1;

  public static SplunkEventCoder of() {
    return SPLUNK_EVENT_CODER;
  }

  @Override
  public void encode(SplunkEvent value, OutputStream out) throws IOException {
    out.write(VERSION);

    LONG_NULLABLE_CODER.encode(value.time(), out);
    STRING_NULLABLE_CODER.encode(value.host(), out);
    STRING_NULLABLE_CODER.encode(value.source(), out);
    STRING_NULLABLE_CODER.encode(value.sourceType(), out);
    STRING_NULLABLE_CODER.encode(value.index(), out);
    STRING_UTF_8_CODER.encode(value.event(), out);
  }

  @Override
  public SplunkEvent decode(InputStream in) throws IOException {
    SplunkEvent.Builder builder = SplunkEvent.newBuilder();

    int v = in.read();

    decodeCommonFields(in, builder);

    return builder.create();
  }

  private void decodeCommonFields(InputStream in, SplunkEvent.Builder builder) throws IOException {
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

    String event = STRING_UTF_8_CODER.decode(in);
    if (event != null) {
      builder.withEvent(event);
    }
  }

  @Override
  public TypeDescriptor<SplunkEvent> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
