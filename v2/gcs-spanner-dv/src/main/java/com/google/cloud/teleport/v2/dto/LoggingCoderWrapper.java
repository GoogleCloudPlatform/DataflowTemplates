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
package com.google.cloud.teleport.v2.dto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Coder} decorator that logs every `.encode()` and `.decode()` call to provide full
 * visibility into serialization events and runtime element classes.
 */
public class LoggingCoderWrapper<T> extends org.apache.beam.sdk.coders.StructuredCoder<T> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingCoderWrapper.class);
  private final Coder<T> delegate;
  private final String name;

  public LoggingCoderWrapper(Coder<T> delegate, String name) {
    this.delegate = delegate;
    this.name = name;
  }

  public static <T> LoggingCoderWrapper<T> of(Coder<T> delegate, String name) {
    return new LoggingCoderWrapper<>(delegate, name);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    if (value != null) {
      LOG.info(
          "[Coder Logging: {}] (Delegate: {}) ENCODING object of class={}: {}",
          name,
          delegate,
          value.getClass().getName(),
          value);
    } else {
      LOG.info("[Coder Logging: {}] (Delegate: {}) ENCODING null object", name, delegate);
    }
    delegate.encode(value, outStream);
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    T value = delegate.decode(inStream);
    if (value != null) {
      LOG.info(
          "[Coder Logging: {}] (Delegate: {}) DECODING object of class={}: {}",
          name,
          delegate,
          value.getClass().getName(),
          value);
    } else {
      LOG.info("[Coder Logging: {}] (Delegate: {}) DECODING null object", name, delegate);
    }
    return value;
  }

  @Override
  public java.util.List<? extends Coder<?>> getCoderArguments() {
    return java.util.Collections.singletonList(delegate);
  }

  @Override
  public java.util.List<? extends Coder<?>> getComponents() {
    return java.util.Collections.singletonList(delegate);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    delegate.verifyDeterministic();
  }
}
