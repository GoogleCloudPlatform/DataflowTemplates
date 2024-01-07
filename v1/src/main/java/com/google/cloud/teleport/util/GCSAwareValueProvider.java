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
package com.google.cloud.teleport.util;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * ValueProvider for a String, that is aware when the string contains a URL (starts with gs://) to
 * resolve for the contents of that file.
 */
public class GCSAwareValueProvider implements ValueProvider<String>, Serializable {

  private transient volatile String cachedValue;

  private final ValueProvider<String> originalProvider;

  public GCSAwareValueProvider(ValueProvider<String> provider) {
    this.originalProvider = provider;
  }

  @Override
  public synchronized String get() {
    if (cachedValue != null) {
      return cachedValue;
    }

    cachedValue = resolve();
    return cachedValue;
  }

  @Override
  public boolean isAccessible() {
    return originalProvider.isAccessible();
  }

  protected String resolve() {

    String originalValue = this.originalProvider.get();

    if (originalValue != null && originalValue.startsWith("gs://")) {
      try {
        return new String(GCSUtils.getGcsFileAsBytes(originalValue), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw new RuntimeException(
            "Error resolving ValueProvider from Cloud Storage path " + originalValue, e);
      }
    }

    return originalValue;
  }
}
