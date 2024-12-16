/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.auth.dbauth;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

/**
 * Implements the {@link DbAuth} interface for credentails provided to the dataflow job locally,
 * like via input parameters as against through a secretManager url.
 */
@AutoValue
public abstract class LocalCredentialsProvider implements DbAuth {
  abstract String userName();

  abstract GuardedStringValueProvider password();

  @Override
  public ValueProvider<String> getUserName() {
    return StaticValueProvider.of(this.userName());
  }

  @Override
  public ValueProvider<String> getPassword() {
    return password();
  }

  public static Builder builder() {
    return new AutoValue_LocalCredentialsProvider.Builder();
  }

  @Override
  public String toString() {
    return "LocalCredentialsProvider{}";
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setUserName(String value);

    abstract Builder setPassword(GuardedStringValueProvider value);

    public Builder setPassword(String password) {
      return this.setPassword(GuardedStringValueProvider.create(password));
    }

    public abstract LocalCredentialsProvider build();
  }
}
