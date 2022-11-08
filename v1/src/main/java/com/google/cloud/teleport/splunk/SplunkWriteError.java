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
package com.google.cloud.teleport.splunk;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * A class for capturing errors writing {@link SplunkEvent}s to Splunk's Http Event Collector (HEC)
 * end point.
 */
@AutoValue
public abstract class SplunkWriteError {

  public static Builder newBuilder() {
    return new AutoValue_SplunkWriteError.Builder();
  }

  @Nullable
  public abstract Integer statusCode();

  @Nullable
  public abstract String statusMessage();

  @Nullable
  public abstract String payload();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setStatusCode(Integer statusCode);

    abstract Integer statusCode();

    abstract Builder setStatusMessage(String statusMessage);

    abstract Builder setPayload(String payload);

    abstract SplunkWriteError autoBuild();

    public Builder withStatusCode(Integer statusCode) {
      checkNotNull(statusCode, "withStatusCode(statusCode) called with null input.");

      return setStatusCode(statusCode);
    }

    public Builder withStatusMessage(String statusMessage) {
      checkNotNull(statusMessage, "withStatusMessage(statusMessage) called with null input.");

      return setStatusMessage(statusMessage);
    }

    public Builder withPayload(String payload) {
      checkNotNull(payload, "withPayload(payload) called with null input.");

      return setPayload(payload);
    }

    public SplunkWriteError build() {
      return autoBuild();
    }
  }
}
