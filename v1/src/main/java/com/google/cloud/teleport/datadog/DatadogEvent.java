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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import javax.annotation.Nullable;

/** A class for Datadog events. */
@AutoValue
public abstract class DatadogEvent {

  public static Builder newBuilder() {
    return new AutoValue_DatadogEvent.Builder();
  }

  @Nullable
  public abstract Long time();

  @Nullable
  public abstract String host();

  @Nullable
  public abstract String source();

  @Nullable
  @SerializedName("sourcetype")
  public abstract String sourceType();

  @Nullable
  public abstract String index();

  @Nullable
  public abstract JsonObject fields();

  @Nullable
  public abstract String event();

  /** A builder class for creating {@link DatadogEvent} objects. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setTime(Long time);

    abstract Builder setHost(String host);

    abstract Builder setSource(String source);

    abstract Builder setSourceType(String sourceType);

    abstract Builder setIndex(String index);

    abstract Builder setEvent(String event);

    abstract Builder setFields(JsonObject fields);

    abstract String event();

    abstract DatadogEvent autoBuild();

    public Builder withTime(Long time) {
      checkNotNull(time, "withTime(time) called with null input.");

      return setTime(time);
    }

    public Builder withHost(String host) {
      checkNotNull(host, "withHost(host) called with null input.");

      return setHost(host);
    }

    public Builder withSource(String source) {
      checkNotNull(source, "withSource(source) called with null input.");

      return setSource(source);
    }

    public Builder withSourceType(String sourceType) {
      checkNotNull(sourceType, "withSourceType(sourceType) called with null input.");

      return setSourceType(sourceType);
    }

    public Builder withIndex(String index) {
      checkNotNull(index, "withIndex(index) called with null input.");

      return setIndex(index);
    }

    public Builder withFields(JsonObject fields) {
      checkNotNull(fields, "withFields(fields) called with null input.");

      return setFields(fields);
    }

    public Builder withEvent(String event) {
      checkNotNull(event, "withEvent(event) called with null input.");

      return setEvent(event);
    }

    public DatadogEvent build() {
      checkNotNull(event(), "Event information is required.");
      return autoBuild();
    }
  }
}
