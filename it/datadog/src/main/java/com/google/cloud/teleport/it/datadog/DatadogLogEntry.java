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
package com.google.cloud.teleport.it.datadog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/** A class for Datadog log entries, copy of DatadogEvent. */
@AutoValue
public abstract class DatadogLogEntry {

  public static Builder newBuilder() {
    return new AutoValue_DatadogLogEntry.Builder();
  }

  @Nullable
  public abstract String ddsource();

  @Nullable
  public abstract String ddtags();

  @Nullable
  public abstract String hostname();

  @Nullable
  public abstract String service();

  @Nullable
  public abstract String message();

  /** A builder class for creating {@link DatadogLogEntry} objects. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setDdsource(String source);

    abstract Builder setDdtags(String tags);

    abstract Builder setHostname(String hostname);

    abstract Builder setService(String service);

    abstract Builder setMessage(String message);

    abstract String message();

    abstract DatadogLogEntry autoBuild();

    public Builder withSource(String source) {
      checkNotNull(source, "withSource(source) called with null input.");

      return setDdsource(source);
    }

    public Builder withTags(String tags) {
      checkNotNull(tags, "withTags(tags) called with null input.");

      return setDdtags(tags);
    }

    public Builder withHostname(String hostname) {
      checkNotNull(hostname, "withHostname(hostname) called with null input.");

      return setHostname(hostname);
    }

    public Builder withService(String service) {
      checkNotNull(service, "withService(service) called with null input.");

      return setService(service);
    }

    public Builder withMessage(String message) {
      checkNotNull(message, "withMessage(message) called with null input.");

      return setMessage(message);
    }

    public DatadogLogEntry build() {
      checkNotNull(message(), "Message is required.");

      return autoBuild();
    }
  }
}
