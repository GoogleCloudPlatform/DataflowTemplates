/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic.dtos;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/** A class representing a New Relic Log record. */
public class NewRelicLogRecord {

  /**
   * Message, can either be a plain text string or a string representing a JSON object. Mandatory.
   */
  private final String message;
  /** Timestamp of the log record. Optional. */
  private final Long timestamp;

  public NewRelicLogRecord(final String message, final Long timestamp) {
    this.message = message;
    this.timestamp = timestamp;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NewRelicLogRecord that = (NewRelicLogRecord) o;

    return new EqualsBuilder()
        .append(message, that.message)
        .append(timestamp, that.timestamp)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(message).append(timestamp).toHashCode();
  }
}
