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

/**
 * A class for capturing errors when sending {@link NewRelicLogRecord}s to New Relic's Logs API end
 * point.
 */
public class NewRelicLogApiSendError {

  /**
   * A JSON representation of the log record that was sent (but failed to be accepted) to the Logs
   * API.
   */
  private final String payload;
  /** Status message returned by the Logs API, or message returned by any thrown exception. */
  private final String statusMessage;
  /**
   * Status code returned by the Logs API. If the error is caused by an exception, this field is
   * empty.
   */
  private final Integer statusCode;

  public NewRelicLogApiSendError(
      final String payload, final String statusMessage, final Integer statusCode) {
    this.payload = payload;
    this.statusMessage = statusMessage;
    this.statusCode = statusCode;
  }

  public String getPayload() {
    return payload;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public Integer getStatusCode() {
    return statusCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NewRelicLogApiSendError that = (NewRelicLogApiSendError) o;

    return new EqualsBuilder()
        .append(payload, that.payload)
        .append(statusMessage, that.statusMessage)
        .append(statusCode, that.statusCode)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(payload)
        .append(statusMessage)
        .append(statusCode)
        .toHashCode();
  }
}
