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
package com.google.cloud.teleport.v2.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.google.gson.Gson;

/**
 * A simple logging Appender that outputs logs in Json format. Inspired by
 * com.google.cloud.logging.logback.LoggingAppender.
 */
public class JsonAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
  private static final Gson gson = new Gson();

  @Override
  protected void append(ILoggingEvent event) {
    StringBuilder message = new StringBuilder(128);
    message.append(event.getLoggerName()).append(" - ");
    message.append(event.getFormattedMessage());
    if (event.getThrowableProxy() != null) {
      message
          .append(CoreConstants.LINE_SEPARATOR)
          .append(ThrowableProxyUtil.asString(event.getThrowableProxy()));
    }
    System.err.println(gson.toJson(new JsonEntry(message, event.getLevel().toString())));
  }

  private static final class JsonEntry {
    final CharSequence message;
    final CharSequence severity;

    public JsonEntry(CharSequence message, CharSequence severity) {
      this.message = message;
      this.severity = severity;
    }
  }
}
