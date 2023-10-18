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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.io.Serializable;
import java.util.Objects;
import org.joda.time.Duration;

/** Each worker task context. */
public class ProcessingContext implements Serializable {

  private Shard shard;
  private Schema schema;

  private String sourceDbTimezoneOffset;
  private String startTimestamp;
  private Duration windowDuration;
  private String gcsPath;

  public ProcessingContext(
      Shard shard,
      Schema schema,
      String sourceDbTimezoneOffset,
      String startTimestamp,
      Duration windowDuration,
      String gcsPath) {
    this.shard = shard;
    this.schema = schema;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.startTimestamp = startTimestamp;
    this.windowDuration = windowDuration;
    this.gcsPath = gcsPath;
  }

  public Shard getShard() {
    return shard;
  }

  public Schema getSchema() {
    return schema;
  }

  public String getSourceDbTimezoneOffset() {
    return sourceDbTimezoneOffset;
  }

  public String getStartTimestamp() {
    return startTimestamp;
  }

  public void setStartTimestamp(String startTimestamp) {
    this.startTimestamp = startTimestamp;
  }

  public String getGCSPath() {
    return gcsPath;
  }

  public Duration getWindowDuration() {
    return windowDuration;
  }

  @Override
  public String toString() {

    return "{ Shard details :"
        + shard.toString()
        + " sourceDbTimezoneOffset: "
        + sourceDbTimezoneOffset
        + " startTimestamp: "
        + startTimestamp
        + " windowDuration: "
        + windowDuration
        + " gcsPath: "
        + gcsPath
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ProcessingContext)) {
      return false;
    }
    final ProcessingContext other = (ProcessingContext) o;
    return this.getShard().equals(other.getShard())
        && this.getSchema().equals(other.getSchema())
        && this.getSourceDbTimezoneOffset().equals(other.getSourceDbTimezoneOffset())
        && this.getStartTimestamp().equals(other.getStartTimestamp())
        && this.getGCSPath().equals(other.getGCSPath())
        && this.getWindowDuration().equals(other.getWindowDuration());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getShard(),
        getSchema(),
        getSourceDbTimezoneOffset(),
        getStartTimestamp(),
        getGCSPath(),
        getWindowDuration());
  }
}
