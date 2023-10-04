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
import org.joda.time.Duration;

/**
 * Each worker task context shard detail, the source connection profile and depending on the buffer
 * read from, either the PubSub project id or the KafkaConnectionProfile.
 */
public class ProcessingContext implements Serializable {

  private Shard shard;
  private Schema schema;

  private String sourceDbTimezoneOffset;
  private String startTimestamp;
  private Duration windowDuration;
  private String gcsPath;
  private String spannerProjectId;
  private String metadataInstance;
  private String metadataDatabase;
  private String tableSuffix;

  public ProcessingContext(
      Shard shard,
      Schema schema,
      String sourceDbTimezoneOffset,
      String startTimestamp,
      Duration windowDuration,
      String gcsPath,
      String spannerProjectId,
      String metadataInstance,
      String metadataDatabase,
      String tableSuffix) {
    this.shard = shard;
    this.schema = schema;
    this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
    this.startTimestamp = startTimestamp;
    this.windowDuration = windowDuration;
    this.gcsPath = gcsPath;
    this.spannerProjectId = spannerProjectId;
    this.metadataInstance = metadataInstance;
    this.metadataDatabase = metadataDatabase;
    this.tableSuffix = tableSuffix;
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

  public String getSpannerProjectId() {
    return spannerProjectId;
  }

  public String getMetadataInstance() {
    return metadataInstance;
  }

  public String getMetadataDatabase() {
    return metadataDatabase;
  }

  public String getTableSuffix() {
    return tableSuffix;
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
        + " spannerProjectId: "
        + spannerProjectId
        + " metadataInstance: "
        + metadataInstance
        + " metadataDatabase: "
        + metadataDatabase
        + " tableSuffix: "
        + tableSuffix
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
        && this.getWindowDuration().equals(other.getWindowDuration())
        && this.getSpannerProjectId().equals(other.getSpannerProjectId())
        && this.getMetadataInstance().equals(other.getMetadataInstance())
        && this.getMetadataDatabase().equals(other.getMetadataDatabase())
        && this.getTableSuffix().equals(other.getTableSuffix());
  }
}
