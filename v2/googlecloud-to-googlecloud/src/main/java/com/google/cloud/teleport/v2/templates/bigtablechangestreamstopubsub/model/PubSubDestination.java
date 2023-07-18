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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstopubsub.model;

import com.google.pubsub.v1.Topic;
import java.io.Serializable;

/** Descriptor of PubSub destination. */
public class PubSubDestination implements Serializable {
  private final String pubSubProject;
  private final String pubSubTopicName;
  private Topic pubSubTopic;
  private final String messageFormat;
  private final String messageEncoding;

  private final boolean useBase64Rowkey;
  private final boolean useBase64ColumnQualifier;
  private final boolean useBase64Value;

  public PubSubDestination(
      String pubSubProject,
      String pubSubTopicName,
      String messageFormat,
      String messageEncoding,
      Boolean useBase64Rowkey,
      Boolean useBase64ColumnQualifier,
      Boolean useBase64Value) {
    this.pubSubProject = pubSubProject;
    this.pubSubTopicName = pubSubTopicName;
    this.messageFormat = messageFormat;
    this.messageEncoding = messageEncoding;
    this.useBase64Rowkey = useBase64Rowkey;
    this.useBase64ColumnQualifier = useBase64ColumnQualifier;
    this.useBase64Value = useBase64Value;
  }

  public String getPubSubProject() {
    return pubSubProject;
  }

  public String getPubSubTopicName() {
    return pubSubTopicName;
  }

  public String getMessageFormat() {
    return messageFormat;
  }

  public String getMessageEncoding() {
    return messageEncoding;
  }

  public Topic getPubSubTopic() {
    return pubSubTopic;
  }

  public void setPubSubTopic(Topic topic) {
    this.pubSubTopic = topic;
  }

  public Boolean getUseBase64Rowkey() {
    return useBase64Rowkey;
  }

  public Boolean getUseBase64ColumnQualifier() {
    return useBase64ColumnQualifier;
  }

  public Boolean getUseBase64Value() {
    return useBase64Value;
  }
}
