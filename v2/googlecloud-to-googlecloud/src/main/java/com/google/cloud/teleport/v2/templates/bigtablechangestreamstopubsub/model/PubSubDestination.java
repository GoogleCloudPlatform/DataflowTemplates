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
  private final MessageFormat messageFormat;
  private final MessageEncoding messageEncoding;

  private final boolean useBase64Rowkey;
  private final boolean useBase64ColumnQualifiers;
  private final boolean useBase64Values;

  private final boolean stripValues;

  public PubSubDestination(
      String pubSubProject,
      String pubSubTopicName,
      Topic pubSubTopic,
      MessageFormat messageFormat,
      MessageEncoding messageEncoding,
      boolean useBase64Rowkey,
      boolean useBase64ColumnQualifiers,
      boolean useBase64Values,
      boolean stripValues) {
    this.pubSubProject = pubSubProject;
    this.pubSubTopicName = pubSubTopicName;
    this.pubSubTopic = pubSubTopic;
    this.messageFormat = messageFormat;
    this.messageEncoding = messageEncoding;
    this.useBase64Rowkey = useBase64Rowkey;
    this.useBase64ColumnQualifiers = useBase64ColumnQualifiers;
    this.useBase64Values = useBase64Values;
    this.stripValues = stripValues;
  }

  public String getPubSubProject() {
    return pubSubProject;
  }

  public String getPubSubTopicName() {
    return pubSubTopicName;
  }

  public MessageFormat getMessageFormat() {
    return messageFormat;
  }

  public MessageEncoding getMessageEncoding() {
    return messageEncoding;
  }

  public Topic getPubSubTopic() {
    return pubSubTopic;
  }

  public void setPubSubTopic(Topic topic) {
    this.pubSubTopic = topic;
  }

  public boolean getUseBase64Rowkey() {
    return useBase64Rowkey;
  }

  public boolean getUseBase64ColumnQualifiers() {
    return useBase64ColumnQualifiers;
  }

  public boolean getUseBase64Values() {
    return useBase64Values;
  }

  public boolean getStripValues() {
    return stripValues;
  }
}
