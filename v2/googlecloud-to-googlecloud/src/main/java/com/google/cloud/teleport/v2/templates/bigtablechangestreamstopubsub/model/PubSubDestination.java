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

import java.io.Serializable;
import com.google.pubsub.v1.Topic;


/** Descriptor of PubSub destination. */
public class PubSubDestination implements Serializable {
  private final String pubSubProject;
  private final String pubSubTopicName;
  private Topic pubSubTopic;
  private final String messageFormat;
  private final String messageEncoding;
  private String topicMessageFormat;
  private String topicMessageEncoding;



  public PubSubDestination(
      String pubSubProject, String pubSubTopicName, String messageFormat, String messageEncoding) {
    this.pubSubProject = pubSubProject;
    this.pubSubTopicName = pubSubTopicName;
    this.messageFormat = messageFormat;
    this.topicMessageFormat = messageFormat;
    this.messageEncoding = messageEncoding;
    this.topicMessageEncoding = messageEncoding;
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

  public Topic getPubSubTopic() { return pubSubTopic; }

  public String getTopicMessageFormat() { return topicMessageFormat; }

  public String getTopicMessageEncoding() { return topicMessageEncoding; }
  public void setPubSubTopic(Topic topic) {
    this.pubSubTopic = topic;
  }

  public void setTopicMessageFormat(String topicMessageFormat) {
    this.topicMessageFormat = topicMessageFormat;
  }

  public void setTopicMessageEncoding(String topicMessageEncoding) {
    this.topicMessageEncoding = topicMessageEncoding;
  }
}
