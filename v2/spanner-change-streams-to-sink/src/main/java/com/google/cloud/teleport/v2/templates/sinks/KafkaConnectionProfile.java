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
package com.google.cloud.teleport.v2.templates.sinks;

import java.io.Serializable;
import java.util.Objects;

/** Class to store Kafka connection metadata. */
public class KafkaConnectionProfile implements Serializable {

  private String bootstrapServer;
  private String dataTopic;
  private String errorTopic;

  public KafkaConnectionProfile(String bootstrapServer, String dataTopic, String errorTopic) {
    this.bootstrapServer = bootstrapServer;
    this.dataTopic = dataTopic;
    this.errorTopic = errorTopic;
  }

  public String getBootstrapServer() {
    return bootstrapServer;
  }

  public String getDataTopic() {
    return dataTopic;
  }

  public String getErrorTopic() {
    return errorTopic;
  }

  @Override
  public String toString() {
    return " { dataTopic: "
        + dataTopic
        + " , errorTopic: "
        + errorTopic
        + " , bootstrapServer: "
        + bootstrapServer
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaConnectionProfile)) {
      return false;
    }
    KafkaConnectionProfile that = (KafkaConnectionProfile) o;
    return Objects.equals(dataTopic, that.dataTopic)
        && Objects.equals(errorTopic, that.errorTopic)
        && Objects.equals(bootstrapServer, that.bootstrapServer);
  }
}
