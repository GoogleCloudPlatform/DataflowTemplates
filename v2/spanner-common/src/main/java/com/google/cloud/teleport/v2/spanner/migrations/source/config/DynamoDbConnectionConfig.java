/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.source.config;

import java.io.Serializable;

/** Represents the connection configuration for an Amazon DynamoDB source. */
public class DynamoDbConnectionConfig implements SourceConnectionConfig, Serializable {

  private String endpoint = "";
  private String region = "";
  private String accessKey = "";
  private String secretKey = "";
  private String secretManagerUri = "";
  private Integer totalSegments;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getSecretManagerUri() {
    return secretManagerUri;
  }

  public void setSecretManagerUri(String secretManagerUri) {
    this.secretManagerUri = secretManagerUri;
  }

  public Integer getTotalSegments() {
    return totalSegments;
  }

  public void setTotalSegments(Integer totalSegments) {
    this.totalSegments = totalSegments;
  }
}
