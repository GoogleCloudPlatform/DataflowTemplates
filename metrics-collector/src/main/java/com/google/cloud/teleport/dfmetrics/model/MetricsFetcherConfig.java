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
package com.google.cloud.teleport.dfmetrics.model;

/**
 * Class {@link MetricsFetcherConfig} represents the user supplied configuration for collecting job
 * metrics.
 */
public class MetricsFetcherConfig {
  @Required private String project;

  @Required private String region;

  @Required private String jobId;

  private ResourcePricing pricing;

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public ResourcePricing getResourcePricing() {
    return pricing;
  }

  public void setResourcePricing(ResourcePricing resourcePricing) {
    this.pricing = resourcePricing;
  }
}
