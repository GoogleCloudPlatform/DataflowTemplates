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

import java.util.Map;

/**
 * Class {@link MetricsFetcherConfig} represents the user supplied configuration for launching
 * template and collecting job metrics.
 */
public class TemplateLauncherConfig {
  @Required private String project;
  @Required private String region;

  private String templateName;

  private String templateVersion;

  @Required private String templateType;
  @Required private String templateSpec;

  private String jobPrefix;
  private String jobName;
  private Integer timeoutInMinutes;

  private Map<String, String> pipelineOptions;
  private Map<String, Object> environmentOptions;

  private ResourcePricing pricing;

  public String getProject() {
    return this.project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getRegion() {
    return this.region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getTemplateName() {
    return templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  public String getTemplateVersion() {
    return templateVersion;
  }

  public void setTemplateVersion(String templateVersion) {
    this.templateVersion = templateVersion;
  }

  public String getTemplateType() {
    return templateType;
  }

  public void setTemplateType(String templateType) {
    this.templateType = templateType;
  }

  public String getTemplateSpec() {
    return templateSpec;
  }

  public void setTemplateSpec(String templateSpec) {
    this.templateSpec = templateSpec;
  }

  public String getJobPrefix() {
    return jobPrefix;
  }

  public void setJobPrefix(String jobPrefix) {
    this.jobPrefix = jobPrefix;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public Integer getTimeoutInMinutes() {
    return timeoutInMinutes;
  }

  public void setTimeoutInMinutes(Integer timeoutInMinutes) {
    this.timeoutInMinutes = timeoutInMinutes;
  }

  public Map<String, String> getPipelineOptions() {
    return pipelineOptions;
  }

  public void setPipelineOptions(Map<String, String> pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public Map<String, Object> getEnvironmentOptions() {
    return environmentOptions;
  }

  public void setEnvironmentOptions(Map<String, Object> environmentOptions) {
    this.environmentOptions = environmentOptions;
  }

  public ResourcePricing getResourcePricing() {
    return pricing;
  }

  public void setResourcePricing(ResourcePricing resourcePricing) {
    this.pricing = resourcePricing;
  }
}
