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
package com.google.cloud.teleport.plugin.model;

import java.util.Map;

/** Image Spec Model. The main payload to communicate parameters to the Dataflow UI. */
public class ImageSpec {

  private String image;
  private ImageSpecMetadata metadata;
  private SdkInfo sdkInfo;
  private Map<String, Object> defaultEnvironment;

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public ImageSpecMetadata getMetadata() {
    return metadata;
  }

  public void setMetadata(ImageSpecMetadata metadata) {
    this.metadata = metadata;
  }

  public SdkInfo getSdkInfo() {
    return sdkInfo;
  }

  public void setSdkInfo(SdkInfo sdkInfo) {
    this.sdkInfo = sdkInfo;
  }

  public Map<String, Object> getDefaultEnvironment() {
    return defaultEnvironment;
  }

  public void setDefaultEnvironment(Map<String, Object> defaultEnvironment) {
    this.defaultEnvironment = defaultEnvironment;
  }

  public void validate() {
    // TODO: what else can be validated here?
    ImageSpecMetadata metadata = getMetadata();
    if (metadata == null) {
      throw new IllegalArgumentException("Image metadata is not specified.");
    }

    if (metadata.getName() == null || metadata.getName().isEmpty()) {
      throw new IllegalArgumentException("Template name can not be empty.");
    }
    if (metadata.getDescription() == null || metadata.getDescription().isEmpty()) {
      throw new IllegalArgumentException("Template description can not be empty.");
    }

    for (ImageSpecParameter parameter : metadata.getParameters()) {
      parameter.validate();
    }
  }
}
