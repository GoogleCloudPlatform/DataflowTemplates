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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Image Spec Metadata, which needs to be generated to expose UI parameters. */
public class ImageSpecMetadata {

  private String name;
  private String description;
  private String additionalHelp;
  private String mainClass;
  private List<ImageSpecParameter> parameters = new ArrayList<>();
  private Map<String, String> runtimeParameters = new HashMap<>();
  private ImageSpecCategory category;
  private String internalName;
  private String module;
  private String documentationLink;
  private boolean googleReleased;
  private boolean udfSupport;
  private boolean flexTemplate;
  private String sourceFilePath;

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getAdditionalHelp() {
    return additionalHelp;
  }

  public void setAdditionalHelp(String additionalHelp) {
    this.additionalHelp = additionalHelp;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getMainClass() {
    return mainClass;
  }

  public void setMainClass(String mainClass) {
    this.mainClass = mainClass;
  }

  public List<ImageSpecParameter> getParameters() {
    return parameters;
  }

  public void setParameters(List<ImageSpecParameter> parameters) {
    this.parameters = parameters;
  }

  public Map<String, String> getRuntimeParameters() {
    return runtimeParameters;
  }

  public void setRuntimeParameters(Map<String, String> runtimeParameters) {
    this.runtimeParameters = runtimeParameters;
  }

  public String getInternalName() {
    return internalName;
  }

  public void setInternalName(String internalName) {
    this.internalName = internalName;
  }

  public ImageSpecCategory getCategory() {
    return category;
  }

  public void setCategory(ImageSpecCategory category) {
    this.category = category;
  }

  public String getDocumentationLink() {
    return documentationLink;
  }

  public void setDocumentationLink(String documentationLink) {
    this.documentationLink = documentationLink;
  }

  public String getModule() {
    return module;
  }

  public void setModule(String module) {
    this.module = module;
  }

  public boolean isGoogleReleased() {
    return googleReleased;
  }

  public void setGoogleReleased(boolean googleReleased) {
    this.googleReleased = googleReleased;
  }

  public boolean isUdfSupport() {
    return udfSupport;
  }

  public void setUdfSupport(boolean udfSupport) {
    this.udfSupport = udfSupport;
  }

  public boolean isFlexTemplate() {
    return flexTemplate;
  }

  public void setFlexTemplate(boolean flexTemplate) {
    this.flexTemplate = flexTemplate;
  }

  public String getSourceFilePath() {
    return sourceFilePath;
  }

  public void setSourceFilePath(String sourceFilePath) {
    this.sourceFilePath = sourceFilePath;
  }

  public Optional<ImageSpecParameter> getParameter(String name) {
    return parameters.stream().filter(parameter -> parameter.getName().equals(name)).findFirst();
  }
}
