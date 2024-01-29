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

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.util.MetadataUtils;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Parameters in a template. */
public class ImageSpecParameter {

  private String name;
  private String groupName;
  private String label;
  private String helpText;
  private Boolean isOptional;
  private Boolean hiddenUi;
  private List<String> regexes;
  private List<ImageSpecParameterEnumOption> enumOptions;
  private ImageSpecParameterType paramType;
  private String defaultValue;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getHelpText() {
    return helpText;
  }

  public void setHelpText(String helpText) {
    this.helpText = helpText;
  }

  public Boolean isOptional() {
    return isOptional;
  }

  public Boolean hiddenUi() {
    return hiddenUi;
  }

  public void setOptional(Boolean optional) {
    if (optional == null || !optional) {
      isOptional = null;
    } else {
      isOptional = true;
    }
  }

  public void setHiddenUi(Boolean hiddenUi) {
    if (hiddenUi == null || !hiddenUi) {
      hiddenUi = null;
    } else {
      hiddenUi = true;
    }
  }

  public List<String> getRegexes() {
    return regexes;
  }

  public void setRegexes(List<String> regexes) {
    this.regexes = regexes;
  }

  public ImageSpecParameterType getParamType() {
    return paramType;
  }

  public void setParamType(ImageSpecParameterType parameterType) {
    this.paramType = parameterType;
  }

  public Boolean getOptional() {
    return isOptional;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public List<ImageSpecParameterEnumOption> getEnumOptions() {
    return enumOptions;
  }

  public void setEnumOptions(List<ImageSpecParameterEnumOption> enumOptions) {
    this.enumOptions = enumOptions;
  }

  public void processParamType(Annotation parameterAnnotation) {
    switch (parameterAnnotation.annotationType().getSimpleName()) {
      case "Text":
        TemplateParameter.Text simpleTextParam = (TemplateParameter.Text) parameterAnnotation;
        if (!simpleTextParam.name().isEmpty()) {
          this.setName(simpleTextParam.name());
        }
        processDescriptions(
            simpleTextParam.groupName(),
            simpleTextParam.description(),
            simpleTextParam.helpText(),
            simpleTextParam.example());
        this.setOptional(simpleTextParam.optional());
        this.setHiddenUi(simpleTextParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);

        break;
      case "GcsReadFile":
        TemplateParameter.GcsReadFile gcsReadFileParam =
            (TemplateParameter.GcsReadFile) parameterAnnotation;
        if (!gcsReadFileParam.name().isEmpty()) {
          this.setName(gcsReadFileParam.name());
        }
        processDescriptions(
            gcsReadFileParam.groupName(),
            gcsReadFileParam.description(),
            gcsReadFileParam.helpText(),
            gcsReadFileParam.example());
        this.setOptional(gcsReadFileParam.optional());
        this.setHiddenUi(gcsReadFileParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_READ_FILE);
        break;
      case "GcsReadFolder":
        TemplateParameter.GcsReadFolder gcsReadFolderParam =
            (TemplateParameter.GcsReadFolder) parameterAnnotation;
        if (!gcsReadFolderParam.name().isEmpty()) {
          this.setName(gcsReadFolderParam.name());
        }
        processDescriptions(
            gcsReadFolderParam.groupName(),
            gcsReadFolderParam.description(),
            gcsReadFolderParam.helpText(),
            gcsReadFolderParam.example());
        this.setOptional(gcsReadFolderParam.optional());
        this.setHiddenUi(gcsReadFolderParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_READ_FOLDER);
        break;
      case "GcsWriteFile":
        TemplateParameter.GcsWriteFile gcsWriteFileParam =
            (TemplateParameter.GcsWriteFile) parameterAnnotation;
        if (!gcsWriteFileParam.name().isEmpty()) {
          this.setName(gcsWriteFileParam.name());
        }
        processDescriptions(
            gcsWriteFileParam.groupName(),
            gcsWriteFileParam.description(),
            gcsWriteFileParam.helpText(),
            gcsWriteFileParam.example());
        this.setOptional(gcsWriteFileParam.optional());
        this.setHiddenUi(gcsWriteFileParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_WRITE_FILE);
        break;
      case "GcsWriteFolder":
        TemplateParameter.GcsWriteFolder gcsWriteFolderParam =
            (TemplateParameter.GcsWriteFolder) parameterAnnotation;
        if (!gcsWriteFolderParam.name().isEmpty()) {
          this.setName(gcsWriteFolderParam.name());
        }
        processDescriptions(
            gcsWriteFolderParam.groupName(),
            gcsWriteFolderParam.description(),
            gcsWriteFolderParam.helpText(),
            gcsWriteFolderParam.example());
        this.setOptional(gcsWriteFolderParam.optional());
        this.setHiddenUi(gcsWriteFolderParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_WRITE_FOLDER);
        break;
      case "PubsubSubscription":
        TemplateParameter.PubsubSubscription pubsubSubscriptionParam =
            (TemplateParameter.PubsubSubscription) parameterAnnotation;
        if (!pubsubSubscriptionParam.name().isEmpty()) {
          this.setName(pubsubSubscriptionParam.name());
        }
        processDescriptions(
            pubsubSubscriptionParam.groupName(),
            pubsubSubscriptionParam.description(),
            pubsubSubscriptionParam.helpText(),
            pubsubSubscriptionParam.example());
        this.setOptional(pubsubSubscriptionParam.optional());
        this.setHiddenUi(pubsubSubscriptionParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.PUBSUB_SUBSCRIPTION);
        break;
      case "PubsubTopic":
        TemplateParameter.PubsubTopic pubsubTopicParam =
            (TemplateParameter.PubsubTopic) parameterAnnotation;
        if (!pubsubTopicParam.name().isEmpty()) {
          this.setName(pubsubTopicParam.name());
        }
        processDescriptions(
            pubsubTopicParam.groupName(),
            pubsubTopicParam.description(),
            pubsubTopicParam.helpText(),
            pubsubTopicParam.example());
        this.setOptional(pubsubTopicParam.optional());
        this.setHiddenUi(pubsubTopicParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.PUBSUB_TOPIC);
        break;
      case "Password":
        TemplateParameter.Password passwordParam = (TemplateParameter.Password) parameterAnnotation;
        if (!passwordParam.name().isEmpty()) {
          this.setName(passwordParam.name());
        }
        processDescriptions(
            passwordParam.groupName(),
            passwordParam.description(),
            passwordParam.helpText(),
            passwordParam.example());
        this.setOptional(passwordParam.optional());
        this.setHiddenUi(passwordParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "ProjectId":
        TemplateParameter.ProjectId projectIdParam =
            (TemplateParameter.ProjectId) parameterAnnotation;
        if (!projectIdParam.name().isEmpty()) {
          this.setName(projectIdParam.name());
        }
        processDescriptions(
            projectIdParam.groupName(),
            projectIdParam.description(),
            projectIdParam.helpText(),
            projectIdParam.example());
        this.setOptional(projectIdParam.optional());
        this.setHiddenUi(projectIdParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "Boolean":
        TemplateParameter.Boolean booleanParam = (TemplateParameter.Boolean) parameterAnnotation;
        if (!booleanParam.name().isEmpty()) {
          this.setName(booleanParam.name());
        }
        processDescriptions(
            booleanParam.groupName(),
            booleanParam.description(),
            booleanParam.helpText(),
            booleanParam.example());
        this.setOptional(booleanParam.optional());
        this.setHiddenUi(booleanParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.BOOLEAN);
        break;
      case "Integer":
        TemplateParameter.Integer integerParam = (TemplateParameter.Integer) parameterAnnotation;
        if (!integerParam.name().isEmpty()) {
          this.setName(integerParam.name());
        }
        processDescriptions(
            integerParam.groupName(),
            integerParam.description(),
            integerParam.helpText(),
            integerParam.example());
        this.setOptional(integerParam.optional());
        this.setHiddenUi(integerParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.NUMBER);
        break;
      case "Long":
        TemplateParameter.Long longParam = (TemplateParameter.Long) parameterAnnotation;
        if (!longParam.name().isEmpty()) {
          this.setName(longParam.name());
        }
        processDescriptions(
            longParam.groupName(),
            longParam.description(),
            longParam.helpText(),
            longParam.example());
        this.setOptional(longParam.optional());
        this.setHiddenUi(longParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.NUMBER);
        break;
      case "Float":
        TemplateParameter.Float floatParam = (TemplateParameter.Float) parameterAnnotation;
        if (!floatParam.name().isEmpty()) {
          this.setName(floatParam.name());
        }
        processDescriptions(
            floatParam.groupName(),
            floatParam.description(),
            floatParam.helpText(),
            floatParam.example());
        this.setOptional(floatParam.optional());
        this.setHiddenUi(floatParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.NUMBER);
        break;
      case "Double":
        TemplateParameter.Double doubleParam = (TemplateParameter.Double) parameterAnnotation;
        if (!doubleParam.name().isEmpty()) {
          this.setName(doubleParam.name());
        }
        processDescriptions(
            doubleParam.groupName(),
            doubleParam.description(),
            doubleParam.helpText(),
            doubleParam.example());
        this.setOptional(doubleParam.optional());
        this.setHiddenUi(doubleParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.NUMBER);
        break;
      case "Enum":
        TemplateParameter.Enum enumParam = (TemplateParameter.Enum) parameterAnnotation;
        if (!enumParam.name().isEmpty()) {
          this.setName(enumParam.name());
        }
        processDescriptions(
            enumParam.groupName(),
            enumParam.description(),
            enumParam.helpText(),
            enumParam.example());
        this.setOptional(enumParam.optional());
        this.setHiddenUi(enumParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.ENUM);
        this.setEnumOptions(buildEnumOptions(enumParam));
        break;
      case "DateTime":
        TemplateParameter.DateTime dateTimeParam = (TemplateParameter.DateTime) parameterAnnotation;
        if (!dateTimeParam.name().isEmpty()) {
          this.setName(dateTimeParam.name());
        }
        processDescriptions(
            dateTimeParam.groupName(),
            dateTimeParam.description(),
            dateTimeParam.helpText(),
            dateTimeParam.example());
        this.setOptional(dateTimeParam.optional());
        this.setHiddenUi(dateTimeParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "BigQueryTable":
        TemplateParameter.BigQueryTable bigQueryTableParam =
            (TemplateParameter.BigQueryTable) parameterAnnotation;
        if (!bigQueryTableParam.name().isEmpty()) {
          this.setName(bigQueryTableParam.name());
        }
        processDescriptions(
            bigQueryTableParam.groupName(),
            bigQueryTableParam.description(),
            bigQueryTableParam.helpText(),
            bigQueryTableParam.example());
        this.setOptional(bigQueryTableParam.optional());
        this.setHiddenUi(bigQueryTableParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.BIGQUERY_TABLE);
        break;
      case "KmsEncryptionKey":
        TemplateParameter.KmsEncryptionKey kmsEncryptionKeyParam =
            (TemplateParameter.KmsEncryptionKey) parameterAnnotation;
        if (!kmsEncryptionKeyParam.name().isEmpty()) {
          this.setName(kmsEncryptionKeyParam.name());
        }
        processDescriptions(
            kmsEncryptionKeyParam.groupName(),
            kmsEncryptionKeyParam.description(),
            kmsEncryptionKeyParam.helpText(),
            kmsEncryptionKeyParam.example());
        this.setOptional(kmsEncryptionKeyParam.optional());
        this.setHiddenUi(kmsEncryptionKeyParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "Duration":
        TemplateParameter.Duration durationParam = (TemplateParameter.Duration) parameterAnnotation;
        if (!durationParam.name().isEmpty()) {
          this.setName(durationParam.name());
        }
        processDescriptions(
            durationParam.groupName(),
            durationParam.description(),
            durationParam.helpText(),
            durationParam.example());
        this.setOptional(durationParam.optional());
        this.setHiddenUi(durationParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      default:
        throw new IllegalArgumentException("Invalid type " + parameterAnnotation);
    }
    this.setRegexes(MetadataUtils.getRegexes(parameterAnnotation));
  }

  private static List<ImageSpecParameterEnumOption> buildEnumOptions(
      TemplateParameter.Enum enumParam) {
    return Arrays.stream(enumParam.enumOptions())
        .map(
            option ->
                new ImageSpecParameterEnumOption(
                    option.value(), option.label(), option.description()))
        .collect(Collectors.toList());
  }

  protected void processDescriptions(
      String groupName, String description, String helpText, String example) {

    if (StringUtils.isNotEmpty(groupName)) {
      this.setGroupName(groupName);
    }

    if (description != null) {
      this.setLabel(description);
    }

    if (helpText != null) {
      this.setHelpText(helpText);

      if (example != null && !example.isEmpty()) {
        this.setHelpText(this.getHelpText() + " (Example: " + example + ")");
      }
    }
  }

  public void validate() {
    if (getName() == null || getName().isEmpty()) {
      throw new IllegalArgumentException("Parameter name can not be empty.");
    }
    if (getLabel() == null || getLabel().isEmpty()) {
      throw new IllegalArgumentException("Parameter label can not be empty.");
    }
  }
}
