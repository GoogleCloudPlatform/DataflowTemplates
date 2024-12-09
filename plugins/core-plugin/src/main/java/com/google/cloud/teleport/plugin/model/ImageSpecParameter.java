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
  private String parentName;
  private String label;
  private String helpText;
  private Boolean isOptional;
  private Boolean hiddenUi;
  private List<String> regexes;
  private List<String> parentTriggerValues;
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
      this.isOptional = null;
    } else {
      this.isOptional = true;
    }
  }

  public void setHiddenUi(Boolean hiddenUi) {
    if (hiddenUi == null || !hiddenUi) {
      this.hiddenUi = null;
    } else {
      this.hiddenUi = true;
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

  public String getParentName() {
    return parentName;
  }

  public void setParentName(String parentName) {
    if (StringUtils.isNotEmpty(parentName)) {
      this.parentName = parentName;
    }
  }

  public List<String> getParentTriggerValues() {
    return parentTriggerValues;
  }

  public void setParentTriggerValues(String[] parentTriggerValues) {
    if (parentTriggerValues != null) {
      this.parentTriggerValues = Arrays.stream(parentTriggerValues).collect(Collectors.toList());
    }
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
        this.setParentName(simpleTextParam.parentName());
        this.setParentTriggerValues(simpleTextParam.parentTriggerValues());
        this.setOptional(simpleTextParam.optional());
        this.setHiddenUi(simpleTextParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "JavascriptUdfFile":
        TemplateParameter.JavascriptUdfFile javascriptUdfFileParam =
            (TemplateParameter.JavascriptUdfFile) parameterAnnotation;
        if (!javascriptUdfFileParam.name().isEmpty()) {
          this.setName(javascriptUdfFileParam.name());
        }
        processDescriptions(
            javascriptUdfFileParam.groupName(),
            javascriptUdfFileParam.description(),
            javascriptUdfFileParam.helpText(),
            javascriptUdfFileParam.example());
        this.setParentName(javascriptUdfFileParam.parentName());
        this.setParentTriggerValues(javascriptUdfFileParam.parentTriggerValues());
        this.setOptional(javascriptUdfFileParam.optional());
        this.setHiddenUi(javascriptUdfFileParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.JAVASCRIPT_UDF_FILE);
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
        this.setParentName(gcsReadFileParam.parentName());
        this.setParentTriggerValues(gcsReadFileParam.parentTriggerValues());
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
        this.setParentName(gcsReadFolderParam.parentName());
        this.setParentTriggerValues(gcsReadFolderParam.parentTriggerValues());
        this.setOptional(gcsReadFolderParam.optional());
        this.setHiddenUi(gcsReadFolderParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_READ_FOLDER);
        break;
      case "GcsReadBucket":
        TemplateParameter.GcsReadBucket gcsReadBucketParam =
            (TemplateParameter.GcsReadBucket) parameterAnnotation;
        if (!gcsReadBucketParam.name().isEmpty()) {
          this.setName(gcsReadBucketParam.name());
        }
        processDescriptions(
            gcsReadBucketParam.groupName(),
            gcsReadBucketParam.description(),
            gcsReadBucketParam.helpText(),
            gcsReadBucketParam.example());
        this.setParentName(gcsReadBucketParam.parentName());
        this.setParentTriggerValues(gcsReadBucketParam.parentTriggerValues());
        this.setOptional(gcsReadBucketParam.optional());
        this.setHiddenUi(gcsReadBucketParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_READ_BUCKET);
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
        this.setParentName(gcsWriteFileParam.parentName());
        this.setParentTriggerValues(gcsWriteFileParam.parentTriggerValues());
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
        this.setParentName(gcsWriteFolderParam.parentName());
        this.setParentTriggerValues(gcsWriteFolderParam.parentTriggerValues());
        this.setOptional(gcsWriteFolderParam.optional());
        this.setHiddenUi(gcsWriteFolderParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_WRITE_FOLDER);
        break;
      case "GcsWriteBucket":
        TemplateParameter.GcsWriteBucket gcsWriteBucketParam =
            (TemplateParameter.GcsWriteBucket) parameterAnnotation;
        if (!gcsWriteBucketParam.name().isEmpty()) {
          this.setName(gcsWriteBucketParam.name());
        }
        processDescriptions(
            gcsWriteBucketParam.groupName(),
            gcsWriteBucketParam.description(),
            gcsWriteBucketParam.helpText(),
            gcsWriteBucketParam.example());
        this.setParentName(gcsWriteBucketParam.parentName());
        this.setParentTriggerValues(gcsWriteBucketParam.parentTriggerValues());
        this.setOptional(gcsWriteBucketParam.optional());
        this.setHiddenUi(gcsWriteBucketParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.GCS_WRITE_BUCKET);
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
        this.setParentName(pubsubSubscriptionParam.parentName());
        this.setParentTriggerValues(pubsubSubscriptionParam.parentTriggerValues());
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
        this.setParentName(pubsubTopicParam.parentName());
        this.setParentTriggerValues(pubsubTopicParam.parentTriggerValues());
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
        this.setParentName(passwordParam.parentName());
        this.setParentTriggerValues(passwordParam.parentTriggerValues());
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
        this.setParentName(projectIdParam.parentName());
        this.setParentTriggerValues(projectIdParam.parentTriggerValues());
        this.setOptional(projectIdParam.optional());
        this.setHiddenUi(projectIdParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "MachineType":
        TemplateParameter.MachineType machineTypeParam =
            (TemplateParameter.MachineType) parameterAnnotation;
        if (!machineTypeParam.name().isEmpty()) {
          this.setName(machineTypeParam.name());
        }
        processDescriptions(
            machineTypeParam.groupName(),
            machineTypeParam.description(),
            machineTypeParam.helpText(),
            machineTypeParam.example());
        this.setParentName(machineTypeParam.parentName());
        this.setParentTriggerValues(machineTypeParam.parentTriggerValues());
        this.setOptional(machineTypeParam.optional());
        this.setHiddenUi(machineTypeParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.MACHINE_TYPE);
        break;
      case "ServiceAccount":
        TemplateParameter.ServiceAccount serviceAccountParam =
            (TemplateParameter.ServiceAccount) parameterAnnotation;
        if (!serviceAccountParam.name().isEmpty()) {
          this.setName(serviceAccountParam.name());
        }
        processDescriptions(
            serviceAccountParam.groupName(),
            serviceAccountParam.description(),
            serviceAccountParam.helpText(),
            serviceAccountParam.example());
        this.setParentName(serviceAccountParam.parentName());
        this.setParentTriggerValues(serviceAccountParam.parentTriggerValues());
        this.setOptional(serviceAccountParam.optional());
        this.setHiddenUi(serviceAccountParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.SERVICE_ACCOUNT);
        break;
      case "WorkerRegion":
        TemplateParameter.WorkerRegion workerRegionParam =
            (TemplateParameter.WorkerRegion) parameterAnnotation;
        if (!workerRegionParam.name().isEmpty()) {
          this.setName(workerRegionParam.name());
        }
        processDescriptions(
            workerRegionParam.groupName(),
            workerRegionParam.description(),
            workerRegionParam.helpText(),
            workerRegionParam.example());
        this.setParentName(workerRegionParam.parentName());
        this.setParentTriggerValues(workerRegionParam.parentTriggerValues());
        this.setOptional(workerRegionParam.optional());
        this.setHiddenUi(workerRegionParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.WORKER_REGION);
        break;
      case "WorkerZone":
        TemplateParameter.WorkerZone workerZoneParam =
            (TemplateParameter.WorkerZone) parameterAnnotation;
        if (!workerZoneParam.name().isEmpty()) {
          this.setName(workerZoneParam.name());
        }
        processDescriptions(
            workerZoneParam.groupName(),
            workerZoneParam.description(),
            workerZoneParam.helpText(),
            workerZoneParam.example());
        this.setParentName(workerZoneParam.parentName());
        this.setParentTriggerValues(workerZoneParam.parentTriggerValues());
        this.setOptional(workerZoneParam.optional());
        this.setHiddenUi(workerZoneParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.WORKER_ZONE);
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
        this.setParentName(booleanParam.parentName());
        this.setParentTriggerValues(booleanParam.parentTriggerValues());
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
        this.setParentName(integerParam.parentName());
        this.setParentTriggerValues(integerParam.parentTriggerValues());
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
        this.setParentName(longParam.parentName());
        this.setParentTriggerValues(longParam.parentTriggerValues());
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
        this.setParentName(floatParam.parentName());
        this.setParentTriggerValues(floatParam.parentTriggerValues());
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
        this.setParentName(doubleParam.parentName());
        this.setParentTriggerValues(doubleParam.parentTriggerValues());
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
        this.setParentName(enumParam.parentName());
        this.setParentTriggerValues(enumParam.parentTriggerValues());
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
        this.setParentName(dateTimeParam.parentName());
        this.setParentTriggerValues(dateTimeParam.parentTriggerValues());
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
        this.setParentName(bigQueryTableParam.parentName());
        this.setParentTriggerValues(bigQueryTableParam.parentTriggerValues());
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
        this.setParentName(kmsEncryptionKeyParam.parentName());
        this.setParentTriggerValues(kmsEncryptionKeyParam.parentTriggerValues());
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
        this.setParentName(durationParam.parentName());
        this.setParentTriggerValues(durationParam.parentTriggerValues());
        this.setOptional(durationParam.optional());
        this.setHiddenUi(durationParam.hiddenUi());
        this.setParamType(ImageSpecParameterType.TEXT);
        break;
      case "KafkaReadTopic":
        TemplateParameter.KafkaReadTopic kafkaReadTopic =
            (TemplateParameter.KafkaReadTopic) parameterAnnotation;
        if (!kafkaReadTopic.name().isEmpty()) {
          this.setName(kafkaReadTopic.name());
        }
        processDescriptions(
            kafkaReadTopic.groupName(),
            kafkaReadTopic.description(),
            kafkaReadTopic.helpText(),
            kafkaReadTopic.example());
        this.setParentName(kafkaReadTopic.parentName());
        this.setParentTriggerValues(kafkaReadTopic.parentTriggerValues());
        this.setOptional(kafkaReadTopic.optional());
        this.setHiddenUi(kafkaReadTopic.hiddenUi());
        this.setParamType(ImageSpecParameterType.KAFKA_READ_TOPIC);
        break;

      case "KafkaWriteTopic":
        TemplateParameter.KafkaWriteTopic kafkaWriteTopic =
            (TemplateParameter.KafkaWriteTopic) parameterAnnotation;
        if (!kafkaWriteTopic.name().isEmpty()) {
          this.setName(kafkaWriteTopic.name());
        }
        processDescriptions(
            kafkaWriteTopic.groupName(),
            kafkaWriteTopic.description(),
            kafkaWriteTopic.helpText(),
            kafkaWriteTopic.example());
        this.setParentName(kafkaWriteTopic.parentName());
        this.setParentTriggerValues(kafkaWriteTopic.parentTriggerValues());
        this.setOptional(kafkaWriteTopic.optional());
        this.setHiddenUi(kafkaWriteTopic.hiddenUi());
        this.setParamType(ImageSpecParameterType.KAFKA_WRITE_TOPIC);
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
        this.setHelpText(this.getHelpText() + " For example, `" + example + "`");
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
