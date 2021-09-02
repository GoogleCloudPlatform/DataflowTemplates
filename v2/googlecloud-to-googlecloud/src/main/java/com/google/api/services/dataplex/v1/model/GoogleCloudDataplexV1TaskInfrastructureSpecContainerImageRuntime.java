/*
 * Copyright (C) 2021 Google LLC
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
package com.google.api.services.dataplex.v1.model;

/**
 * Container Image Runtime Configuration used with Batch execution.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime
    extends com.google.api.client.json.GenericJson {

  /** Required. Container image to use. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String image;

  /**
   * Optional. A list of Java JARS to add to the classpath. Valid input includes Cloud Storage URIs
   * to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> javaJars;

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format, for example core:hadoop.tmp.dir. For more information, see Cluster
   * properties (https://cloud.google.com/dataproc/docs/concepts/cluster-properties). The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> properties;

  /**
   * Optional. A list of python packages to be installed. Valid formats include Cloud Storage URI to
   * a PIP installable library. For example, gs://bucket-name/my/path/to/lib.tar.gz The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> pythonPackages;

  /**
   * Required. Container image to use.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getImage() {
    return image;
  }

  /**
   * Required. Container image to use.
   *
   * @param image image or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime setImage(
      java.lang.String image) {
    this.image = image;
    return this;
  }

  /**
   * Optional. A list of Java JARS to add to the classpath. Valid input includes Cloud Storage URIs
   * to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getJavaJars() {
    return javaJars;
  }

  /**
   * Optional. A list of Java JARS to add to the classpath. Valid input includes Cloud Storage URIs
   * to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar
   *
   * @param javaJars javaJars or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime setJavaJars(
      java.util.List<java.lang.String> javaJars) {
    this.javaJars = javaJars;
    return this;
  }

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format, for example core:hadoop.tmp.dir. For more information, see Cluster
   * properties (https://cloud.google.com/dataproc/docs/concepts/cluster-properties).
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getProperties() {
    return properties;
  }

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format, for example core:hadoop.tmp.dir. For more information, see Cluster
   * properties (https://cloud.google.com/dataproc/docs/concepts/cluster-properties).
   *
   * @param properties properties or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime setProperties(
      java.util.Map<String, java.lang.String> properties) {
    this.properties = properties;
    return this;
  }

  /**
   * Optional. A list of python packages to be installed. Valid formats include Cloud Storage URI to
   * a PIP installable library. For example, gs://bucket-name/my/path/to/lib.tar.gz
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getPythonPackages() {
    return pythonPackages;
  }

  /**
   * Optional. A list of python packages to be installed. Valid formats include Cloud Storage URI to
   * a PIP installable library. For example, gs://bucket-name/my/path/to/lib.tar.gz
   *
   * @param pythonPackages pythonPackages or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime setPythonPackages(
      java.util.List<java.lang.String> pythonPackages) {
    this.pythonPackages = pythonPackages;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime)
        super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime clone() {
    return (GoogleCloudDataplexV1TaskInfrastructureSpecContainerImageRuntime) super.clone();
  }
}
