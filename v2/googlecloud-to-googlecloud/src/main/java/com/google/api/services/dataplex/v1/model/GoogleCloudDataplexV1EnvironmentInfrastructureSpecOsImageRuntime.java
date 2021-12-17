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
 * Software Runtime Configuration to run Analyze.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime
    extends com.google.api.client.json.GenericJson {

  /** Required. Dataplex Image version. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String imageVersion;

  /**
   * Optional. List of Java jars to be included in the runtime environment. Valid input includes
   * Cloud Storage URIs to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar The value
   * may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> javaLibraries;

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.Map<String, java.lang.String> properties;

  /**
   * Optional. A list of python packages to be installed. Valid formats include Cloud Storage URI to
   * a PIP installable library. For example, gs://bucket-name/my/path/to/lib.tar.gz The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> pythonPackages;

  /**
   * Required. Dataplex Image version.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getImageVersion() {
    return imageVersion;
  }

  /**
   * Required. Dataplex Image version.
   *
   * @param imageVersion imageVersion or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime setImageVersion(
      java.lang.String imageVersion) {
    this.imageVersion = imageVersion;
    return this;
  }

  /**
   * Optional. List of Java jars to be included in the runtime environment. Valid input includes
   * Cloud Storage URIs to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getJavaLibraries() {
    return javaLibraries;
  }

  /**
   * Optional. List of Java jars to be included in the runtime environment. Valid input includes
   * Cloud Storage URIs to Jar binaries. For example, gs://bucket-name/my/path/to/file.jar
   *
   * @param javaLibraries javaLibraries or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime setJavaLibraries(
      java.util.List<java.lang.String> javaLibraries) {
    this.javaLibraries = javaLibraries;
    return this;
  }

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format.
   *
   * @return value or {@code null} for none
   */
  public java.util.Map<String, java.lang.String> getProperties() {
    return properties;
  }

  /**
   * Optional. Override to common configuration of open source components installed on the Dataproc
   * cluster. The properties to set on daemon config files. Property keys are specified in
   * prefix:property format.
   *
   * @param properties properties or {@code null} for none
   */
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime setProperties(
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
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime setPythonPackages(
      java.util.List<java.lang.String> pythonPackages) {
    this.pythonPackages = pythonPackages;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime set(
      String fieldName, Object value) {
    return (GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime)
        super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime clone() {
    return (GoogleCloudDataplexV1EnvironmentInfrastructureSpecOsImageRuntime) super.clone();
  }
}
