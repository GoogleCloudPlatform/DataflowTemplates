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
package com.google.api.services.datastream.v1alpha1.model;

/**
 * MySQL SSL configuration information.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the DataStream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class MysqlSslConfig extends com.google.api.client.json.GenericJson {

  /**
   * Input only. PEM-encoded certificate of the CA that signed the source database server's
   * certificate. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String caCertificate;

  /**
   * Output only. Indicates whether the ca_certificate field is set. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean caCertificateSet;

  /**
   * Input only. PEM-encoded certificate that will be used by the replica to authenticate against
   * the source database server. If this field is used then the 'client_key' and the
   * 'ca_certificate' fields are mandatory. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String clientCertificate;

  /**
   * Output only. Indicates whether the client_certificate field is set. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean clientCertificateSet;

  /**
   * Input only. PEM-encoded private key associated with the Client Certificate. If this field is
   * used then the 'client_certificate' and the 'ca_certificate' fields are mandatory. The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String clientKey;

  /** Output only. Indicates whether the client_key field is set. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean clientKeySet;

  /**
   * Input only. PEM-encoded certificate of the CA that signed the source database server's
   * certificate.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getCaCertificate() {
    return caCertificate;
  }

  /**
   * Input only. PEM-encoded certificate of the CA that signed the source database server's
   * certificate.
   *
   * @param caCertificate caCertificate or {@code null} for none
   */
  public MysqlSslConfig setCaCertificate(java.lang.String caCertificate) {
    this.caCertificate = caCertificate;
    return this;
  }

  /**
   * Output only. Indicates whether the ca_certificate field is set.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getCaCertificateSet() {
    return caCertificateSet;
  }

  /**
   * Output only. Indicates whether the ca_certificate field is set.
   *
   * @param caCertificateSet caCertificateSet or {@code null} for none
   */
  public MysqlSslConfig setCaCertificateSet(java.lang.Boolean caCertificateSet) {
    this.caCertificateSet = caCertificateSet;
    return this;
  }

  /**
   * Input only. PEM-encoded certificate that will be used by the replica to authenticate against
   * the source database server. If this field is used then the 'client_key' and the
   * 'ca_certificate' fields are mandatory.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getClientCertificate() {
    return clientCertificate;
  }

  /**
   * Input only. PEM-encoded certificate that will be used by the replica to authenticate against
   * the source database server. If this field is used then the 'client_key' and the
   * 'ca_certificate' fields are mandatory.
   *
   * @param clientCertificate clientCertificate or {@code null} for none
   */
  public MysqlSslConfig setClientCertificate(java.lang.String clientCertificate) {
    this.clientCertificate = clientCertificate;
    return this;
  }

  /**
   * Output only. Indicates whether the client_certificate field is set.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getClientCertificateSet() {
    return clientCertificateSet;
  }

  /**
   * Output only. Indicates whether the client_certificate field is set.
   *
   * @param clientCertificateSet clientCertificateSet or {@code null} for none
   */
  public MysqlSslConfig setClientCertificateSet(java.lang.Boolean clientCertificateSet) {
    this.clientCertificateSet = clientCertificateSet;
    return this;
  }

  /**
   * Input only. PEM-encoded private key associated with the Client Certificate. If this field is
   * used then the 'client_certificate' and the 'ca_certificate' fields are mandatory.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getClientKey() {
    return clientKey;
  }

  /**
   * Input only. PEM-encoded private key associated with the Client Certificate. If this field is
   * used then the 'client_certificate' and the 'ca_certificate' fields are mandatory.
   *
   * @param clientKey clientKey or {@code null} for none
   */
  public MysqlSslConfig setClientKey(java.lang.String clientKey) {
    this.clientKey = clientKey;
    return this;
  }

  /**
   * Output only. Indicates whether the client_key field is set.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getClientKeySet() {
    return clientKeySet;
  }

  /**
   * Output only. Indicates whether the client_key field is set.
   *
   * @param clientKeySet clientKeySet or {@code null} for none
   */
  public MysqlSslConfig setClientKeySet(java.lang.Boolean clientKeySet) {
    this.clientKeySet = clientKeySet;
    return this;
  }

  @Override
  public MysqlSslConfig set(String fieldName, Object value) {
    return (MysqlSslConfig) super.set(fieldName, value);
  }

  @Override
  public MysqlSslConfig clone() {
    return (MysqlSslConfig) super.clone();
  }
}
