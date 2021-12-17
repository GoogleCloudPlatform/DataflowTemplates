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
 * Describes CSV and similar semi-structured data formats.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1StorageFormatCsvOptions
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. The delimiter being used to separate values. This defaults to ','. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String delimiter;

  /**
   * Optional. The character encoding of the data. The default is UTF-8. The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private java.lang.String encoding;

  /**
   * Optional. The number of rows to interpret as header rows that should be skipped when reading
   * data rows. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Integer headerRows;

  /**
   * Optional. The character used to quote column values. This defaults to empty, implying unquoted
   * data. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String quote;

  /**
   * Optional. The delimiter being used to separate values. This defaults to ','.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDelimiter() {
    return delimiter;
  }

  /**
   * Optional. The delimiter being used to separate values. This defaults to ','.
   *
   * @param delimiter delimiter or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatCsvOptions setDelimiter(java.lang.String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  /**
   * Optional. The character encoding of the data. The default is UTF-8.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEncoding() {
    return encoding;
  }

  /**
   * Optional. The character encoding of the data. The default is UTF-8.
   *
   * @param encoding encoding or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatCsvOptions setEncoding(java.lang.String encoding) {
    this.encoding = encoding;
    return this;
  }

  /**
   * Optional. The number of rows to interpret as header rows that should be skipped when reading
   * data rows.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getHeaderRows() {
    return headerRows;
  }

  /**
   * Optional. The number of rows to interpret as header rows that should be skipped when reading
   * data rows.
   *
   * @param headerRows headerRows or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatCsvOptions setHeaderRows(java.lang.Integer headerRows) {
    this.headerRows = headerRows;
    return this;
  }

  /**
   * Optional. The character used to quote column values. This defaults to empty, implying unquoted
   * data.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getQuote() {
    return quote;
  }

  /**
   * Optional. The character used to quote column values. This defaults to empty, implying unquoted
   * data.
   *
   * @param quote quote or {@code null} for none
   */
  public GoogleCloudDataplexV1StorageFormatCsvOptions setQuote(java.lang.String quote) {
    this.quote = quote;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1StorageFormatCsvOptions set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1StorageFormatCsvOptions) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1StorageFormatCsvOptions clone() {
    return (GoogleCloudDataplexV1StorageFormatCsvOptions) super.clone();
  }
}
