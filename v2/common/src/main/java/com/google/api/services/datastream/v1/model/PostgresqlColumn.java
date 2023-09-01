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
package com.google.api.services.datastream.v1.model;

/**
 * Postgresql Column.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class PostgresqlColumn extends com.google.api.client.json.GenericJson {

  /** Column name. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String column;

  /** The PostgreSQL data type. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String dataType;

  /** Column length. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer length;

  /** Column precision. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer precision;

  /** Column scale. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer scale;

  /** Whether or not the column represents a primary key. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean primaryKey;

  /** Whether or not the column can accept a null value. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Boolean nullable;

  /** Column encoding. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String encoding;

  /** The ordinal position of the column in the table. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.Integer ordinalPosition;

  /**
   * Column name.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getColumn() {
    return column;
  }

  /**
   * Column name.
   *
   * @param column column or {@code null} for none
   */
  public PostgresqlColumn setColumn(java.lang.String column) {
    this.column = column;
    return this;
  }

  /**
   * The PostgreSQL data type.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDataType() {
    return dataType;
  }

  /**
   * The PostgreSQL data type.
   *
   * @param dataType dataType or {@code null} for none
   */
  public PostgresqlColumn setDataType(java.lang.String dataType) {
    this.dataType = dataType;
    return this;
  }

  /**
   * Column encoding.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getEncoding() {
    return encoding;
  }

  /**
   * Column encoding.
   *
   * @param encoding encoding or {@code null} for none
   */
  public PostgresqlColumn setEncoding(java.lang.String encoding) {
    this.encoding = encoding;
    return this;
  }

  /**
   * Column length.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getLength() {
    return length;
  }

  /**
   * Column length.
   *
   * @param length length or {@code null} for none
   */
  public PostgresqlColumn setLength(java.lang.Integer length) {
    this.length = length;
    return this;
  }

  /**
   * Whether or not the column can accept a null value.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getNullable() {
    return nullable;
  }

  /**
   * Whether or not the column can accept a null value.
   *
   * @param nullable nullable or {@code null} for none
   */
  public PostgresqlColumn setNullable(java.lang.Boolean nullable) {
    this.nullable = nullable;
    return this;
  }

  /**
   * The ordinal position of the column in the table.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getOrdinalPosition() {
    return ordinalPosition;
  }

  /**
   * The ordinal position of the column in the table.
   *
   * @param ordinalPosition ordinalPosition or {@code null} for none
   */
  public PostgresqlColumn setOrdinalPosition(java.lang.Integer ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
    return this;
  }

  /**
   * Column precision.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getPrecision() {
    return precision;
  }

  /**
   * Column precision.
   *
   * @param precision precision or {@code null} for none
   */
  public PostgresqlColumn setPrecision(java.lang.Integer precision) {
    this.precision = precision;
    return this;
  }

  /**
   * Whether or not the column represents a primary key.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Whether or not the column represents a primary key.
   *
   * @param primaryKey primaryKey or {@code null} for none
   */
  public PostgresqlColumn setPrimaryKey(java.lang.Boolean primaryKey) {
    this.primaryKey = primaryKey;
    return this;
  }

  /**
   * Column scale.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getScale() {
    return scale;
  }

  /**
   * Column scale.
   *
   * @param scale scale or {@code null} for none
   */
  public PostgresqlColumn setScale(java.lang.Integer scale) {
    this.scale = scale;
    return this;
  }

  @Override
  public PostgresqlColumn set(String fieldName, Object value) {
    return (PostgresqlColumn) super.set(fieldName, value);
  }

  @Override
  public PostgresqlColumn clone() {
    return (PostgresqlColumn) super.clone();
  }
}
