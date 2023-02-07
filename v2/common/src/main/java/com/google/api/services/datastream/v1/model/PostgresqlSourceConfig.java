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
 * /** data source configuration
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Datastream API. For a detailed explanation see: <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class PostgresqlSourceConfig extends com.google.api.client.json.GenericJson {

  /** PostgreSQL objects to exclude from the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private PostgresqlRdbms excludeObjects;

  /** PostgreSQL objects to include in the stream. The value may be {@code null}. */
  @com.google.api.client.util.Key private PostgresqlRdbms includeObjects;

  /** The name of the logical replication slot that's configured with the pgoutput plugin. */
  @com.google.api.client.util.Key private String replicationSlot;

  /**
   * The name of the publication that includes the set of all tables that are defined in the
   * stream's include_objects.
   */
  @com.google.api.client.util.Key private String publication;

  /**
   * PostgreSQL objects to exclude from the stream.
   *
   * @return value or {@code null} for none
   */
  public PostgresqlRdbms getExcludeObjects() {
    return excludeObjects;
  }

  /**
   * PostgreSQL objects to exclude from the stream.
   *
   * @param excludeObjects excludeObjects or {@code null} for none
   */
  public PostgresqlSourceConfig setExcludeObjects(PostgresqlRdbms excludeObjects) {
    this.excludeObjects = excludeObjects;
    return this;
  }

  /**
   * PostgreSQL objects to include in the stream.
   *
   * @return value or {@code null} for none
   */
  public PostgresqlRdbms getIncludeObjects() {
    return includeObjects;
  }

  /**
   * PostgreSQL objects to include in the stream.
   *
   * @param includeObjects includeObjects or {@code null} for none
   */
  public PostgresqlSourceConfig setIncludeObjects(PostgresqlRdbms includeObjects) {
    this.includeObjects = includeObjects;
    return this;
  }

  /**
   * PostgreSQL name of the publication that includes the set of all tables that are defined in the
   * stream's include_objects.
   *
   * @return value
   */
  public String getPublication() {
    return publication;
  }

  /**
   * PostgreSQL name of the publication that includes the set of all tables that are defined in the
   * stream's include_objects.
   *
   * @param publication publication
   */
  public PostgresqlSourceConfig setPublication(String publication) {
    this.publication = publication;
    return this;
  }

  /**
   * PostgreSQL name of the logical replication slot that's configured with the pgoutput plugin.
   *
   * @return value
   */
  public String getReplicationSlot() {
    return replicationSlot;
  }

  /**
   * PostgreSQL name of the logical replication slot that's configured with the pgoutput plugin.
   *
   * @param replicationSlot replicationSlot
   */
  public PostgresqlSourceConfig setReplicationSlot(String replicationSlot) {
    this.replicationSlot = replicationSlot;
    return this;
  }

  @Override
  public PostgresqlSourceConfig set(String fieldName, Object value) {
    return (PostgresqlSourceConfig) super.set(fieldName, value);
  }

  @Override
  public PostgresqlSourceConfig clone() {
    return (PostgresqlSourceConfig) super.clone();
  }
}
