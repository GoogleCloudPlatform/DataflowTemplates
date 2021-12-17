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
 * Represents a textual expression in the Common Expression Language (CEL) syntax. CEL is a C-like
 * expression language. The syntax and semantics of CEL are documented at https://github.com/google
 * /cel-spec.Example (Comparison): title: "Summary size limit" description: "Determines if a summary
 * is less than 100 chars" expression: "document.summary.size() < 100" Example (Equality): title:
 * "Requestor is owner" description: "Determines if requestor is the document owner" expression:
 * "document.owner == request.auth.claims.email" Example (Logic): title: "Public documents"
 * description: "Determine whether the document should be publicly visible" expression:
 * "document.type != 'private' && document.type != 'internal'" Example (Data Manipulation): title:
 * "Notification string" description: "Create a notification string with a timestamp." expression:
 * "'New message received at ' + string(document.create_time)" The exact variables and functions
 * that may be referenced within an expression are determined by the service that evaluates it. See
 * the service documentation for additional information.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleTypeExpr extends com.google.api.client.json.GenericJson {

  /**
   * Optional. Description of the expression. This is a longer text which describes the expression,
   * e.g. when hovered over it in a UI. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String description;

  /**
   * Textual representation of an expression in Common Expression Language syntax. The value may be
   * {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String expression;

  /**
   * Optional. String indicating the location of the expression for error reporting, e.g. a file
   * name and a position in the file. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String location;

  /**
   * Optional. Title for the expression, i.e. a short string describing its purpose. This can be
   * used e.g. in UIs which allow to enter the expression. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String title;

  /**
   * Optional. Description of the expression. This is a longer text which describes the expression,
   * e.g. when hovered over it in a UI.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getDescription() {
    return description;
  }

  /**
   * Optional. Description of the expression. This is a longer text which describes the expression,
   * e.g. when hovered over it in a UI.
   *
   * @param description description or {@code null} for none
   */
  public GoogleTypeExpr setDescription(java.lang.String description) {
    this.description = description;
    return this;
  }

  /**
   * Textual representation of an expression in Common Expression Language syntax.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getExpression() {
    return expression;
  }

  /**
   * Textual representation of an expression in Common Expression Language syntax.
   *
   * @param expression expression or {@code null} for none
   */
  public GoogleTypeExpr setExpression(java.lang.String expression) {
    this.expression = expression;
    return this;
  }

  /**
   * Optional. String indicating the location of the expression for error reporting, e.g. a file
   * name and a position in the file.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getLocation() {
    return location;
  }

  /**
   * Optional. String indicating the location of the expression for error reporting, e.g. a file
   * name and a position in the file.
   *
   * @param location location or {@code null} for none
   */
  public GoogleTypeExpr setLocation(java.lang.String location) {
    this.location = location;
    return this;
  }

  /**
   * Optional. Title for the expression, i.e. a short string describing its purpose. This can be
   * used e.g. in UIs which allow to enter the expression.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getTitle() {
    return title;
  }

  /**
   * Optional. Title for the expression, i.e. a short string describing its purpose. This can be
   * used e.g. in UIs which allow to enter the expression.
   *
   * @param title title or {@code null} for none
   */
  public GoogleTypeExpr setTitle(java.lang.String title) {
    this.title = title;
    return this;
  }

  @Override
  public GoogleTypeExpr set(String fieldName, Object value) {
    return (GoogleTypeExpr) super.set(fieldName, value);
  }

  @Override
  public GoogleTypeExpr clone() {
    return (GoogleTypeExpr) super.clone();
  }
}
