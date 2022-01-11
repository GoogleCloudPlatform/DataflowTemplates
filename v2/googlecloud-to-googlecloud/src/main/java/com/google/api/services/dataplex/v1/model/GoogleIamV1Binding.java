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
 * Associates members with a role.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleIamV1Binding extends com.google.api.client.json.GenericJson {

  /** The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String bindingId;

  /**
   * The condition that is associated with this binding.If the condition evaluates to true, then
   * this binding applies to the current request.If the condition evaluates to false, then this
   * binding does not apply to the current request. However, a different role binding might grant
   * the same role to one or more of the members in this binding.To learn which resources support
   * conditions in their IAM policies, see the IAM documentation
   * (https://cloud.google.com/iam/help/conditions/resource-policies). The value may be {@code
   * null}.
   */
  @com.google.api.client.util.Key private GoogleTypeExpr condition;

  /**
   * Specifies the identities requesting access for a Cloud Platform resource. members can have the
   * following values: allUsers: A special identifier that represents anyone who is on the internet;
   * with or without a Google account. allAuthenticatedUsers: A special identifier that represents
   * anyone who is authenticated with a Google account or a service account. user:{emailid}: An
   * email address that represents a specific Google account. For example, alice@example.com .
   * serviceAccount:{emailid}: An email address that represents a service account. For example, my-
   * other-app@appspot.gserviceaccount.com. group:{emailid}: An email address that represents a
   * Google group. For example, admins@example.com. deleted:user:{emailid}?uid={uniqueid}: An email
   * address (plus unique identifier) representing a user that has been recently deleted. For
   * example, alice@example.com?uid=123456789012345678901. If the user is recovered, this value
   * reverts to user:{emailid} and the recovered user retains the role in the binding.
   * deleted:serviceAccount:{emailid}?uid={uniqueid}: An email address (plus unique identifier)
   * representing a service account that has been recently deleted. For example, my-other-
   * app@appspot.gserviceaccount.com?uid=123456789012345678901. If the service account is undeleted,
   * this value reverts to serviceAccount:{emailid} and the undeleted service account retains the
   * role in the binding. deleted:group:{emailid}?uid={uniqueid}: An email address (plus unique
   * identifier) representing a Google group that has been recently deleted. For example,
   * admins@example.com?uid=123456789012345678901. If the group is recovered, this value reverts to
   * group:{emailid} and the recovered group retains the role in the binding. domain:{domain}: The G
   * Suite domain (primary) that represents all the users of that domain. For example, google.com or
   * example.com. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.util.List<java.lang.String> members;

  /**
   * Role that is assigned to members. For example, roles/viewer, roles/editor, or roles/owner. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String role;

  /** @return value or {@code null} for none */
  public java.lang.String getBindingId() {
    return bindingId;
  }

  /** @param bindingId bindingId or {@code null} for none */
  public GoogleIamV1Binding setBindingId(java.lang.String bindingId) {
    this.bindingId = bindingId;
    return this;
  }

  /**
   * The condition that is associated with this binding.If the condition evaluates to true, then
   * this binding applies to the current request.If the condition evaluates to false, then this
   * binding does not apply to the current request. However, a different role binding might grant
   * the same role to one or more of the members in this binding.To learn which resources support
   * conditions in their IAM policies, see the IAM documentation
   * (https://cloud.google.com/iam/help/conditions/resource-policies).
   *
   * @return value or {@code null} for none
   */
  public GoogleTypeExpr getCondition() {
    return condition;
  }

  /**
   * The condition that is associated with this binding.If the condition evaluates to true, then
   * this binding applies to the current request.If the condition evaluates to false, then this
   * binding does not apply to the current request. However, a different role binding might grant
   * the same role to one or more of the members in this binding.To learn which resources support
   * conditions in their IAM policies, see the IAM documentation
   * (https://cloud.google.com/iam/help/conditions/resource-policies).
   *
   * @param condition condition or {@code null} for none
   */
  public GoogleIamV1Binding setCondition(GoogleTypeExpr condition) {
    this.condition = condition;
    return this;
  }

  /**
   * Specifies the identities requesting access for a Cloud Platform resource. members can have the
   * following values: allUsers: A special identifier that represents anyone who is on the internet;
   * with or without a Google account. allAuthenticatedUsers: A special identifier that represents
   * anyone who is authenticated with a Google account or a service account. user:{emailid}: An
   * email address that represents a specific Google account. For example, alice@example.com .
   * serviceAccount:{emailid}: An email address that represents a service account. For example, my-
   * other-app@appspot.gserviceaccount.com. group:{emailid}: An email address that represents a
   * Google group. For example, admins@example.com. deleted:user:{emailid}?uid={uniqueid}: An email
   * address (plus unique identifier) representing a user that has been recently deleted. For
   * example, alice@example.com?uid=123456789012345678901. If the user is recovered, this value
   * reverts to user:{emailid} and the recovered user retains the role in the binding.
   * deleted:serviceAccount:{emailid}?uid={uniqueid}: An email address (plus unique identifier)
   * representing a service account that has been recently deleted. For example, my-other-
   * app@appspot.gserviceaccount.com?uid=123456789012345678901. If the service account is undeleted,
   * this value reverts to serviceAccount:{emailid} and the undeleted service account retains the
   * role in the binding. deleted:group:{emailid}?uid={uniqueid}: An email address (plus unique
   * identifier) representing a Google group that has been recently deleted. For example,
   * admins@example.com?uid=123456789012345678901. If the group is recovered, this value reverts to
   * group:{emailid} and the recovered group retains the role in the binding. domain:{domain}: The G
   * Suite domain (primary) that represents all the users of that domain. For example, google.com or
   * example.com.
   *
   * @return value or {@code null} for none
   */
  public java.util.List<java.lang.String> getMembers() {
    return members;
  }

  /**
   * Specifies the identities requesting access for a Cloud Platform resource. members can have the
   * following values: allUsers: A special identifier that represents anyone who is on the internet;
   * with or without a Google account. allAuthenticatedUsers: A special identifier that represents
   * anyone who is authenticated with a Google account or a service account. user:{emailid}: An
   * email address that represents a specific Google account. For example, alice@example.com .
   * serviceAccount:{emailid}: An email address that represents a service account. For example, my-
   * other-app@appspot.gserviceaccount.com. group:{emailid}: An email address that represents a
   * Google group. For example, admins@example.com. deleted:user:{emailid}?uid={uniqueid}: An email
   * address (plus unique identifier) representing a user that has been recently deleted. For
   * example, alice@example.com?uid=123456789012345678901. If the user is recovered, this value
   * reverts to user:{emailid} and the recovered user retains the role in the binding.
   * deleted:serviceAccount:{emailid}?uid={uniqueid}: An email address (plus unique identifier)
   * representing a service account that has been recently deleted. For example, my-other-
   * app@appspot.gserviceaccount.com?uid=123456789012345678901. If the service account is undeleted,
   * this value reverts to serviceAccount:{emailid} and the undeleted service account retains the
   * role in the binding. deleted:group:{emailid}?uid={uniqueid}: An email address (plus unique
   * identifier) representing a Google group that has been recently deleted. For example,
   * admins@example.com?uid=123456789012345678901. If the group is recovered, this value reverts to
   * group:{emailid} and the recovered group retains the role in the binding. domain:{domain}: The G
   * Suite domain (primary) that represents all the users of that domain. For example, google.com or
   * example.com.
   *
   * @param members members or {@code null} for none
   */
  public GoogleIamV1Binding setMembers(java.util.List<java.lang.String> members) {
    this.members = members;
    return this;
  }

  /**
   * Role that is assigned to members. For example, roles/viewer, roles/editor, or roles/owner.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getRole() {
    return role;
  }

  /**
   * Role that is assigned to members. For example, roles/viewer, roles/editor, or roles/owner.
   *
   * @param role role or {@code null} for none
   */
  public GoogleIamV1Binding setRole(java.lang.String role) {
    this.role = role;
    return this;
  }

  @Override
  public GoogleIamV1Binding set(String fieldName, Object value) {
    return (GoogleIamV1Binding) super.set(fieldName, value);
  }

  @Override
  public GoogleIamV1Binding clone() {
    return (GoogleIamV1Binding) super.clone();
  }
}
