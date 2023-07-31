/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.secretmanager;

import org.apache.beam.it.common.ResourceManager;

/** Interface for managing GCP Secret manager resources in integration tests. */
public interface SecretManagerResourceManager extends ResourceManager {

  /**
   * Creates a Secret with given name on GCP Secret Manager.
   *
   * @param secretId Secret ID of the secret to be created
   * @param secretData Value of the secret to be added
   */
  void createSecret(String secretId, String secretData);

  /**
   * Creates a new SecretVersion containing secret data and attaches it to an existing Secret.
   *
   * @param parent Parent secret to which version will be added
   * @param payload Value of the secret to be added
   */
  void addSecretVersion(String secretId, String payload);

  /**
   * Calls Secret Manager with a Secret Version and returns the secret value.
   *
   * @param secretVersion Secret Version of the form
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   * @return the secret value in Secret Manager
   */
  String accessSecret(String secretVersion);

  /**
   * Enables a SecretVersion. Sets the state of the SecretVersion to ENABLED.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  void enableSecretVersion(String secretVersion);

  /**
   * Disables a SecretVersion. Sets the state of the SecretVersion to DISABLED.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  void disableSecretVersion(String secretVersion);

  /**
   * Sets the state of the SecretVersion to DESTROYED and irrevocably destroys the secret data.
   *
   * @param secretVersion The resource name of the SecretVersion to destroy in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  void destroySecretVersion(String secretVersion);

  /**
   * Deletes a Secret.
   *
   * @param secretId The resource name of the SecretVersion to delete in the format of
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   */
  void deleteSecret(String secretId);
}
