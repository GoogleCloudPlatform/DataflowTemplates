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
package com.google.cloud.teleport.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * {@link SecretManagerValueProvider} class is a subclass of {@link ValueProvider} that takes in a
 * {@link ValueProvider}&lt;String&gt; of the form
 * projects/{project}/secrets/{secret}/versions/{secret_version} and returns the secret value in
 * Secret Manager.
 */
public class SecretManagerValueProvider implements ValueProvider<String>, Serializable {

  private transient volatile String cachedValue;
  private final SerializableFunction<String, String> translator;
  private final ValueProvider<String> secretVersion;

  @Override
  public String get() {
    if (cachedValue == null) {
      cachedValue = translator.apply(secretVersion.get());
    }
    return cachedValue;
  }

  @Override
  public boolean isAccessible() {
    return secretVersion.isAccessible();
  }

  private static class SecretTranslatorInput implements SerializableFunction<String, String> {

    private SecretTranslatorInput() {}

    public static SecretTranslatorInput of() {
      return new SecretTranslatorInput();
    }

    @Override
    public String apply(String secretVersion) {
      SecretVersionName secretVersionName = parseSecretVersion(secretVersion);

      try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
        AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
        return response.getPayload().getData().toStringUtf8();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Parses a Secret Version and returns a {@link SecretVersionName}.
     *
     * @param secretVersion Secret Version of the form
     *     projects/{project}/secrets/{secret}/versions/{secret_version}
     * @return {@link SecretVersionName}
     */
    private SecretVersionName parseSecretVersion(String secretVersion) {
      if (SecretVersionName.isParsableFrom(secretVersion)) {
        return SecretVersionName.parse(secretVersion);
      } else {
        throw new IllegalArgumentException(
            "Provided Secret must be in the form"
                + " projects/{project}/secrets/{secret}/versions/{secret_version}");
      }
    }
  }

  public SecretManagerValueProvider(ValueProvider<String> secretVersion) {
    this.secretVersion = secretVersion;
    this.translator = SecretTranslatorInput.of();
  }
}
