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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import com.google.cloud.teleport.templates.common.DatadogApiKeySource;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DatadogApiKeyNestedValueProvider}. */
@RunWith(JUnit4.class)
public class DatadogApiKeyNestedValueProviderTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#resolveApiKeySource()} correctly resolves the
   * apiKey source as {@code DatadogApiKeySource.KMS} when a apiKey and KMS Key are provided.
   */
  @Test
  public void testResolveApiKeySource_kms() {
    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/locations/test-region/keyRings/test-key-ring/cryptoKeys/test-key"),
            ValueProvider.StaticValueProvider.of("test-apiKey"),
            ValueProvider.StaticValueProvider.of(null));

    DatadogApiKeySource apiKeySource = apiKeyProvider.resolveApiKeySource();

    assertEquals(DatadogApiKeySource.KMS, apiKeySource);
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#resolveApiKeySource()} correctly resolves the
   * apiKey source as {@code DatadogApiKeySource.PLAINTEXT} when only a plaintext apiKey is
   * provided.
   */
  @Test
  public void testResolveApiKeySource_plaintext() {
    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of("test-apiKey"),
            ValueProvider.StaticValueProvider.of(null));

    DatadogApiKeySource apiKeySource = apiKeyProvider.resolveApiKeySource();

    assertEquals(DatadogApiKeySource.PLAINTEXT, apiKeySource);
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#resolveApiKeySource()} fails when a Secret
   * Manager Secret ID is passed without a apiKey source.
   */
  @Test
  public void testResolveApiKeySource_secretManager() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Could not resolve apiKeySource from given parameters. Pass in a apiKeySource parameter with"
            + " value one of SECRET_MANAGER, KMS or PLAINTEXT.");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/secrets/test-secret/versions/test-version"),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null));

    apiKeyProvider.resolveApiKeySource();
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} returns an
   * instance of {@link SecretManagerValueProvider} when a Secret ID and 'SECRET_MANAGER' apiKey
   * source are provided.
   */
  @Test
  public void testGetApiKey_secretManager() {
    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("SECRET_MANAGER");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/secrets/test-secret/versions/test-version"),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            apiKeySource);

    ValueProvider<String> finalApiKey =
        apiKeyProvider.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
    assertTrue(finalApiKey instanceof SecretManagerValueProvider);
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} fails when
   * the apiKey source is 'SECRET_MANAGER' and no Secret ID is provided.
   */
  @Test
  public void testGetApiKey_secretManagerInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "apiKeySecretId is required to retrieve apiKey from Secret Manager");

    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("SECRET_MANAGER");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            apiKeySource);

    apiKeyProvider.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} returns an
   * instance of {@link KMSEncryptedNestedValueProvider} when a KMS Key, encrypted apiKey and 'KMS'
   * apiKey source are provided.
   */
  @Test
  public void testGetApiKey_kms() {
    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("KMS");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/locations/test-region/keyRings/test-key-ring/cryptoKeys/test-key"),
            ValueProvider.StaticValueProvider.of("test-apiKey"),
            apiKeySource);

    ValueProvider<String> finalApiKey =
        apiKeyProvider.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
    assertTrue(finalApiKey instanceof KMSEncryptedNestedValueProvider);
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} fails when
   * the apiKey source is 'KMS' but no KMS Key or encrypted apiKey params are provided.
   */
  @Test
  public void testGetApiKey_kmsInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "apiKey and apiKeyKmsEncryptionKey are required while decrypting using KMS Key");

    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("KMS");

    DatadogApiKeyNestedValueProvider apiKey =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            apiKeySource);

    apiKey.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} returns a
   * plaintext apiKey when the apiKey source is 'PLAINTEXT'.
   */
  @Test
  public void testGetApiKey_plaintext() {
    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("PLAINTEXT");
    ValueProvider<String> expectedApiKey = ValueProvider.StaticValueProvider.of("test-apiKey");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            expectedApiKey,
            apiKeySource);

    ValueProvider<String> actualApiKey =
        apiKeyProvider.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
    assertEquals(expectedApiKey.get(), actualApiKey.get());
  }

  /**
   * Test that {@link DatadogApiKeyNestedValueProvider#getApiKey(DatadogApiKeySource)} fails when
   * the apiKey source is 'PLAINTEXT' but no plaintext apiKey is provided.
   */
  @Test
  public void testGetApiKey_plaintextInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("apiKey is required for writing events");

    ValueProvider<String> apiKeySource = ValueProvider.StaticValueProvider.of("PLAINTEXT");

    DatadogApiKeyNestedValueProvider apiKeyProvider =
        new DatadogApiKeyNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            apiKeySource);

    apiKeyProvider.getApiKey(DatadogApiKeySource.valueOf(apiKeySource.get()));
  }
}
