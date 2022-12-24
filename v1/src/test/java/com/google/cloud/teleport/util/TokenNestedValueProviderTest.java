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

import com.google.cloud.teleport.templates.common.SplunkTokenSource;
import org.apache.beam.sdk.options.ValueProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TokenNestedValueProvider}. */
@RunWith(JUnit4.class)
public class TokenNestedValueProviderTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  /**
   * Test that {@link TokenNestedValueProvider#resolveTokenSource()} correctly resolves the token
   * source as {@code SplunkTokenSource.KMS} when a token and KMS Key are provided.
   */
  @Test
  public void testResolveTokenSource_kms() {
    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/locations/test-region/keyRings/test-key-ring/cryptoKeys/test-key"),
            ValueProvider.StaticValueProvider.of("test-token"),
            ValueProvider.StaticValueProvider.of(null));

    SplunkTokenSource tokenSource = tokenProvider.resolveTokenSource();

    assertEquals(SplunkTokenSource.KMS, tokenSource);
  }

  /**
   * Test that {@link TokenNestedValueProvider#resolveTokenSource()} correctly resolves the token
   * source as {@code SplunkTokenSource.PLAINTEXT} when only a plaintext token is provided.
   */
  @Test
  public void testResolveTokenSource_plaintext() {
    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of("test-token"),
            ValueProvider.StaticValueProvider.of(null));

    SplunkTokenSource tokenSource = tokenProvider.resolveTokenSource();

    assertEquals(SplunkTokenSource.PLAINTEXT, tokenSource);
  }

  /**
   * Test that {@link TokenNestedValueProvider#resolveTokenSource()} fails when a Secret Manager
   * Secret ID is passed without a token source.
   */
  @Test
  public void testResolveTokenSource_secretManager() {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Could not resolve tokenSource from given parameters. Pass in a tokenSource parameter with"
            + " value one of SECRET_MANAGER, KMS or PLAINTEXT.");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/secrets/test-secret/versions/test-version"),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null));

    tokenProvider.resolveTokenSource();
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} returns an instance of
   * {@link SecretManagerValueProvider} when a Secret ID and 'SECRET_MANAGER' token source are
   * provided.
   */
  @Test
  public void testGetToken_secretManager() {
    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("SECRET_MANAGER");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/secrets/test-secret/versions/test-version"),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            tokenSource);

    ValueProvider<String> finalToken =
        tokenProvider.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
    assertTrue(finalToken instanceof SecretManagerValueProvider);
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} fails when the token
   * source is 'SECRET_MANAGER' and no Secret ID is provided.
   */
  @Test
  public void testGetToken_secretManagerInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "tokenSecretId is required to retrieve token from Secret Manager");

    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("SECRET_MANAGER");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            tokenSource);

    tokenProvider.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} returns an instance of
   * {@link KMSEncryptedNestedValueProvider} when a KMS Key, encrypted token and 'KMS' token source
   * are provided.
   */
  @Test
  public void testGetToken_kms() {
    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("KMS");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(
                "projects/test-project/locations/test-region/keyRings/test-key-ring/cryptoKeys/test-key"),
            ValueProvider.StaticValueProvider.of("test-token"),
            tokenSource);

    ValueProvider<String> finalToken =
        tokenProvider.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
    assertTrue(finalToken instanceof KMSEncryptedNestedValueProvider);
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} fails when the token
   * source is 'KMS' but no KMS Key or encrypted token params are provided.
   */
  @Test
  public void testGetToken_kmsInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "token and tokenKmsEncryptionKey are required while decrypting using KMS Key");

    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("KMS");

    TokenNestedValueProvider token =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            tokenSource);

    token.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} returns a plaintext
   * token when the token source is 'PLAINTEXT'.
   */
  @Test
  public void testGetToken_plaintext() {
    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("PLAINTEXT");
    ValueProvider<String> expectedToken = ValueProvider.StaticValueProvider.of("test-token");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            expectedToken,
            tokenSource);

    ValueProvider<String> actualToken =
        tokenProvider.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
    assertEquals(expectedToken.get(), actualToken.get());
  }

  /**
   * Test that {@link TokenNestedValueProvider#getToken(SplunkTokenSource)} fails when the token
   * source is 'PLAINTEXT' but no plaintext token is provided.
   */
  @Test
  public void testGetToken_plaintextInvalidParams() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("token is required for writing events");

    ValueProvider<String> tokenSource = ValueProvider.StaticValueProvider.of("PLAINTEXT");

    TokenNestedValueProvider tokenProvider =
        new TokenNestedValueProvider(
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            ValueProvider.StaticValueProvider.of(null),
            tokenSource);

    tokenProvider.getToken(SplunkTokenSource.valueOf(tokenSource.get()));
  }
}
