/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.gcp.kms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link KMSResourceManager}. */
@RunWith(JUnit4.class)
public class KMSResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  private static final String PROJECT_ID = "test-project";
  private static final String REGION = "us-central1";
  private static final String KEYRING_ID = "test-keyring";
  private static final String KEY_ID = "test-key";

  @Mock private KMSClientFactory kmsClientFactory;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private KeyManagementServiceClient serviceClient;

  private KMSResourceManager testManager;

  @Before
  public void setUp() {
    testManager =
        new KMSResourceManager(
            kmsClientFactory, KMSResourceManager.builder(PROJECT_ID).setRegion(REGION));
  }

  @Test
  public void testGetOrCreateCryptoKeyShouldThrowErrorWhenClientFailsToConnect() {
    when(kmsClientFactory.getKMSClient()).thenThrow(KMSResourceManagerException.class);

    assertThrows(
        KMSResourceManagerException.class,
        () -> testManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID));
  }

  @Test
  public void testGetOrCreateCryptoKeyShouldCreateKeyRingWhenItDoesNotExist() {
    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.listKeyRings(any(LocationName.class)).iterateAll()).thenReturn(List.of());

    testManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID);
    verify(serviceClient).createKeyRing(any(LocationName.class), anyString(), any(KeyRing.class));
  }

  @Test
  public void testGetOrCreateCryptoKeyShouldNotCreateKeyRingWhenItAlreadyExists() {
    KeyRing keyRingMock = mock(KeyRing.class);

    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.listKeyRings(any(LocationName.class)).iterateAll())
        .thenReturn(List.of(keyRingMock));
    when(keyRingMock.getName())
        .thenReturn(KeyRingName.of(PROJECT_ID, REGION, KEYRING_ID).toString());

    testManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID);
    verify(serviceClient, never())
        .createKeyRing(any(LocationName.class), anyString(), any(KeyRing.class));
  }

  @Test
  public void testGetOrCreateCryptoKeyShouldCreateCryptoKeyWhenItDoesNotExist() {
    KeyRing keyRingMock = mock(KeyRing.class);

    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.createKeyRing(any(LocationName.class), anyString(), any(KeyRing.class)))
        .thenReturn(keyRingMock);
    when(keyRingMock.getName())
        .thenReturn(KeyRingName.of(PROJECT_ID, REGION, KEYRING_ID).toString());
    when(serviceClient.listCryptoKeys(KEYRING_ID).iterateAll()).thenReturn(List.of());

    testManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID);
    verify(serviceClient).createCryptoKey(anyString(), anyString(), any(CryptoKey.class));
  }

  @Test
  public void testGetOrCreateCryptoKeyShouldNotCreateCryptoKeyWhenItAlreadyExists() {
    KeyRing keyRingMock = mock(KeyRing.class);
    CryptoKey keyMock = mock(CryptoKey.class);
    String keyRingName = KeyRingName.of(PROJECT_ID, REGION, KEYRING_ID).toString();

    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.createKeyRing(any(LocationName.class), anyString(), any(KeyRing.class)))
        .thenReturn(keyRingMock);
    when(keyRingMock.getName()).thenReturn(keyRingName);
    when(serviceClient.listCryptoKeys(keyRingName).iterateAll()).thenReturn(List.of(keyMock));
    when(keyMock.getName())
        .thenReturn(CryptoKeyName.of(PROJECT_ID, REGION, KEYRING_ID, KEY_ID).toString());

    testManager.getOrCreateCryptoKey(KEYRING_ID, KEY_ID);
    verify(serviceClient, never()).createCryptoKey(anyString(), anyString(), any(CryptoKey.class));
  }

  @Test
  public void testEncryptShouldThrowErrorWhenClientFailsToConnect() {
    when(kmsClientFactory.getKMSClient()).thenThrow(KMSResourceManagerException.class);

    assertThrows(
        KMSResourceManagerException.class,
        () -> testManager.encrypt(KEYRING_ID, KEY_ID, "test message"));
  }

  @Test
  public void testEncryptShouldEncodeEncryptedMessageWithBase64() {
    EncryptResponse encryptedResponse = mock(EncryptResponse.class, Answers.RETURNS_DEEP_STUBS);
    String ciphertext = "ciphertext";

    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.encrypt(any(CryptoKeyName.class), any(ByteString.class)))
        .thenReturn(encryptedResponse);
    when(encryptedResponse.getCiphertext().toByteArray()).thenReturn(ciphertext.getBytes());

    String encryptedMessage = testManager.encrypt(KEYRING_ID, KEY_ID, "test message");
    String actual = new String(Base64.getDecoder().decode(encryptedMessage.getBytes()));

    assertThat(actual).isEqualTo(ciphertext);
  }

  @Test
  public void testDecryptShouldThrowErrorWhenClientFailsToConnect() {
    when(kmsClientFactory.getKMSClient()).thenThrow(KMSResourceManagerException.class);

    assertThrows(
        KMSResourceManagerException.class,
        () -> testManager.decrypt(KEYRING_ID, KEY_ID, "ciphertext"));
  }

  @Test
  public void testDecryptShouldEncodeEncryptedMessageWithUTF8() {
    DecryptResponse decryptedResponse = mock(DecryptResponse.class, Answers.RETURNS_DEEP_STUBS);
    String ciphertext = "ciphertext";
    String base64EncodedCiphertext = new String(Base64.getEncoder().encode(ciphertext.getBytes()));

    when(kmsClientFactory.getKMSClient()).thenReturn(serviceClient);
    when(serviceClient.decrypt(any(CryptoKeyName.class), any(ByteString.class)))
        .thenReturn(decryptedResponse);
    when(decryptedResponse.getPlaintext().toStringUtf8()).thenReturn(ciphertext);

    String actual = testManager.decrypt(KEYRING_ID, KEY_ID, base64EncodedCiphertext);

    verify(serviceClient)
        .decrypt(any(CryptoKeyName.class), eq(ByteString.copyFrom(ciphertext.getBytes())));
    assertThat(actual).isEqualTo(ciphertext);
  }
}
