/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import javax.net.ssl.SSLContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SslContextFactoryTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testCreateSslContextWithInvalidPath() {
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> SslContextFactory.createSslContext("invalid/path", "password", null, null));
    assertThat(exception).hasMessageThat().isEqualTo("Cannot create SSL context");
  }

  @Test
  public void testCreateSslContextWithTrustStore() throws IOException, InterruptedException {
    File keystoreFile = new File(folder.getRoot(), "test.keystore");
    generateKeystore(keystoreFile);
    File truststoreFile = new File(folder.getRoot(), "test.truststore");
    generateTruststore(keystoreFile, truststoreFile);

    SSLContext sslContext =
        SslContextFactory.createSslContext(
            truststoreFile.getAbsolutePath(), "password", null, null);
    assertThat(sslContext).isNotNull();
  }

  @Test
  public void testCreateSslContextWithKeyStoreOnly() throws IOException, InterruptedException {
    File keystoreFile = new File(folder.getRoot(), "test.keystore");
    generateKeystore(keystoreFile);

    SSLContext sslContext =
        SslContextFactory.createSslContext(null, null, keystoreFile.getAbsolutePath(), "password");
    assertThat(sslContext).isNotNull();
  }

  @Test
  public void testCreateSslContextWithKeyStoreAndTrustStore()
      throws IOException, InterruptedException {
    File keystoreFile = new File(folder.getRoot(), "test.keystore");
    generateKeystore(keystoreFile);
    File truststoreFile = new File(folder.getRoot(), "test.truststore");
    generateTruststore(keystoreFile, truststoreFile);

    SSLContext sslContext =
        SslContextFactory.createSslContext(
            truststoreFile.getAbsolutePath(),
            "password",
            keystoreFile.getAbsolutePath(),
            "password");
    assertThat(sslContext).isNotNull();
  }

  private void generateKeystore(File keystoreFile) throws IOException, InterruptedException {
    runCommand(
        "keytool",
        "-genkey",
        "-alias",
        "test",
        "-keystore",
        keystoreFile.getAbsolutePath(),
        "-storepass",
        "password",
        "-keypass",
        "password",
        "-dname",
        "CN=Test, OU=Test, O=Test, L=Test, S=Test, C=US",
        "-keyalg",
        "RSA",
        "-validity",
        "365");
  }

  private void generateTruststore(File keystoreFile, File truststoreFile)
      throws IOException, InterruptedException {
    File certFile = new File(folder.getRoot(), "test.crt");
    runCommand(
        "keytool",
        "-export",
        "-alias",
        "test",
        "-keystore",
        keystoreFile.getAbsolutePath(),
        "-file",
        certFile.getAbsolutePath(),
        "-storepass",
        "password");
    runCommand(
        "keytool",
        "-import",
        "-alias",
        "test",
        "-file",
        certFile.getAbsolutePath(),
        "-keystore",
        truststoreFile.getAbsolutePath(),
        "-storepass",
        "password",
        "-noprompt");
  }

  private void runCommand(String... command) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(command);
    Process p = pb.start();
    int exitCode = p.waitFor();
    if (exitCode != 0) {
      throw new IOException("Command failed with exit code " + exitCode);
    }
  }
}
