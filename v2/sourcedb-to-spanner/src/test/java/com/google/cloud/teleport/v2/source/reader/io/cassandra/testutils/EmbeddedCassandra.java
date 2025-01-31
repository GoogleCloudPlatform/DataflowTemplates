/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;
import com.github.nosan.embedded.cassandra.commons.ClassPathResource;
import com.github.nosan.embedded.cassandra.cql.CqlScript;
import com.google.common.collect.ImmutableList;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Utility Class to start and stop Embedded Cassandra. {@link Cassandra Embedded Cassandra} is
 * equivalent to real cassandra at the level of network protocol. So using this over mocks wherever
 * possible gives us much better test coverage. Note: Prefer using {@link SharedEmbeddedCassandra}
 * to share an instance of Embedded Cassandra.
 *
 * <p>Note on SSL Mode:
 *
 * <p>When the test Cassandra Server has to run with SSL enabled, it needs to present an SSL
 * certificate to the client, which the client can verify. In a UT environment, we won't have a
 * certificate authority that will sign the certificates. For this, We can either check in a private
 * Key and Cert to the repo itself which is used in UT, which is less ideal, or, We can generate a
 * temporary random key and certificate which would be used by the server and trusted by the client
 * in a UT setting. We are taking the later route in order to avoid having to check in keys and
 * certificates to the repo.
 */
public class EmbeddedCassandra implements AutoCloseable {
  private Cassandra embeddedCassandra;
  private String clusterName;
  private ImmutableList<InetSocketAddress> contactPoints;
  private final Settings settings;
  private static final String LOCAL_DATA_CENTER = "datacenter1";

  /** Temporary file for storing the certificate key. */
  private java.io.File keyStoreFile = null;

  /** Temporary file for storing the certificate. */
  private java.io.File trustStoreFile = null;

  public EmbeddedCassandra(String config, @Nullable String cqlResource, boolean clientEncryption)
      throws IOException {
    var builder =
        new CassandraBuilder()
            .addEnvironmentVariable("JAVA_HOME", System.getProperty("java.home"))
            .addEnvironmentVariable("JRE_HOME", System.getProperty("jre.home"))
            // Check [CASSANDRA-13396](https://issues.apache.org/jira/browse/CASSANDRA-13396)
            .addSystemProperty("cassandra.insecure.udf", "true")
            .configFile(new ClassPathResource(config))
            // Choose from available ports on the test machine.
            .addConfigProperty("native_transport_port", 0)
            .addConfigProperty("storage_port", 0)
            .addSystemProperty("cassandra.jmx.local.port", 0)
            .registerShutdownHook(true);
    if (clientEncryption) {

      // Generate temporary keystore and truststore files
      keyStoreFile = java.io.File.createTempFile("client", ".keystore");
      trustStoreFile = java.io.File.createTempFile("client", ".truststore");
      builder =
          builder
              .addConfigProperty("client_encryption_options.enabled", true)
              .addConfigProperty("client_encryption_options.optional", true)
              .addConfigProperty(
                  "client_encryption_options.keystore", keyStoreFile.getAbsolutePath());
      createTemporaryKeyStore(keyStoreFile, trustStoreFile);
    }
    // Ref: https://stackoverflow.com/questions/78195798/embedded-cassandra-not-working-in-java-21
    if (Runtime.version().compareTo(Runtime.Version.parse("12")) >= 0) {
      builder = builder.addSystemProperty("java.security.manager", "allow");
    }
    /*
    * TODO (vardhanvthigle): Get EmbeddedCassandra 4.0 working with our UT JVM.
    // If we spawn Cassandra 4.0.0 for testing, it tries to set biased locking, which is not recognized by some JVMs.
    builder = builder.addJvmOptions("-XX:+IgnoreUnrecognizedVMOptions");
    // This is needed as Cassandra 4.0 goes for deep reflections for java packages.
    builder = builder.addEnvironmentVariable("JDK_JAVA_OPTIONS", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED"
        + "--add-opens java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED");
    builder = builder.version("4.0.15");
    */
    embeddedCassandra = builder.build();
    embeddedCassandra.start();
    settings = embeddedCassandra.getSettings();
    clusterName = (String) settings.getConfigProperties().get("cluster_name");
    contactPoints =
        ImmutableList.of(new InetSocketAddress(settings.getAddress(), settings.getPort()));
    if (StringUtils.isNotBlank(cqlResource)) {
      try (CqlSession session =
          CqlSession.builder()
              .addContactPoint(new InetSocketAddress(settings.getAddress(), settings.getPort()))
              .withLocalDatacenter(LOCAL_DATA_CENTER)
              .build()) {
        CqlScript.ofClassPath(cqlResource).forEachStatement(session::execute);
      }
    }
  }

  /** Generate a Random KeyPair for Signing the SSL certificate in UT environment. */
  private static KeyPair generateTestKeyPair() throws NoSuchAlgorithmException {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    return keyPairGenerator.generateKeyPair();
  }

  /** Generate a random Key Pair and a Self Signed Certificate for the UT environment. */
  private static void createTemporaryKeyStore(
      java.io.File keyStoreFile, java.io.File trustStoreFile) {
    Security.addProvider(new BouncyCastleProvider());

    try {
      // Generate KeyPair
      KeyPair keyPair = generateTestKeyPair();

      // Generate Certificate
      X509Certificate certificate = generateTestCertificate(keyPair);

      // Create and save keystore
      createKeyStore(keyStoreFile, keyPair, certificate, "cassandra".toCharArray());

      // Create and save truststore
      createTrustStore(trustStoreFile, certificate, "cassandra".toCharArray());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void createKeyStore(
      java.io.File keyStoreFile, KeyPair keyPair, X509Certificate certificate, char[] password)
      throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "client",
        keyPair.getPrivate(),
        password,
        new java.security.cert.Certificate[] {certificate});
    try (FileOutputStream fos = new FileOutputStream(keyStoreFile)) {
      keyStore.store(fos, password);
    }
  }

  private static void createTrustStore(
      java.io.File trustStoreFile, X509Certificate certificate, char[] password) throws Exception {
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("localhost", certificate);
    try (FileOutputStream fos = new FileOutputStream(trustStoreFile)) {
      trustStore.store(fos, password);
    }
  }

  /** Generate a selfsigned test certificate. */
  private static X509Certificate generateTestCertificate(KeyPair keyPair) throws Exception {
    // Prepare necessary information
    X500Name issuer = new X500Name("CN=localhost");
    BigInteger serial = new BigInteger(160, new SecureRandom());
    Date notBefore = new Date();
    Date notAfter = new Date(notBefore.getTime() + 365 * 24 * 60 * 60 * 1000L); // 1 year validity
    X500Name subject = issuer;
    SubjectPublicKeyInfo publicKeyInfo =
        SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());

    // Create certificate builder
    X509v3CertificateBuilder certBuilder =
        new X509v3CertificateBuilder(issuer, serial, notBefore, notAfter, subject, publicKeyInfo);

    // Add Basic Constraints (optional, for CA certificates)
    certBuilder.addExtension(
        org.bouncycastle.asn1.x509.Extension.basicConstraints, true, new BasicConstraints(true));

    // Create content signer
    ContentSigner contentSigner =
        new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());

    // Build the certificate holder
    X509CertificateHolder certHolder = certBuilder.build(contentSigner);

    // Convert to X509Certificate
    return new JcaX509CertificateConverter().getCertificate(certHolder);
  }

  public Cassandra getEmbeddedCassandra() {
    return embeddedCassandra;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public Settings getSettings() {
    return this.settings;
  }

  public String getLocalDataCenter() {
    return LOCAL_DATA_CENTER;
  }

  public ImmutableList<InetSocketAddress> getContactPoints() {
    return this.contactPoints;
  }

  public Path getKeyStorePath() {
    return this.keyStoreFile.toPath();
  }

  public Path getTrustStorePath() {
    return this.trustStoreFile.toPath();
  }

  @Override
  public void close() throws Exception {
    if (embeddedCassandra != null) {
      embeddedCassandra.stop();
    }

    if (keyStoreFile != null && keyStoreFile.exists()) {
      keyStoreFile.delete();
      keyStoreFile = null;
    }
    if (trustStoreFile != null && trustStoreFile.exists()) {
      trustStoreFile.delete();
      trustStoreFile = null;
    }
  }
}
