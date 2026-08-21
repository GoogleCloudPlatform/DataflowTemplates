/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.security.KeyStore;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

/**
 * A wrapper over {@link CassandraContainer} that configures and starts a Cassandra instance with
 * SSL/TLS encryption enabled. It automatically generates a self-signed certificate, keystore, and
 * truststore, and injects them into the container's configuration.
 */
public class CassandraSslContainerWrapper
    extends TestContainerResourceManager<GenericContainer<?>> {

  private final CqlSession session;
  private final File trustStoreFile;
  private final String keyspaceName;

  private CassandraSslContainerWrapper(
      Builder builder, CassandraContainer container, File trustStoreFile, String password)
      throws Exception {
    super(container, builder);
    this.keyspaceName = builder.keyspaceName;
    this.trustStoreFile = trustStoreFile;

    // Load the generated truststore to establish an SSL connection with the container
    KeyStore trustStore = KeyStore.getInstance("JKS");
    try (FileInputStream trustStoreFileStream = new FileInputStream(trustStoreFile)) {
      trustStore.load(trustStoreFileStream, password.toCharArray());
    }
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);

    // Wait for the Cassandra node to become available and establish the CQL session
    CqlSession tempSession = null;
    long timeout = System.currentTimeMillis() + 60000;
    while (true) {
      try {
        tempSession =
            CqlSession.builder()
                .addContactPoint(new InetSocketAddress(this.getHost(), this.getPort(9042)))
                .withLocalDatacenter("datacenter1")
                .withSslContext(sslContext)
                .build();
        break;
      } catch (Exception e) {
        if (System.currentTimeMillis() > timeout) {
          throw new RuntimeException("Failed to connect to Cassandra with SSL", e);
        }
        Thread.sleep(2000);
      }
    }
    session = tempSession;

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \""
            + keyspaceName
            + "\" WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
  }

  /**
   * Gets the exposed port for the Cassandra native transport (default 9042).
   *
   * @return the port mapped to the container's 9042 port.
   */
  public int getPort() {
    return super.getPort(9042);
  }

  /**
   * Gets the name of the keyspace created within the container.
   *
   * @return the keyspace name.
   */
  public String getKeyspaceName() {
    return keyspaceName;
  }

  /**
   * Gets the generated truststore file that contains the self-signed certificate. This is necessary
   * for configuring client connections to the container.
   *
   * @return the truststore file.
   */
  public File getTrustStoreFile() {
    return trustStoreFile;
  }

  /**
   * Executes a CQL statement against the managed Cassandra instance in the current keyspace.
   *
   * @param query the CQL query to execute.
   * @return the {@link ResultSet} from the execution.
   */
  public ResultSet executeStatement(String query) {
    session.execute("USE \"" + keyspaceName + "\"");
    return session.execute(query);
  }

  /**
   * Reads all rows from the specified table in the current keyspace.
   *
   * @param table the name of the table to read from.
   * @return an {@link Iterable} of {@link Row} containing all rows in the table.
   */
  public Iterable<Row> readTable(String table) {
    return session.execute("SELECT * FROM \"" + keyspaceName + "\"." + table).all();
  }

  /** Closes the CQL session and stops the underlying TestContainers container. */
  @Override
  public void cleanupAll() {
    if (session != null) {
      session.close();
    }
    super.cleanupAll();
  }

  /** Builder for {@link CassandraSslContainerWrapper}. */
  public static class Builder
      extends TestContainerResourceManager.Builder<CassandraSslContainerWrapper> {
    private String keyspaceName;

    /**
     * Constructs a new Builder.
     *
     * @param testId the test identifier used for tracking resources.
     */
    public Builder(String testId) {
      super(testId, "cassandra", "4.1.4");
    }

    /**
     * Sets the keyspace name to create upon initialization.
     *
     * @param keyspaceName the name of the keyspace.
     * @return the builder instance.
     */
    public Builder setKeyspaceName(String keyspaceName) {
      this.keyspaceName = keyspaceName;
      return this;
    }

    /**
     * Builds and starts the {@link CassandraSslContainerWrapper}, including certificate generation.
     *
     * @return the initialized container wrapper.
     */
    @Override
    public CassandraSslContainerWrapper build() {
      try {
        // Create temporary directory for storing generated certificates and keystores
        File tempDir = Files.createTempDirectory("cassandra-ssl").toFile();
        File keyStoreFile = new File(tempDir, "keystore.jks");
        File trustStoreFile = new File(tempDir, "truststore.jks");
        File certFile = new File(tempDir, "cassandra.cer");

        String password = "cassandra_ssl_password";
        String keytool = System.getProperty("java.home") + "/bin/keytool";

        // 1. Generate an RSA keypair and store it in the keystore
        ProcessBuilder pb1 =
            new ProcessBuilder(
                keytool,
                "-genkeypair",
                "-keyalg",
                "RSA",
                "-alias",
                "cassandra",
                "-keystore",
                keyStoreFile.getAbsolutePath(),
                "-storepass",
                password,
                "-keypass",
                password,
                "-validity",
                "365",
                "-keysize",
                "2048",
                "-dname",
                "CN=cassandra, OU=Test, O=Test, L=Test, S=Test, C=US");
        pb1.redirectErrorStream(true);
        if (pb1.start().waitFor() != 0) {
          throw new RuntimeException("Failed to generate keypair");
        }

        // 2. Export the public certificate from the generated keystore
        ProcessBuilder pb2 =
            new ProcessBuilder(
                keytool,
                "-export",
                "-alias",
                "cassandra",
                "-file",
                certFile.getAbsolutePath(),
                "-keystore",
                keyStoreFile.getAbsolutePath(),
                "-storepass",
                password);
        pb2.redirectErrorStream(true);
        if (pb2.start().waitFor() != 0) {
          throw new RuntimeException("Failed to export cert");
        }

        // 3. Import the exported certificate into a new truststore
        ProcessBuilder pb3 =
            new ProcessBuilder(
                keytool,
                "-import",
                "-v",
                "-trustcacerts",
                "-alias",
                "cassandra",
                "-file",
                certFile.getAbsolutePath(),
                "-keystore",
                trustStoreFile.getAbsolutePath(),
                "-storepass",
                password,
                "-noprompt");
        pb3.redirectErrorStream(true);
        if (pb3.start().waitFor() != 0) {
          throw new RuntimeException("Failed to import to truststore");
        }

        // 4. Start the Cassandra container with the generated keystore and truststore,
        //    and modify cassandra.yaml to enable client_encryption_options
        CassandraContainer container =
            new CassandraContainer(this.containerImageName + ":" + this.containerImageTag)
                .withCopyFileToContainer(
                    MountableFile.forHostPath(keyStoreFile.getAbsolutePath()),
                    "/etc/cassandra/keystore.jks")
                .withCopyFileToContainer(
                    MountableFile.forHostPath(trustStoreFile.getAbsolutePath()),
                    "/etc/cassandra/truststore.jks")
                .withCreateContainerCmdModifier(
                    cmd ->
                        cmd.withEntrypoint(
                            "bash",
                            "-c",
                            "echo -e '\\nclient_encryption_options:\\n    enabled: true\\n    optional: false\\n    keystore: /etc/cassandra/keystore.jks\\n    keystore_password: cassandra_ssl_password\\n    require_client_auth: false\\n    truststore: /etc/cassandra/truststore.jks\\n    truststore_password: cassandra_ssl_password' >> /etc/cassandra/cassandra.yaml && /usr/local/bin/docker-entrypoint.sh cassandra -f"))
                .waitingFor(
                    Wait.forLogMessage(".*Starting listening for CQL clients.*\\n", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        return new CassandraSslContainerWrapper(this, container, trustStoreFile, password);
      } catch (Exception e) {
        throw new RuntimeException("Failed to start SSL Cassandra", e);
      }
    }
  }

  /**
   * Creates a new Builder for the Cassandra SSL container wrapper.
   *
   * @param testId the test identifier.
   * @return a new {@link Builder}.
   */
  public static Builder builder(String testId) {
    return new Builder(testId);
  }
}
