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
import java.io.IOException;
import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

/**
 * Utility Class to start and stop Embedded Cassandra. {@link Cassandra Embedded Cassandra} is
 * equivalent to real cassandra at the level of network protocol. So using this over mocks wherever
 * possible gives us much better test coverage. Note: Prefer using {@link SharedEmbeddedCassandra}
 * to share an instance of Embedded Cassandra.
 */
public class EmbeddedCassandra implements AutoCloseable {
  private Cassandra embeddedCassandra;
  private String clusterName;
  private ImmutableList<InetSocketAddress> contactPoints;
  private final Settings settings;
  private static final String LOCAL_DATA_CENTER = "datacenter1";

  public EmbeddedCassandra(String config, @Nullable String cqlResource) throws IOException {
    var builder =
        new CassandraBuilder()
            .addEnvironmentVariable("JAVA_HOME", System.getProperty("java.home"))
            .addEnvironmentVariable("JRE_HOME", System.getProperty("jre.home"))
            // Check [CASSANDRA-13396](https://issues.apache.org/jira/browse/CASSANDRA-13396)
            .addSystemProperty("cassandra.insecure.udf", "true")
            .configFile(new ClassPathResource(config));
    // Ref: https://stackoverflow.com/questions/78195798/embedded-cassandra-not-working-in-java-21
    if (Runtime.version().compareTo(Runtime.Version.parse("12")) >= 0) {
      builder = builder.addSystemProperty("java.security.manager", "allow");
    }
    /*
    * TODO (vardhanvthigle): Get EmbeddedCassandea 4.0 working with our UT JVM.
    // If we spawn Cassandra 4.0.0 for testing, it tries to set biased locking, which is not recognized by some JVMs.
    builder = builder.addJvmOptions("-XX:+IgnoreUnrecognizedVMOptions");
    // This is needed as Cassnadra 4.0 goes for deep reflections for java pacakges.
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

  @Override
  public void close() throws Exception {
    if (embeddedCassandra != null) {
      embeddedCassandra.stop();
    }
  }
}
