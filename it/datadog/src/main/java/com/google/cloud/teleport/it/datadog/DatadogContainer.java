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
package com.google.cloud.teleport.it.datadog;

import java.time.Duration;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Constructs a Datadog container.
 *
 * <p>Tested on a Datadog version 8.2.
 *
 * <p>More information about docker-datadog can be found here:
 *
 * <p><a href="https://datadog.github.io/docker-datadog/">https://datadog.github.io/docker-datadog/</a>
 */
public class DatadogContainer extends GenericContainer<DatadogContainer> {
  private static final Logger log = LoggerFactory.getLogger(DatadogContainer.class);

  /** Datadog Default HTTP port. */
  private static final int DATADOG_INTERNAL_PORT = 8000;

  /** Datadog Default HTTP Event Collector (HEC) port. */
  private static final int DATADOG_HEC_INTERNAL_PORT = 8088;

  private static final int DATADOGD_INTERNAL_PORT = 8089;

  /** Datadog Docker base image. */
  private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("datadog/datadog");

  private static final String DEFAULTS_FILE_PATH = "/tmp/defaults/default.yml";

  public DatadogContainer(@NonNull String dockerImageName) {
    this(DockerImageName.parse(dockerImageName));
  }

  public DatadogContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

    this.withExposedPorts(DATADOG_INTERNAL_PORT, DATADOG_HEC_INTERNAL_PORT, DATADOGD_INTERNAL_PORT);
    this.withEnv(Map.of("DATADOG_START_ARGS", "--accept-license"));
    this.waitingFor(
        Wait.forLogMessage("(?i).*Ansible playbook complete.*", 1)
            .withStartupTimeout(Duration.ofMinutes(3)));
  }

  /**
   * Define the Datadog password to set.
   *
   * @param password Password to set
   * @return this
   */
  public DatadogContainer withPassword(String password) {
    this.withEnv("DATADOG_PASSWORD", password);
    return this;
  }

  /**
   * Define the Datadog HTTP Event Collector (HEC) token to set.
   *
   * @param hecToken Token to set
   * @return this
   */
  public DatadogContainer withHecToken(String hecToken) {
    this.withEnv("DATADOG_HEC_TOKEN", hecToken);
    return this;
  }

  /**
   * Define whether ssl will be used for connecting to the datadog server.
   *
   * @return this
   */
  public DatadogContainer withDatadogdSslDisabled() {
    this.withEnv("DATADOGD_SSL_ENABLE", "false");
    return this;
  }

  /**
   * Define a defaults file to use for configuring the Datadog server.
   *
   * <p>More information about the defaults file can be found here:
   *
   * <p><a
   * href="https://datadog.github.io/docker-datadog/ADVANCED.html#runtime-configuration">https://datadog.github.io/docker-datadog/ADVANCED.html#runtime-configuration</a>
   *
   * @param defaults A datadog defaults file to copy to container.
   * @return this
   */
  public DatadogContainer withDefaultsFile(Transferable defaults) {
    this.withCopyToContainer(defaults, DEFAULTS_FILE_PATH);
    return this;
  }

  // TODO - Future config environment variables that may be useful to add
  // DATADOG_S2S_PORT
  // DATADOG_SVC_PORT
  // DATADOG_SECRET
  // DATADOGD_SSL_CERT
  // DATADOGD_SSL_CA
  // DATADOGD_SSL_PASSWORD
}
