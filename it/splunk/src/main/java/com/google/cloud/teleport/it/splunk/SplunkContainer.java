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
package com.google.cloud.teleport.it.splunk;

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
 * Constructs a Splunk container.
 *
 * <p>Tested on a Splunk version 8.2.
 *
 * <p>More information about docker-splunk can be found here:
 *
 * <p><a href="https://splunk.github.io/docker-splunk/">https://splunk.github.io/docker-splunk/</a>
 */
public class SplunkContainer extends GenericContainer<SplunkContainer> {
  private static final Logger log = LoggerFactory.getLogger(SplunkContainer.class);

  /** Splunk Default HTTP port. */
  private static final int SPLUNK_INTERNAL_PORT = 8000;

  /** Splunk Default HTTP Event Collector (HEC) port. */
  private static final int SPLUNK_HEC_INTERNAL_PORT = 8088;

  private static final int SPLUNKD_INTERNAL_PORT = 8089;

  /** Splunk Docker base image. */
  private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("splunk/splunk");

  private static final String DEFAULTS_FILE_PATH = "/tmp/defaults/default.yml";

  public SplunkContainer(@NonNull String dockerImageName) {
    this(DockerImageName.parse(dockerImageName));
  }

  public SplunkContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

    this.withExposedPorts(SPLUNK_INTERNAL_PORT, SPLUNK_HEC_INTERNAL_PORT, SPLUNKD_INTERNAL_PORT);
    this.withEnv(Map.of("SPLUNK_START_ARGS", "--accept-license"));
    this.waitingFor(
        Wait.forLogMessage("(?i).*Ansible playbook complete.*", 1)
            .withStartupTimeout(Duration.ofMinutes(3)));
  }

  /**
   * Define the Splunk password to set.
   *
   * @param password Password to set
   * @return this
   */
  public SplunkContainer withPassword(String password) {
    this.withEnv("SPLUNK_PASSWORD", password);
    return this;
  }

  /**
   * Define the Splunk HTTP Event Collector (HEC) token to set.
   *
   * @param hecToken Token to set
   * @return this
   */
  public SplunkContainer withHecToken(String hecToken) {
    this.withEnv("SPLUNK_HEC_TOKEN", hecToken);
    return this;
  }

  /**
   * Define whether ssl will be used for connecting to the splunk server.
   *
   * @return this
   */
  public SplunkContainer withSplunkdSslDisabled() {
    this.withEnv("SPLUNKD_SSL_ENABLE", "false");
    return this;
  }

  /**
   * Define a defaults file to use for configuring the Splunk server.
   *
   * <p>More information about the defaults file can be found here:
   *
   * <p><a
   * href="https://splunk.github.io/docker-splunk/ADVANCED.html#runtime-configuration">https://splunk.github.io/docker-splunk/ADVANCED.html#runtime-configuration</a>
   *
   * @param defaults A splunk defaults file to copy to container.
   * @return this
   */
  public SplunkContainer withDefaultsFile(Transferable defaults) {
    this.withCopyToContainer(defaults, DEFAULTS_FILE_PATH);
    return this;
  }

  // TODO - Future config environment variables that may be useful to add
  // SPLUNK_S2S_PORT
  // SPLUNK_SVC_PORT
  // SPLUNK_SECRET
  // SPLUNKD_SSL_CERT
  // SPLUNKD_SSL_CA
  // SPLUNKD_SSL_PASSWORD
}
