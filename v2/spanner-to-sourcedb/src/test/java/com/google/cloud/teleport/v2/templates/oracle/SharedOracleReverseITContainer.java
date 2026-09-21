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
package com.google.cloud.teleport.v2.templates.oracle;

import org.apache.beam.it.gcp.cloudsql.CloudOracleResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedOracleReverseITContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SharedOracleReverseITContainer.class);
  private static final Object lock = new Object();

  private static SpannerOracleResourceManager instance;

  public static SpannerOracleResourceManager getInstance() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          LOG.info("Initializing global Singleton Static Oracle pool.");
          String host = System.getProperty("oracleStaticHost", "10.128.0.108");
          String password = System.getProperty("oracleStaticPassword", "TestPassword123");
          CloudOracleResourceManager.Builder builder =
              CloudOracleResourceManager.builder("oracle_static");
          builder.setUsername("system");
          builder.setPassword(password);
          builder.setDatabaseName("XE");
          builder.setHost(host);
          builder.setPort(1521);
          instance = new SpannerOracleResourceManager(builder);
        }
      }
    }
    return instance;
  }
}
