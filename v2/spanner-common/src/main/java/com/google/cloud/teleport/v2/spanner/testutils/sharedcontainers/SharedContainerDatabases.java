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
package com.google.cloud.teleport.v2.spanner.testutils.sharedcontainers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.beam.it.jdbc.AbstractJDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

/**
 * Resource managers backed by a per-test database on a {@link SharedDatabaseServer}, for use from
 * integration test base classes.
 *
 * <p>Databases are dropped once all tests of the class that created them are done, because many
 * test classes keep their resource managers in static fields shared by all of their test methods.
 * Base classes must therefore declare {@link #dropDatabasesAfterClass()} as a public static
 * {@code @ClassRule} field.
 */
public final class SharedContainerDatabases {

  private static final Map<Class<?>, List<Runnable>> CLEANUPS = new ConcurrentHashMap<>();

  private SharedContainerDatabases() {}

  /** JUnit class rule that drops the databases a test class created on shared servers. */
  public static TestRule dropDatabasesAfterClass() {
    return (base, description) ->
        new Statement() {
          @Override
          public void evaluate() throws Throwable {
            try {
              base.evaluate();
            } finally {
              List<Runnable> cleanups = CLEANUPS.remove(description.getTestClass());
              if (cleanups != null) {
                cleanups.forEach(Runnable::run);
              }
            }
          }
        };
  }

  /**
   * Returns a MySQL resource manager for a test of {@code testClass}. By default it points at a new
   * database on the shared MySQL container; with sharing turned off (see {@link
   * SharedDatabaseServer#isEnabled()}) it gets a container of its own.
   */
  public static MySQLResourceManager mySql(Class<?> testClass, String testId) {
    if (!SharedDatabaseServer.isEnabled()) {
      return dedicatedMySql(testId);
    }
    MySQLResourceManager.Builder builder = MySQLResourceManager.builder(testId);
    pointAtSharedDatabase(builder, MySQLEngine.INSTANCE, testClass, testId);
    return builder.build();
  }

  /**
   * Returns a MySQL resource manager backed by a container of its own. Use it for tests that change
   * server-wide settings (e.g. {@code SET GLOBAL}), which would affect other tests on a shared
   * container.
   */
  public static MySQLResourceManager dedicatedMySql(String testId) {
    return MySQLResourceManager.builder(testId).build();
  }

  /**
   * Creates a database for the test on the engine's shared server, registers it for cleanup at the
   * end of {@code testClass}, and points {@code builder} at it.
   */
  private static void pointAtSharedDatabase(
      AbstractJDBCResourceManager.Builder<?> builder,
      SharedDatabaseServer.Engine engine,
      Class<?> testClass,
      String testId) {
    SharedDatabaseServer server = SharedDatabaseServer.get(engine);
    String databaseName = server.createDatabase(testId);
    CLEANUPS
        .computeIfAbsent(testClass, k -> new CopyOnWriteArrayList<>())
        .add(() -> server.dropDatabase(databaseName));
    builder.setDatabaseName(databaseName);
    builder.setUsername(server.getUsername());
    builder.setPassword(server.getPassword());
    builder.setHost(server.getHost());
    builder.setPort(server.getPort());
    builder.useStaticContainer();
  }
}
