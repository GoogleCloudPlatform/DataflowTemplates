/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.google.cloud.teleport.bigtable;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

/** Base to extend from to perform lightweight Cassandra tests. */
public class CassandraBaseTest {

  static PrimingClient primingClient;
  static ActivityClient activityClient;
  static Scassandra scassandra;

  private static Map.Entry<Integer,Integer> getFreePort() throws IOException {
    int firstPort;
    ServerSocket firstSocket = new ServerSocket(0);
    firstPort = firstSocket.getLocalPort();

    int secondPort;
    ServerSocket secondSocket = new ServerSocket(0);
    secondPort = secondSocket.getLocalPort();

    firstSocket.close();
    secondSocket.close();

    return new SimpleEntry<>(firstPort, secondPort);
  }

  @BeforeClass
  public static void startScassandraServer() throws Exception {

    Map.Entry<Integer, Integer> ports = getFreePort();

    scassandra = ScassandraFactory.createServer(ports.getKey(), ports.getValue());
    scassandra.start();
    primingClient = scassandra.primingClient();
    activityClient = scassandra.activityClient();
  }

  @AfterClass
  public static void shutdown() {
    scassandra.stop();
  }

  @Before
  public void setup() {
    activityClient.clearAllRecordedActivity();
    primingClient.clearAllPrimes();
  }
}
