/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;

import com.google.api.services.datastream.v1alpha1.model.OracleTable;
import com.google.api.services.datastream.v1alpha1.model.SourceConfig;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.junit.Ignore;
import org.junit.Test;

/** Test cases for the {@link SchemaUtils} class. */
public class DataStreamClientTest {

  /**
   * Test whether {@link DataStreamClient.getParentFromConnectionProfileName} regex.
   */
  @Test
  public void testDataStreamClientGetParent() throws IOException, GeneralSecurityException {
    String projectId = "my-project";
    String connProfileName = "projects/402074789819/locations/us-central1/connectionProfiles/cp-1";
    String expectedParent = "projects/402074789819/locations/us-central1";

    DataStreamClient datastream = new DataStreamClient(null);
    String parent = datastream.getParentFromConnectionProfileName(connProfileName);

    assertEquals(parent, expectedParent);
  }

  /**
   * Test whether {@link DataStreamClient#getSourceConnectionProfileName(String)}
   * gets a Streams source connection profile name.
   */
  @Ignore
  @Test
  public void testDataStreamClientGetSourceConnectionProfileName() 
      throws IOException, GeneralSecurityException {
    String projectId = "dataflow-bigstream-integration";
    String streamName = "projects/dataflow-bigstream-integration/locations/us-central1/streams/dfstream1";
    String connProfileName = "projects/402074789819/locations/us-central1/connectionProfiles/cp-1";
    
    DataStreamClient datastream = new DataStreamClient(null);

    SourceConfig sourceConnProfile = datastream.getSourceConnectionProfile(streamName);
    String sourceConnProfileName = sourceConnProfile.getSourceConnectionProfileName();

    assertEquals(sourceConnProfileName, connProfileName);
  }

  /**
   * Test whether {@link DataStreamClient#discoverTableSchema(...)}
   * gets a Streams source connection profile name.
   */
  @Ignore
  @Test
  public void testDataStreamClientDiscoverOracleTableSchema() 
      throws IOException, GeneralSecurityException {
    String projectId = "dataflow-bigstream-integration";
    String streamName = "projects/dataflow-bigstream-integration/locations/us-central1/streams/dfstream1";
    String schemaName = "HR";
    String tableName = "JOBS";
    
    DataStreamClient datastream = new DataStreamClient(null);
    SourceConfig sourceConnProfile = datastream.getSourceConnectionProfile(streamName);
    OracleTable table = datastream.discoverOracleTableSchema(
        streamName, schemaName, tableName, sourceConnProfile);

    String columnName = table.getOracleColumns().get(0).getColumnName();
    Boolean isPrimaryKey = table.getOracleColumns().get(0).getPrimaryKey();

    assertEquals(columnName, "JOB_TITLE");
    assertEquals(isPrimaryKey, null);
  }
}
