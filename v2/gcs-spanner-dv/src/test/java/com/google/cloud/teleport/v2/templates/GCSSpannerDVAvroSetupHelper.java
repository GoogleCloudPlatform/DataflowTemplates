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

import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.gcp.artifacts.ArtifactClient;

/**
 * A centralized helper class for setting up Avro data in GCS for integration tests. This class
 * abstracts away the boilerplate of loading Avro schemas, building records, and serializing them
 * into .avro files uploaded to GCS.
 */
public class GCSSpannerDVAvroSetupHelper {

  private final Schema usersSchema;
  private final Schema accountRolesSchema;

  public GCSSpannerDVAvroSetupHelper() throws IOException {
    // Parse the schemas exactly once when the helper is instantiated.
    // Assumes you will save the extracted production schemas to these paths in src/test/resources/
    this.usersSchema =
        new Schema.Parser()
            .parse(Resources.getResource("GCSSpannerDVAvroSetupHelper/users.avsc").openStream());
    this.accountRolesSchema =
        new Schema.Parser()
            .parse(
                Resources.getResource("GCSSpannerDVAvroSetupHelper/account_roles.avsc")
                    .openStream());
  }

  public Schema getUsersSchema() {
    return usersSchema;
  }

  public Schema getAccountRolesSchema() {
    return accountRolesSchema;
  }

  /**
   * Serializes a list of GenericRecords into a binary .avro file and uploads it to GCS.
   *
   * @param gcsClient The ArtifactClient from TemplateTestBase
   * @param gcsFileName The name of the file to create in GCS (e.g. "users/part-000.avro")
   * @param schema The Avro schema for the records
   * @param records The records to serialize
   */
  public void uploadAvroFileToGcs(
      ArtifactClient gcsClient, String gcsFileName, Schema schema, List<GenericRecord> records)
      throws IOException {
    File tempFile = File.createTempFile("test-avro-", ".avro");
    tempFile.deleteOnExit();

    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, tempFile);
      for (GenericRecord record : records) {
        dataFileWriter.append(record);
      }
    }

    // Upload the temporary file to GCS
    gcsClient.uploadArtifact(gcsFileName, tempFile.getAbsolutePath());

    // Clean up locally
    tempFile.delete();
  }

  // --- Private Helper Methods ---

  public GenericRecord createUsersRecord(
      long userId, String eventId, String fullName, int age, Instant timestamp, String shardId) {

    GenericRecordBuilder outerBuilder = new GenericRecordBuilder(usersSchema);
    outerBuilder.set("tableName", "Users");
    outerBuilder.set("shardId", shardId);
    outerBuilder.set("primaryKeys", Arrays.asList("user_id", "event_id"));

    Schema payloadSchema = usersSchema.getField("payload").schema();
    GenericRecordBuilder payloadBuilder = new GenericRecordBuilder(payloadSchema);

    payloadBuilder.set("user_id", userId);
    payloadBuilder.set("event_id", eventId);
    payloadBuilder.set("full_name", fullName);
    payloadBuilder.set("age", age);
    payloadBuilder.set("created_at", timestamp.toEpochMilli() * 1000L); // Convert to micros

    outerBuilder.set("payload", payloadBuilder.build());
    return outerBuilder.build();
  }

  public GenericRecord createAccountRolesRecord(int roleId, String roleName, String shardId) {
    GenericRecordBuilder outerBuilder = new GenericRecordBuilder(accountRolesSchema);
    outerBuilder.set("tableName", "AccountRoles");
    outerBuilder.set("shardId", shardId);
    outerBuilder.set("primaryKeys", Arrays.asList("role_id"));

    Schema payloadSchema = accountRolesSchema.getField("payload").schema();
    GenericRecordBuilder payloadBuilder = new GenericRecordBuilder(payloadSchema);

    payloadBuilder.set("role_id", roleId);
    payloadBuilder.set("role_name", roleName);

    outerBuilder.set("payload", payloadBuilder.build());
    return outerBuilder.build();
  }
}
