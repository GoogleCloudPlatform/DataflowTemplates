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
package com.google.cloud.teleport.v2.templates;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class TestClass {

  public static void main(String[] args) {
    String path = "anandinguva--test-1";
    String prefix1 = "anandinguva-kafka-to-gcs-binary-encoding/FullName";
    List<GenericRecord> records = new ArrayList<>();
    Storage storage =
        StorageOptions.newBuilder().setProjectId("dataflow-testing-311516").build().getService();
    Page<Blob> blobs = storage.list(path, Storage.BlobListOption.prefix(prefix1));
    for (Blob blob : blobs.iterateAll()) {
      byte[] content = blob.getContent();
      DatumReader<GenericRecord> datumReader =
          new GenericDatumReader<>(); // GenericRecord for schema-less reading
      try (DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<>(new SeekableByteArrayInput(content), datumReader)) {
        for (GenericRecord record : dataFileReader) {
          records.add(record);
          System.out.println(record);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println(records);
  }
}
