/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/** Infers and prints a Cloud Spanner schema based on a content of Avro files. */
public class AvroToDdlTool {

  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.out.println("Please specify the avro files");
      System.exit(1);
    }

    List<Schema> schemaList = new ArrayList<>();
    for (String filePath : args) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<>(new File(filePath), datumReader);
      Schema schema = dataFileReader.getSchema();
      System.out.println(schema.toString(true));
      schemaList.add(schema);
    }
    Ddl ddl = new AvroSchemaToDdlConverter().toDdl(schemaList);
    ddl.prettyPrint(System.out);
  }
}
