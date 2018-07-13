/*
 * Copyright (C) 2018 Google Inc.
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
 */

package com.google.cloud.teleport.spanner;

import static com.google.cloud.teleport.spanner.Matchers.equalsIgnoreWhitespace;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.RandomDdlGenerator;
import java.io.IOException;
import java.util.Collection;
import org.apache.avro.Schema;
import org.junit.Test;

/**
 * An end to end test for {@link DdlToAvroSchemaConverter} and {@link AvroSchemaToDdlConverter}.
 * Generates a random schema, and verifies that converting to Avro and back doesn't change
 * the Ddl.
 */
public class CopySchemaTest {

  @Test
  public void copyRandomSchema() {
    Ddl ddl =
        RandomDdlGenerator.builder()
            .setMaxPkComponents(2)
            .setMaxBranchPerLevel(new int[] {5, 4, 3, 2, 2, 3, 3})
            .build()
            .generate();
    try {
      ddl.prettyPrint(System.out);
    } catch (IOException e) {
      e.printStackTrace();
    }

    DdlToAvroSchemaConverter ddlToAvro = new DdlToAvroSchemaConverter("spanner", "test");
    AvroSchemaToDdlConverter avroToDdl = new AvroSchemaToDdlConverter();

    Collection<Schema> schemas = ddlToAvro.convert(ddl);
    Ddl copied = avroToDdl.toDdl(schemas);

    assertThat(copied.prettyPrint(), equalsIgnoreWhitespace(ddl.prettyPrint()));
  }
}
