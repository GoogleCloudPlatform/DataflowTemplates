/*
 * Copyright (C) 2025 Google LLC
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class DataStreamMongoDBToFirestoreTest {

  @Test
  public void inputArgs_inputFilePattern() {
    String[] args = new String[] {"--inputFilePattern=gs://test-bkt/"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFilePattern = options.getInputFilePattern();

    assertEquals(inputFilePattern, "gs://test-bkt/");
  }

  @Test
  public void inputArgs_connectionUri_startWithMongodb() {
    String[] args = new String[] {"--connectionUri=mongodb://my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String connectionUri = options.getConnectionUri();

    assertEquals(connectionUri, "mongodb://my-connection-string");
  }

  @Test
  public void inputArgs_connectionUri_startWithMongodbSrv() {
    String[] args = new String[] {"--connectionUri=mongodb+srv://my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String connectionUri = options.getConnectionUri();

    assertEquals(connectionUri, "mongodb+srv://my-connection-string");
  }

  @Test
  public void inputArgs_connectionUri_invalid() {
    String[] args = new String[] {"--connectionUri=my-connection-string"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    assertThrows(IllegalArgumentException.class, () -> DataStreamMongoDBToFirestore.run(options));
  }

  @Test
  public void inputArgs_inputFileFormat_json() {
    String[] args = new String[] {"--inputFileFormat=json"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFileFormat = options.getInputFileFormat();

    assertEquals(inputFileFormat, "json");
  }

  @Test
  public void inputArgs_inputFileFormat_avro() {
    String[] args = new String[] {"--inputFileFormat=avro"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);
    String inputFileFormat = options.getInputFileFormat();

    assertEquals(inputFileFormat, "avro");
  }

  @Test
  public void inputArgs_inputFileFormat_invalid() {
    String[] args =
        new String[] {"--connectionUri=mongodb://my-connection-string", "--inputFileFormat=other"};
    DataStreamMongoDBToFirestore.Options options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(DataStreamMongoDBToFirestore.Options.class);

    assertThrows(IllegalArgumentException.class, () -> DataStreamMongoDBToFirestore.run(options));
  }
}
