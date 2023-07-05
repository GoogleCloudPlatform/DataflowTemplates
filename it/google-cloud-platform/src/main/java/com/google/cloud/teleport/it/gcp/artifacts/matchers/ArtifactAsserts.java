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
package com.google.cloud.teleport.it.gcp.artifacts.matchers;

import static com.google.cloud.teleport.it.gcp.artifacts.matchers.ArtifactsSubject.genericRecordToRecords;
import static com.google.cloud.teleport.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static com.google.common.truth.Truth.assertAbout;

import com.google.cloud.teleport.it.gcp.artifacts.Artifact;
import com.google.cloud.teleport.it.truthmatchers.RecordsSubject;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;

public class ArtifactAsserts {

  /**
   * Creates an {@link ArtifactsSubject} to assert information within a list of artifacts obtained
   * from Cloud Storage.
   *
   * @param artifacts Artifacts in list format to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifacts(@Nullable List<Artifact> artifacts) {
    return assertAbout(ArtifactsSubject.records()).that(artifacts);
  }

  /**
   * Creates an {@link ArtifactsSubject} to assert information for an artifact obtained from Cloud
   * Storage.
   *
   * @param artifact Artifact to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifact(@Nullable Artifact artifact) {
    return assertAbout(ArtifactsSubject.records()).that(List.of(artifact));
  }

  /**
   * Creates an {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in Avro/Parquet {@link GenericRecord} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatGenericRecords(List<GenericRecord> records) {
    return assertThatRecords(genericRecordToRecords(records));
  }
}
