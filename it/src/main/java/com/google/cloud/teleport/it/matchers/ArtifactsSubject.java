/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.matchers;

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatGenericRecords;
import static com.google.common.hash.Hashing.sha256;

import com.google.cloud.teleport.it.artifacts.Artifact;
import com.google.cloud.teleport.it.common.AvroTestUtil;
import com.google.cloud.teleport.it.common.ParquetTestUtil;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.Nullable;

/**
 * Subject that has assertion operations for artifact lists (GCS files), usually coming from the
 * result of a template.
 */
public final class ArtifactsSubject extends Subject {

  @Nullable private final List<Artifact> actual;

  private ArtifactsSubject(FailureMetadata metadata, @Nullable List<Artifact> actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public static Factory<ArtifactsSubject, List<Artifact>> records() {
    return ArtifactsSubject::new;
  }

  /** Check if artifact list has files (is not empty). */
  public void hasFiles() {
    check("there are files").that(actual).isNotEmpty();
  }

  /**
   * Check if artifact list has a specific number of files.
   *
   * @param expectedFiles Expected Rows
   */
  public void hasFiles(int expectedFiles) {
    check("there are %d files", expectedFiles).that(actual.size()).isEqualTo(expectedFiles);
  }

  /**
   * Check if any of the artifacts has a specific content.
   *
   * @param content Content to search for
   */
  public void hasContent(String content) {
    if (!actual.stream().anyMatch(artifact -> new String(artifact.contents()).contains(content))) {
      failWithActual("expected to contain", content);
    }
  }

  /**
   * Check if any of the artifacts have a specific content hash (using SHA-256).
   *
   * @param hash Content to search for
   */
  public void hasHash(String hash) {
    if (!actual.stream()
        .anyMatch(artifact -> sha256().hashBytes(artifact.contents()).toString().equals(hash))) {
      failWithActual("expected to contain hash", hash);
    }
  }

  /**
   * Parse artifacts to Avro records to be used for assertions.
   *
   * @param schema Avro Schema to use on the conversion.
   */
  public RecordsSubject asAvroRecords(Schema schema) {
    List<GenericRecord> allRecords = new ArrayList<>();

    for (Artifact artifact : this.actual) {
      try {
        allRecords.addAll(AvroTestUtil.readRecords(schema, artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Avro.", e);
      }
    }
    return assertThatGenericRecords(allRecords);
  }

  /** Parse artifacts to Parquet GenericRecord to be used for assertions. */
  public RecordsSubject asParquetRecords() {
    List<GenericRecord> allRecords = new ArrayList<>();

    for (Artifact artifact : this.actual) {
      try {
        allRecords.addAll(ParquetTestUtil.readRecords(artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Parquet.", e);
      }
    }
    return assertThatGenericRecords(allRecords);
  }
}
