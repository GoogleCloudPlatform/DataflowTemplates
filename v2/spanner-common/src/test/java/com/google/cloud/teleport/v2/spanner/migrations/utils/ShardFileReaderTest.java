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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class ShardFileReaderTest {
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private ISecretManagerAccessor secretManagerAccessorMockImpl;

  @Test
  public void shardFileReading() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    List<Shard> shards = shardFileReader.getOrderedShardDetails("src/test/resources/shard.json");
    List<Shard> expectedShards =
        Arrays.asList(
            new Shard("shardA", "hostShardA", "3306", "test", "test", "test", ""),
            new Shard("shardB", "hostShardB", "3306", "test", "test", "test", ""));

    assertEquals(shards, expectedShards);
  }

  @Test
  public void shardFileReadingFileNotExists() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails("src/test/resources/somemissingfile.json"));
    assertTrue(thrown.getMessage().contains("Failed to read shard input file"));
  }

  @Test
  public void shardFileReadingWithSecret() {

    when(secretManagerAccessorMockImpl.getSecret(
            "projects/545418958905/secrets/secretA/versions/latest"))
        .thenReturn("secretA");
    when(secretManagerAccessorMockImpl.getSecret("projects/545418958905/secrets/secretB"))
        .thenReturn("secretB");
    when(secretManagerAccessorMockImpl.getSecret("projects/545418958905/secrets/secretC/"))
        .thenReturn("secretC");

    ShardFileReader shardFileReader = new ShardFileReader(secretManagerAccessorMockImpl);
    List<Shard> shards =
        shardFileReader.getOrderedShardDetails("src/test/resources/shard-with-secret.json");
    List<Shard> expectedShards =
        Arrays.asList(
            new Shard(
                "shardA",
                "hostShardA",
                "3306",
                "test",
                "secretA",
                "test",
                "projects/545418958905/secrets/secretA/versions/latest"),
            new Shard(
                "shardB",
                "hostShardB",
                "3306",
                "test",
                "secretB",
                "test",
                "projects/545418958905/secrets/secretB"),
            new Shard(
                "shardC",
                "hostShardC",
                "3306",
                "test",
                "secretC",
                "test",
                "projects/545418958905/secrets/secretC/"),
            new Shard("shardD", "hostShardD", "3306", "test", "test", "test", ""));

    assertEquals(shards, expectedShards);
  }

  @Test
  public void shardFileSecretPatternIncorrect() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails(
                    "src/test/resources/shard-with-secret-error.json"));
    assertTrue(
        thrown
            .getMessage()
            .contains("does not adhere to expected pattern projects/.*/secrets/.*/versions/.*"));
  }

  @Test
  public void shardFileWithNoCredentials() {
    ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                shardFileReader.getOrderedShardDetails(
                    "src/test/resources/shard-with-nocreds.json"));
    assertTrue(
        thrown
            .getMessage()
            .contains("Neither password nor secretManagerUri was found in the shard file"));
  }
}
