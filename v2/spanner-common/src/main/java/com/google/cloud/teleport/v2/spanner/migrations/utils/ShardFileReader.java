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

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read the input files in GCS and convert it into a relevant object. */
public class ShardFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(ShardFileReader.class);

  private static final Pattern partialPattern = Pattern.compile("projects/.*/secrets/.*");
  private static final Pattern fullPattern = Pattern.compile("projects/.*/secrets/.*/versions/.*");
  private static final Pattern partialWithSlash = Pattern.compile("projects/.*/secrets/.*/");

  private ISecretManagerAccessor secretManagerAccessor;

  public ShardFileReader(ISecretManagerAccessor secretManagerAccessor) {
    this.secretManagerAccessor = secretManagerAccessor;
  }

  public List<Shard> getOrderedShardDetails(String sourceShardsFilePath) {

    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(sourceShardsFilePath, false)))) {

      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      Type listOfShardObject = new TypeToken<ArrayList<Shard>>() {}.getType();
      List<Shard> shardList =
          new GsonBuilder()
              .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
              .create()
              .fromJson(result, listOfShardObject);

      for (Shard shard : shardList) {
        LOG.info(" The shard is: {} ", shard);
        String password =
            resolvePassword(
                sourceShardsFilePath,
                shard.getSecretManagerUri(),
                shard.getLogicalShardId(),
                shard.getPassword());
        if (password == null || password.isEmpty()) {
          throw new RuntimeException(
              "Neither password nor secretManagerUri was found in the shard file "
                  + sourceShardsFilePath
                  + "  for shard "
                  + shard.getLogicalShardId());
        }
        shard.setPassword(password);
      }

      Collections.sort(
          shardList,
          new Comparator<Shard>() {
            public int compare(Shard s1, Shard s2) {
              return s1.getLogicalShardId().compareTo(s2.getLogicalShardId());
            }
          });

      return shardList;

    } catch (IOException e) {
      LOG.error(
          "Failed to read shard input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read shard input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
    }
  }

  private String resolvePassword(
      String sourceShardsFilePath,
      String secretManagerUri,
      String logicalShardId,
      String password) {
    if (secretManagerUri != null && !secretManagerUri.isEmpty()) {
      LOG.info(
          "Secret Manager will be used to get password for shard {} having secret {}",
          logicalShardId,
          secretManagerUri);
      if (partialPattern.matcher(secretManagerUri).matches()) {
        LOG.info("The matched secret for shard {} is : {}", logicalShardId, secretManagerUri);
        if (fullPattern.matcher(secretManagerUri).matches()) {
          LOG.info("The secret for shard {} is : {}", logicalShardId, secretManagerUri);
          return secretManagerAccessor.getSecret(secretManagerUri);
        } else {
          // partial match hence get the latest version
          String versionToAppend = "versions/latest";
          if (partialWithSlash.matcher(secretManagerUri).matches()) {
            secretManagerUri += versionToAppend;
          } else {
            secretManagerUri += "/" + versionToAppend;
          }

          LOG.info("The generated secret for shard {} is : {}", logicalShardId, secretManagerUri);
          return secretManagerAccessor.getSecret(secretManagerUri);
        }
      } else {
        LOG.error(
            "The secretManagerUri field with value {} for shard {} , specified in file {} does"
                + " not adhere to expected pattern projects/.*/secrets/.*/versions/.*",
            secretManagerUri,
            logicalShardId,
            sourceShardsFilePath);
        throw new RuntimeException(
            "The secretManagerUri field with value "
                + secretManagerUri
                + " for shard "
                + logicalShardId
                + ", specified in file "
                + sourceShardsFilePath
                + " does not adhere to expected pattern"
                + " projects/.*/secrets/.*/versions/.*");
      }
    }
    LOG.info("using plaintext password for shard: {}", logicalShardId);
    return password;
  }

  /**
   * Read the sharded migration config and return a list of physical shards.
   *
   * @param sourceShardsFilePath
   * @return
   */
  public List<Shard> readForwardMigrationShardingConfig(String sourceShardsFilePath) {
    String jsonString = null;
    try {
      InputStream stream =
          Channels.newInputStream(
              FileSystems.open(FileSystems.matchNewResource(sourceShardsFilePath, false)));

      jsonString = IOUtils.toString(stream, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error(
          "Failed to read shard input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read shard input file. Make sure it is ASCII or UTF-8 encoded and contains a"
              + " well-formed JSON string.",
          e);
    }

    // TODO - create a structure for the shard config and map directly to the object
    Type shardConfiguration = new TypeToken<Map>() {}.getType();
    Map shardConfigMap =
        new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create()
            .fromJson(jsonString, shardConfiguration);

    List<Shard> shardList = new ArrayList<>();
    List<Map> dataShards =
        (List)
            (((Map) shardConfigMap.getOrDefault("shardConfigurationBulk", new HashMap<>()))
                .getOrDefault("dataShards", new ArrayList<>()));

    for (Map dataShard : dataShards) {
      List<Map> databases = (List) (dataShard.getOrDefault("databases", new ArrayList<>()));

      String host = (String) (dataShard.get("host"));
      if (databases.isEmpty()) {
        LOG.warn("no databases found for host: {}", host);
        throw new RuntimeException("no databases found for host: " + String.valueOf(host));
      }

      String password =
          resolvePassword(
              sourceShardsFilePath,
              (String) dataShard.get("secretManagerUri"),
              host,
              (String) dataShard.get("password"));
      if (password == null || password.isEmpty()) {
        LOG.warn("could not fetch password for host: {}", host);
        throw new RuntimeException(
            "Neither password nor secretManagerUri was found in the shard file "
                + sourceShardsFilePath
                + "  for host "
                + host);
      }
      String namespace =
          Optional.ofNullable(dataShard.get("namespace")).map(Object::toString).orElse(null);

      Shard shard =
          new Shard(
              "",
              host,
              dataShard.getOrDefault("port", 0).toString(),
              (String) (dataShard.get("user")),
              password,
              "",
              namespace,
              (String) (dataShard.get("secretManagerUri")),
              dataShard.getOrDefault("connectionProperties", "").toString());

      for (Map database : databases) {
        shard
            .getDbNameToLogicalShardIdMap()
            .put((String) (database.get("dbName")), (String) (database.get("databaseId")));
      }
      shardList.add(shard);
    }
    return shardList;
  }
}
