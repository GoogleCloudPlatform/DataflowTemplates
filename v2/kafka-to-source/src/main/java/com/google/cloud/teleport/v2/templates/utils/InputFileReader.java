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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.templates.common.KafkaConnectionProfile;
import com.google.cloud.teleport.v2.templates.common.Shard;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to read the input files in GCS and convert it into a relevant object. */
public class InputFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(InputFileReader.class);

  public static List<Shard> getOrderedShardDetails(String sourceShardsFilePath, String sourceType) {

    if (!"mysql".equals(sourceType)) {
      LOG.error("Only mysql source type is supported.");
      throw new RuntimeException(
          "Input sourceType value : "
              + sourceType
              + " is unsupported. Supported values are : mysql");
    }

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

      for (Shard s : shardList) {
        LOG.info(" The shard is: " + s.toString());
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

  public static KafkaConnectionProfile getKafkaConnectionProfile(String kafkaClusterFilePath) {
    try (InputStream stream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(kafkaClusterFilePath, false)))) {
      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);

      KafkaConnectionProfile response =
          new GsonBuilder()
              .setFieldNamingPolicy(FieldNamingPolicy.IDENTITY)
              .create()
              .fromJson(result, KafkaConnectionProfile.class);
      return response;

    } catch (IOException e) {
      LOG.error(
          "Failed to read kafka cluster input file. Make sure it is ASCII or UTF-8 encoded and"
              + " contains a well-formed JSON string.",
          e);
      throw new RuntimeException(
          "Failed to read kafka cluster input file. Make sure it is ASCII or UTF-8 encoded and"
              + " contains a well-formed JSON string.",
          e);
    }
  }
}
