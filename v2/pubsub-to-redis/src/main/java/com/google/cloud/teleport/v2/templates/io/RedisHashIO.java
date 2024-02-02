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
package com.google.cloud.teleport.v2.templates.io;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

public abstract class RedisHashIO {

  public static WriteHash write() {

    return (new AutoValue_RedisHashIO_WriteHash.Builder())
        .setConnectionConfiguration(RedisConnectionConfiguration.create())
        .build();
  }

  @AutoValue
  public abstract static class WriteHash
      extends PTransform<@NonNull PCollection<KV<String, KV<String, String>>>, @NonNull PDone> {
    public WriteHash() {}

    @Nullable
    abstract RedisConnectionConfiguration connectionConfiguration();

    @Nullable
    abstract Long expireTime();

    abstract RedisHashIO.WriteHash.Builder builder();

    public RedisHashIO.WriteHash withConnectionConfiguration(
        RedisConnectionConfiguration connectionConfiguration) {
      Preconditions.checkArgument(connectionConfiguration != null, "connection cannot be null");
      return this.builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    public RedisHashIO.WriteHash withTtl(Long expireTimeMillis) {
      return this.builder().setExpireTime(expireTimeMillis).build();
    }

    @NonNull
    public PDone expand(PCollection<KV<String, KV<String, String>>> input) {
      Preconditions.checkArgument(
          connectionConfiguration() != null, "withConnectionConfiguration() is required");
      input.apply(ParDo.of(new RedisHashIO.WriteHash.WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    private static class WriteFn extends DoFn<KV<String, KV<String, String>>, Void> {
      private static final int DEFAULT_BATCH_SIZE = 1000;
      private static final long NO_EXPIRATION = -1L;
      private final RedisHashIO.WriteHash spec;
      private transient Jedis jedis;
      private transient Pipeline pipeline;
      private transient Transaction transaction;
      private int batchCount;

      public WriteFn(RedisHashIO.WriteHash spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        this.jedis = Objects.requireNonNull(this.spec.connectionConfiguration()).connect();
      }

      @StartBundle
      public void startBundle() {
        this.pipeline = this.jedis.pipelined();
        this.transaction = this.jedis.multi();
        this.batchCount = 0;
      }

      @ProcessElement
      public void processElement(DoFn<KV<String, KV<String, String>>, Void>.ProcessContext ctx) {
        KV<String, KV<String, String>> record = ctx.element();

        writeRecord(Objects.requireNonNull(record));

        batchCount++;

        if (batchCount >= DEFAULT_BATCH_SIZE) {
          this.transaction.exec();
          this.pipeline.sync();
          this.transaction.multi();
          this.batchCount = 0;
        }
      }

      private void writeRecord(KV<String, KV<String, String>> record) {
        Map<String, String> hashFieldValues = new HashMap<>();
        String hashKey = record.getKey();
        KV<String, String> hashValue = record.getValue();
        String fieldKey = hashValue.getKey();
        String fieldValue = hashValue.getValue();
        Long expireTime = this.spec.expireTime();

        hashFieldValues.putIfAbsent(
            "redisInsertTimeAsLong", String.valueOf(System.currentTimeMillis()));
        hashFieldValues.put(fieldKey, fieldValue);

        transaction.hset(hashKey, hashFieldValues);
        if (expireTime != null && !Objects.equals(expireTime, NO_EXPIRATION)) {
          transaction.expire(hashKey, expireTime);
        }
      }

      @FinishBundle
      public void finishBundle() {
        this.transaction.exec();
        if (this.transaction != null) {
          this.transaction.close();
        }

        this.transaction = null;
        this.batchCount = 0;
      }

      @Teardown
      public void teardown() {
        this.jedis.close();
      }
    }

    @AutoValue.Builder
    abstract static class Builder {
      Builder() {}

      abstract WriteHash.Builder setExpireTime(Long expireTimeMillis);

      abstract WriteHash.Builder setConnectionConfiguration(
          RedisConnectionConfiguration connectionConfiguration);

      abstract WriteHash build();
    }
  }
}
