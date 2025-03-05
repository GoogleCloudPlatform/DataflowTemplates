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
package com.google.cloud.teleport.v2.templates.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.CqlDurationCodec;
import java.nio.ByteBuffer;
import java.time.Duration;
import net.jcip.annotations.ThreadSafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@ThreadSafe
public class DurationCodec implements TypeCodec<Duration> {

  private final TypeCodec<CqlDuration> innerCodec = new CqlDurationCodec();

  public DurationCodec() {}

  @Override
  public @NotNull GenericType<Duration> getJavaType() {
    return GenericType.DURATION;
  }

  @Override
  public @NotNull DataType getCqlType() {
    return DataTypes.DURATION;
  }

  @Override
  public @Nullable ByteBuffer encode(
      @Nullable Duration value, @NotNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    CqlDuration cqlDuration =
        CqlDuration.newInstance(
            (int) (value.toDays() / 30), (int) (value.toDays() % 30), value.toNanosPart());
    return innerCodec.encode(cqlDuration, protocolVersion);
  }

  @Override
  public @Nullable Duration decode(
      @Nullable ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }
    CqlDuration cqlDuration = innerCodec.decode(bytes, protocolVersion);
    if (cqlDuration == null) {
      return null;
    }
    return Duration.ofDays((long) cqlDuration.getMonths() * 30 + cqlDuration.getDays())
        .plusNanos(cqlDuration.getNanoseconds());
  }

  @Override
  public @Nullable Duration parse(@Nullable String value) {
    if (value == null || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    CqlDuration cqlDuration = innerCodec.parse(value);
    if (cqlDuration == null) {
      return null;
    }
    return Duration.ofDays((long) cqlDuration.getMonths() * 30 + cqlDuration.getDays())
        .plusNanos(cqlDuration.getNanoseconds());
  }

  @Override
  public @NotNull String format(@Nullable Duration value) {
    if (value == null) {
      return "NULL";
    }
    CqlDuration cqlDuration =
        CqlDuration.newInstance(
            (int) (value.toDays() / 30), (int) (value.toDays() % 30), value.toNanosPart());
    return innerCodec.format(cqlDuration);
  }
}
