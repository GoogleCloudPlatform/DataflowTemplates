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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
public abstract class SerializableSSLOptionsFactory
    implements SSLOptionsProvider.SSLOptionsFactory, Serializable {

  @Nullable
  abstract String trustStorePath();

  @Nullable
  abstract String trustStorePassword();

  @Nullable
  abstract String keyStorePath();

  @Nullable
  abstract String keyStorePassword();

  @Nullable
  abstract List<String> sslCipherSuites();

  public static Builder builder() {
    return new AutoValue_SerializableSSLOptionsFactory.Builder();
  }

  @Override
  public SSLOptions create() {
    RemoteEndpointAwareJdkSSLOptions.Builder builder = RemoteEndpointAwareJdkSSLOptions.builder();
    if (trustStorePath() != null || keyStorePath() != null) {
      builder.withSSLContext(
          SslContextFactory.createSslContext(
              trustStorePath(), trustStorePassword(), keyStorePath(), keyStorePassword()));
    }
    if (sslCipherSuites() != null && !sslCipherSuites().isEmpty()) {
      builder.withCipherSuites(sslCipherSuites().toArray(new String[0]));
    }
    return builder.build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setTrustStorePath(String value);

    public abstract Builder setTrustStorePassword(String value);

    public abstract Builder setKeyStorePath(String value);

    public abstract Builder setKeyStorePassword(String value);

    public abstract Builder setSslCipherSuites(List<String> value);

    public abstract SerializableSSLOptionsFactory build();
  }
}
