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
package com.infusionsoft.dataflow.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.commons.lang3.StringUtils;

public class DatastoreUtils {

  public static Datastore getDatastore(PipelineOptions options, String projectId) {
    checkNotNull(options, "options must not be null");
    checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");

    final Credentials credential = options.as(GcpOptions.class).getGcpCredential();

    final Object initializer;
    if (credential != null) {
      initializer =
          new ChainingHttpRequestInitializer(
              new HttpRequestInitializer[] {
                new HttpCredentialsAdapter(credential), new RetryHttpRequestInitializer()
              });
    } else {
      initializer = new RetryHttpRequestInitializer();
    }

    final DatastoreOptions.Builder builder =
        (new com.google.datastore.v1.client.DatastoreOptions.Builder())
            .projectId(projectId)
            .initializer((HttpRequestInitializer) initializer);
    builder.host("batch-datastore.googleapis.com");

    return DatastoreFactory.get().create(builder.build());
  }

  public static String createCompositeKey(Object key1, Object key2, @Nullable Object... keys) {
    checkNotNull(key1, "key1 must not be null");
    checkNotNull(key2, "key2 must not be null");

    final List<String> items = new ArrayList<>();
    items.add(String.valueOf(key1));
    items.add(String.valueOf(key2));

    if (keys != null) {
      Arrays.stream(keys).filter(Objects::nonNull).map(String::valueOf).forEach(items::add);
    }

    return StringUtils.join(items, ":");
  }

  public static long getId(Key key) {
    checkNotNull(key, "key must not be null");

    final List<Key.PathElement> elementList = key.getPathList();
    final Key.PathElement lastElement = elementList.get(elementList.size() - 1);

    return lastElement.getId();
  }

  public static String getName(Key key) {
    checkNotNull(key, "key must not be null");

    final List<Key.PathElement> elementList = key.getPathList();
    final Key.PathElement lastElement = elementList.get(elementList.size() - 1);

    return lastElement.getName();
  }
}
