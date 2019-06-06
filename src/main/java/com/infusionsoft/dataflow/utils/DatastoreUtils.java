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
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.RetryHttpRequestInitializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

public class DatastoreUtils {

  public static Datastore getDatastore(PipelineOptions options, String projectId) {
    checkNotNull(options, "options must not be null");
    checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");

    final Credentials credential = options.as(GcpOptions.class).getGcpCredential();

    final Object initializer;
    if (credential != null) {
      initializer = new ChainingHttpRequestInitializer(new HttpRequestInitializer[]{new HttpCredentialsAdapter(credential), new RetryHttpRequestInitializer()});
    } else {
      initializer = new RetryHttpRequestInitializer();
    }

    final DatastoreOptions.Builder builder = (new com.google.datastore.v1.client.DatastoreOptions.Builder()).projectId(projectId).initializer((HttpRequestInitializer)initializer);
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
      Arrays.stream(keys)
          .filter(Objects::nonNull)
          .map(String::valueOf)
          .forEach(items::add);
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
