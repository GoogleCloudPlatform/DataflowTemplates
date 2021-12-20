package com.infusionsoft.dataflow.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudStorageUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CloudStorageUtils.class);

  private static final Storage.BlobTargetOption PRIVATE = Storage.BlobTargetOption.predefinedAcl(Storage.PredefinedAcl.PROJECT_PRIVATE);

  public static Storage getStorage(String projectId) {
    checkArgument(StringUtils.isNotBlank(projectId), "projectId must not be blank");

    return StorageOptions.newBuilder()
        .setProjectId(projectId)
        .build()
        .getService();
  }

  public static Blob upload(Storage storage, String bucketName, String fileName,
                            String data, ContentType contentType) {

    checkNotNull(storage, "storage must not be null");
    checkArgument(StringUtils.isNotBlank(bucketName), "bucketName must not be blank");
    checkArgument(StringUtils.isNotBlank(fileName), "fileName must not be blank");
    checkArgument(StringUtils.isNotBlank(data), "data must not be blank");
    checkNotNull(contentType, "contentType must not be null");

    final byte[] content = data.getBytes(Charsets.UTF_8);

    final BlobInfo info = BlobInfo.newBuilder(bucketName, fileName)
        .setContentType(contentType.getMimeType())
        .build();

    return storage.create(info, content, PRIVATE);
  }

  public static void delete(Storage storage, String bucketName, String... fileNames) {
    checkNotNull(storage, "storage must not be null");
    checkArgument(StringUtils.isNotBlank(bucketName), "bucketName must not be blank");

    final List<BlobId> ids = toBlobIds(bucketName, fileNames);

    if (ids.size() > 0) {
      final List<Boolean> deleted = storage.delete(ids);
      LOG.debug("{} -> {}", fileNames, deleted);
    }
  }

  private static List<BlobId> toBlobIds(String bucketName, String... fileNames) {
    checkNotNull(fileNames, "fileNames must not be null");

    return Arrays.stream(fileNames)
        .filter(StringUtils::isNotBlank)
        .map(fileName -> BlobId.of(bucketName, fileName))
        .collect(Collectors.toList());
  }

  public static List<Blob> load(Storage storage, String bucketName, String... fileNames) {
    checkNotNull(storage, "storage must not be null");
    checkArgument(StringUtils.isNotBlank(bucketName), "bucketName must not be blank");

    final List<BlobId> ids = toBlobIds(bucketName, fileNames);

    return ids.size() > 0 ? storage.get(ids) : Collections.emptyList();
  }

  public static Page<Blob> find(Storage storage, String bucketName, String prefix, long limit, @Nullable String token) {
    checkNotNull(storage, "storage must not be null");
    checkArgument(StringUtils.isNotBlank(prefix), "prefix must not be blank");
    checkArgument(limit > 0, "limit must be > 0");

    return StringUtils.isNotBlank(token)
        ? storage.list(bucketName, BlobListOption.prefix(prefix), BlobListOption.pageSize(limit), BlobListOption.pageToken(token))
        : storage.list(bucketName, BlobListOption.prefix(prefix), BlobListOption.pageSize(limit));
  }
}
