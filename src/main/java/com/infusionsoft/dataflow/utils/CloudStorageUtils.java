package com.infusionsoft.dataflow.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Charsets;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;

public class CloudStorageUtils {

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
}
