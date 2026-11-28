/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.validator.dofn;

import com.google.cloud.teleport.v2.mongodb.validator.util.BsonEncodingUtils;
import com.google.cloud.teleport.v2.mongodb.validator.util.MongoClientManager;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.Closeable;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Serializable seam for opening sorted slice cursors and executing batch {@code $in} point lookups
 * against Source MongoDB and Target Firestore.
 */
public interface SliceReaderFactory extends Serializable {

  /** Closeable iterator of {@link BsonDocument}s. */
  interface CloseableDocIterator extends Iterator<BsonDocument>, Closeable {
    @Override
    void close();
  }

  /**
   * Opens an {@code _id}-ascending cursor over documents matching {@code baseFilter} (and strictly
   * greater than {@code resumeAfterExclusiveId} when non-null).
   */
  CloseableDocIterator openSortedCursor(
      boolean isTarget,
      String collection,
      BsonDocument baseFilter,
      @Nullable BsonValue resumeAfterExclusiveId,
      int batchSize,
      boolean idOnlyProjection);

  /**
   * Opens an unordered cursor over documents matching {@code filter} (used for {@code BATCH_LOOKUP}
   * slices).
   */
  CloseableDocIterator openUnorderedCursor(
      boolean isTarget,
      String collection,
      BsonDocument filter,
      int batchSize,
      boolean idOnlyProjection);

  /**
   * Fetches documents whose {@code _id} is in {@code ids}, keyed by canonical encoded {@code _id}
   * bytes ({@link ByteBuffer}). When {@code strongPrimaryRead} is true, reads from primary with
   * majority read concern.
   */
  Map<ByteBuffer, BsonDocument> fetchBatchByIds(
      boolean isTarget,
      String collection,
      List<BsonValue> ids,
      boolean strongPrimaryRead,
      boolean idOnlyProjection);

  /** Production implementation backed by {@link MongoClientManager}. */
  final class LiveMongoSliceReaderFactory implements SliceReaderFactory {

    private static final long serialVersionUID = 1L;

    private final String sourceUri;
    private final String targetUri;
    private final String sourceDb;
    private final String targetDb;
    private final String sourceReadPreferenceName;

    public LiveMongoSliceReaderFactory(
        String sourceUri,
        String targetUri,
        String sourceDb,
        String targetDb,
        String sourceReadPreferenceName) {
      this.sourceUri = sourceUri;
      this.targetUri = targetUri;
      this.sourceDb = sourceDb;
      this.targetDb = targetDb;
      this.sourceReadPreferenceName = sourceReadPreferenceName;
    }

    private MongoCollection<RawBsonDocument> getCollection(
        boolean isTarget, String collection, boolean strongPrimaryRead) {
      if (isTarget) {
        MongoClient tgtClient = MongoClientManager.getOrCreate(targetUri, ReadPreference.primary());
        return tgtClient
            .getDatabase(targetDb)
            .getCollection(collection, RawBsonDocument.class)
            .withReadPreference(ReadPreference.primary());
      }
      ReadPreference pref =
          strongPrimaryRead
              ? ReadPreference.primary()
              : MongoClientManager.parseReadPreference(sourceReadPreferenceName);
      MongoClient srcClient = MongoClientManager.getOrCreate(sourceUri, pref);
      MongoCollection<RawBsonDocument> col =
          srcClient
              .getDatabase(sourceDb)
              .getCollection(collection, RawBsonDocument.class)
              .withReadPreference(pref);
      if (strongPrimaryRead) {
        col = col.withReadConcern(ReadConcern.MAJORITY);
      }
      return col;
    }

    @Override
    public CloseableDocIterator openSortedCursor(
        boolean isTarget,
        String collection,
        BsonDocument baseFilter,
        @Nullable BsonValue resumeAfterExclusiveId,
        int batchSize,
        boolean idOnlyProjection) {
      BsonDocument effectiveFilter = buildEffectiveFilter(baseFilter, resumeAfterExclusiveId);
      FindIterable<RawBsonDocument> iterable =
          getCollection(isTarget, collection, /* strongPrimaryRead= */ false)
              .find(effectiveFilter)
              .sort(new BsonDocument("_id", new BsonInt32(1)))
              .batchSize(Math.max(100, batchSize));
      if (idOnlyProjection) {
        iterable = iterable.projection(new BsonDocument("_id", new BsonInt32(1)));
      }
      return wrapCursor(iterable.cursor());
    }

    @Override
    public CloseableDocIterator openUnorderedCursor(
        boolean isTarget,
        String collection,
        BsonDocument filter,
        int batchSize,
        boolean idOnlyProjection) {
      FindIterable<RawBsonDocument> iterable =
          getCollection(isTarget, collection, /* strongPrimaryRead= */ false)
              .find(filter)
              .batchSize(Math.max(100, batchSize));
      if (idOnlyProjection) {
        iterable = iterable.projection(new BsonDocument("_id", new BsonInt32(1)));
      }
      return wrapCursor(iterable.cursor());
    }

    @Override
    public Map<ByteBuffer, BsonDocument> fetchBatchByIds(
        boolean isTarget,
        String collection,
        List<BsonValue> ids,
        boolean strongPrimaryRead,
        boolean idOnlyProjection) {
      Map<ByteBuffer, BsonDocument> out = new HashMap<>(ids.size() * 2);
      if (ids.isEmpty()) {
        return out;
      }
      BsonDocument filter =
          new BsonDocument("_id", new BsonDocument("$in", new BsonArray(new ArrayList<>(ids))));
      FindIterable<RawBsonDocument> iterable =
          getCollection(isTarget, collection, strongPrimaryRead)
              .find(filter)
              .batchSize(Math.max(100, ids.size()));
      if (idOnlyProjection) {
        iterable = iterable.projection(new BsonDocument("_id", new BsonInt32(1)));
      }
      try (MongoCursor<RawBsonDocument> cursor = iterable.cursor()) {
        while (cursor.hasNext()) {
          RawBsonDocument doc = cursor.next();
          BsonValue id = doc.get("_id");
          if (id != null) {
            out.put(ByteBuffer.wrap(BsonEncodingUtils.encodeIdBytes(id)), doc);
          }
        }
      }
      return out;
    }

    static BsonDocument buildEffectiveFilter(
        BsonDocument baseFilter, @Nullable BsonValue resumeAfterExclusiveId) {
      if (resumeAfterExclusiveId == null) {
        return baseFilter;
      }
      BsonDocument resumePred =
          new BsonDocument("_id", new BsonDocument("$gt", resumeAfterExclusiveId));
      if (baseFilter == null || baseFilter.isEmpty()) {
        return resumePred;
      }
      BsonArray andClauses = new BsonArray();
      andClauses.add(baseFilter);
      andClauses.add(resumePred);
      return new BsonDocument("$and", andClauses);
    }

    private static CloseableDocIterator wrapCursor(MongoCursor<RawBsonDocument> cursor) {
      return new CloseableDocIterator() {
        @Override
        public boolean hasNext() {
          return cursor.hasNext();
        }

        @Override
        public BsonDocument next() {
          return cursor.next();
        }

        @Override
        public void close() {
          cursor.close();
        }
      };
    }
  }
}
