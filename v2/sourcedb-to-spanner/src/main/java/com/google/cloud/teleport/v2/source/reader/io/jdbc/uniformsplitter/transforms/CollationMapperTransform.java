/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProvider;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationMapper;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Generate the Side-Input that encodes the Collation Mapping information for given instance of
 * {@link ReadWithUniformPartitions}. Note: this PTransform Round-robins collations across available
 * shards. All shards of for a given migration are assumed to have the same collation mapping. It's
 * unlikely for different shards to have a different mapping of collations, this assumption helps us
 * avoid the need for maintaining a per-Shard map of collation (which would add significant
 * presssure on the worker memory). It also helps us parallelize the discovery across shards to get
 * better performance.
 */
@AutoValue
public abstract class CollationMapperTransform
    extends PTransform<PBegin, PCollectionView<Map<CollationReference, CollationMapper>>>
    implements Serializable {

  /** List of {@link CollationReference} to discover the mapping for. */
  public abstract ImmutableList<KV<String, CollationReference>> collationReferences();

  /** Provider for connection pool. */
  public abstract DataSourceProvider dataSourceProvider();

  /** Provider to dialect specific Collation mapping query. */
  public abstract UniformSplitterDBAdapter dbAdapter();

  /**
   * Generate the Side-Input that encodes the Collation Mapping information for given instance of
   * {@link ReadWithUniformPartitions}.
   *
   * @param input PBegin
   * @return {@link PCollectionView} for discovered {@link CollationReference}, {@link
   *     CollationMapper} pairs.
   */
  @Override
  public PCollectionView<Map<CollationReference, CollationMapper>> expand(PBegin input) {
    try {
      if (collationReferences().isEmpty()) {
        return input
            .apply(
                Create.empty(
                    KvCoder.of(
                        input.getPipeline().getCoderRegistry().getCoder(CollationReference.class),
                        input.getPipeline().getCoderRegistry().getCoder(CollationMapper.class))))
            .apply("To Empty Map View", View.asMap());
      }
      return input
          .apply("Create-Collation-References", Create.of(collationReferences()))
          .apply(
              "Generate-Mappers",
              ParDo.of(new CollationMapperDoFn(dataSourceProvider(), dbAdapter())))
          .setCoder(
              KvCoder.of(
                  input.getPipeline().getCoderRegistry().getCoder(CollationReference.class),
                  input.getPipeline().getCoderRegistry().getCoder(CollationMapper.class)))
          .apply("CollationMapperView", View.asMap());
    } catch (CannotProvideCoderException e) {
      // This line is hard to unit test as the coders for serializable classes will be available.
      throw new RuntimeException(e);
    }
  }

  public static Builder builder() {
    return new AutoValue_CollationMapperTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    private ImmutableList<CollationReference> collationReferencesToDiscover;

    public Builder setCollationReferencesToDiscover(ImmutableList<CollationReference> value) {
      this.collationReferencesToDiscover = value;
      return this;
    }

    abstract Builder setCollationReferences(ImmutableList<KV<String, CollationReference>> value);

    public abstract Builder setDataSourceProvider(DataSourceProvider value);

    abstract DataSourceProvider dataSourceProvider();

    public abstract Builder setDbAdapter(UniformSplitterDBAdapter value);

    public abstract CollationMapperTransform autoBuild();

    public CollationMapperTransform build() {
      ImmutableList<CollationReference> deDupedRefs =
          collationReferencesToDiscover.stream()
              .distinct()
              .collect(ImmutableList.toImmutableList());
      ImmutableList<String> ids =
          ImmutableList.copyOf(this.dataSourceProvider().getDataSourceIds());
      Preconditions.checkState(ids.size() > 0, "No DataSources Configured for collation detection");
      ImmutableList.Builder<KV<String, CollationReference>> collationReferencesBuilder =
          ImmutableList.builder();
      // Round-robin collations across available shards.
      // All shards of the same database should have the same collation mapping.
      for (int i = 0; i < deDupedRefs.size(); i++) {
        collationReferencesBuilder.add(KV.of(ids.get(i % ids.size()), deDupedRefs.get(i)));
      }
      this.setCollationReferences(collationReferencesBuilder.build());
      return autoBuild();
    }
  }
}
