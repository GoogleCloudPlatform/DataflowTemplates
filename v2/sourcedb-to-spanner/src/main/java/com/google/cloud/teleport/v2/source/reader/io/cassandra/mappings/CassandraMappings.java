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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.mappings;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraFieldMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueArrayMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueExtractor;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueExtractorV4;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueMapMapper;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.rowmapper.CassandraRowValueMapper;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapping;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.provider.unified.UnifiedMappingProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.avro.Schema;

/** Represent Unified type mapping, value extractor and value mappings for Cassandra. */
@AutoValue
public abstract class CassandraMappings {
  public abstract ImmutableMap<String, UnifiedTypeMapping> typeMapping();

  public abstract ImmutableMap<String, CassandraFieldMapper<?>> fieldMapping();

  abstract ImmutableMap<String, Class> typeToClassMapping();

  abstract ImmutableMap<String, Class> typeToClassMappingV4();

  public static Builder builder() {
    return new AutoValue_CassandraMappings.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    abstract ImmutableMap.Builder<String, UnifiedTypeMapping> typeMappingBuilder();

    abstract ImmutableMap.Builder<String, CassandraFieldMapper<?>> fieldMappingBuilder();

    abstract ImmutableMap.Builder<String, Class> typeToClassMappingBuilder();

    abstract ImmutableMap.Builder<String, Class> typeToClassMappingV4Builder();

    /**
     * Maintain mappings for a given type, as primitive as well as part of collections.
     *
     * @param cassandraType - name of the cassandra type, as discovered by the schema discovery.
     * @param type - Unified mapping type.
     * @param rowValueExtractor - {@link CassandraRowValueExtractor} to extract value from {@link
     *     com.datastax.driver.core.Row Cassandra Row}
     * @param rowValueMapper - {@link CassandraRowValueMapper} to map value to {@link
     *     com.google.cloud.teleport.v2.source.reader.io.row.SourceRow}
     * @param typeClass - Class of the extracted value. Generally return type of the
     *     rowValueExtractor.
     * @return Builder
     */
    public <T> Builder put(
        String cassandraType,
        UnifiedMappingProvider.Type type,
        CassandraRowValueExtractor<T> rowValueExtractor,
        CassandraRowValueExtractorV4<T> rowValueExtractorV4,
        CassandraRowValueMapper<T> rowValueMapper,
        Class<T> typeClass) {
      return this.put(
          cassandraType,
          type,
          rowValueExtractor,
          rowValueExtractorV4,
          rowValueMapper,
          rowValueMapper,
          typeClass,
          typeClass);
    }

    /**
     * Maintain mappings for a given type, as primitive as well as part of collections.
     *
     * @param cassandraType - name of the cassandra type, as discovered by the schema discovery.
     * @param type - Unified mapping type.
     * @param rowValueExtractor - {@link CassandraRowValueExtractor} to extract value from {@link
     *     com.datastax.driver.core.Row Cassandra Row}
     * @param rowValueMapper - {@link CassandraRowValueMapper} to map value to {@link
     *     com.google.cloud.teleport.v2.source.reader.io.row.SourceRow}
     * @param typeClass - Class of the extracted value. Generally return type of the
     *     rowValueExtractor.
     * @return Builder
     */
    public <T, V> Builder put(
        String cassandraType,
        UnifiedMappingProvider.Type type,
        CassandraRowValueExtractor<T> rowValueExtractor,
        CassandraRowValueExtractorV4<V> rowValueExtractorV4,
        CassandraRowValueMapper<T> rowValueMapper,
        CassandraRowValueMapper<V> rowValueMapperV4,
        Class<T> typeClass,
        Class<V> typeClassV4) {
      // typeClass is Null for "UNSUPPORTED" type.
      if (typeClass != null) {
        this.typeToClassMappingBuilder().put(cassandraType.toUpperCase(), typeClass);
      }

      if (typeClassV4 != null) {
        this.typeToClassMappingV4Builder().put(cassandraType.toUpperCase(), typeClassV4);
      }

      this.typeMappingBuilder()
          .put(cassandraType.toUpperCase(), UnifiedMappingProvider.getMapping(type));
      this.fieldMappingBuilder()
          .put(
              cassandraType.toUpperCase(),
              CassandraFieldMapper.create(
                  rowValueExtractor, rowValueExtractorV4, rowValueMapper, rowValueMapperV4));
      if (!type.equals(UnifiedMappingProvider.Type.UNSUPPORTED)) {
        putList(
            cassandraType,
            type,
            rowValueExtractor,
            rowValueMapper,
            rowValueMapperV4,
            typeClass,
            typeClassV4);
        putSet(
            cassandraType,
            type,
            rowValueExtractor,
            rowValueMapper,
            rowValueMapperV4,
            typeClass,
            typeClassV4);
      }
      return this;
    }

    private <T, V> void putList(
        String cassandraType,
        UnifiedMappingProvider.Type type,
        CassandraRowValueExtractor<T> rowValueExtractor,
        CassandraRowValueMapper<T> rowValueMapper,
        CassandraRowValueMapper<V> rowValueMapperV4,
        Class<T> typeClass,
        Class<V> typeClassV4) {
      String listType = "LIST<" + cassandraType.toUpperCase() + ">";
      this.typeMappingBuilder().put(listType, UnifiedMappingProvider.getArrayMapping(type));
      TypeToken<T> typeToken = TypeToken.of(typeClass);
      this.fieldMappingBuilder()
          .put(
              listType,
              CassandraFieldMapper.create(
                  (row, name) -> row.getList(name, typeToken),
                  (row, name) -> row.getList(name, typeClassV4),
                  CassandraRowValueArrayMapper.create(rowValueMapper),
                  CassandraRowValueArrayMapper.create(rowValueMapperV4)));
    }

    private <T, V> void putSet(
        String cassandraType,
        UnifiedMappingProvider.Type type,
        CassandraRowValueExtractor<T> rowValueExtractor,
        CassandraRowValueMapper<T> rowValueMapper,
        CassandraRowValueMapper<V> rowValueMapperV4,
        Class<T> typeClass,
        Class<V> typeClassV4) {
      String setType = "SET<" + cassandraType.toUpperCase() + ">";
      TypeToken<T> typeToken = TypeToken.of(typeClass);
      this.typeMappingBuilder().put(setType, UnifiedMappingProvider.getArrayMapping(type));
      this.fieldMappingBuilder()
          .put(
              setType,
              CassandraFieldMapper.create(
                  (row, name) -> row.getSet(name, typeToken),
                  (row, name) -> row.getSet(name, typeClassV4),
                  CassandraRowValueArrayMapper.create(rowValueMapper),
                  CassandraRowValueArrayMapper.create(rowValueMapperV4)));
    }

    abstract CassandraMappings autoBuild();

    public CassandraMappings build() {
      addMapBindings();
      return autoBuild();
    }

    /** Add bindings for handling Cassandra `Map` Types to Json. */
    private void addMapBindings() {
      /* Add Mappings for Map Types.*/
      ImmutableMap<String, Class> classMappings = typeToClassMappingBuilder().build();
      ImmutableMap<String, Class> classMappingsV4 = typeToClassMappingV4Builder().build();
      ImmutableMap<String, CassandraFieldMapper<?>> fieldMappers = fieldMappingBuilder().build();
      ImmutableMap<String, UnifiedTypeMapping> typeMappings = typeMappingBuilder().build();
      ImmutableSet<String> primitiveTypes = classMappings.keySet();

      for (String key : primitiveTypes) {
        Class keyClass = classMappings.get(key);
        Class keyClassV4 = classMappingsV4.get(key);
        for (String value : primitiveTypes) {
          // The Map type as represented in Cassandra Schema
          String mapType = "MAP<" + key.toUpperCase() + "," + value.toUpperCase() + ">";

          // Get the mappers for Key and Value.
          Class valueClass = classMappings.get(value);
          Class valueClassV4 = classMappingsV4.get(value);
          Schema keySchema = typeMappings.get(key).getSchema(new Long[] {}, new Long[] {});
          Schema valueSchema = typeMappings.get(value).getSchema(new Long[] {}, new Long[] {});
          CassandraRowValueMapper<?> keyValueMapper = fieldMappers.get(key).rowValueMapper();
          CassandraRowValueMapper<?> valueValueMapper = fieldMappers.get(value).rowValueMapper();

          CassandraRowValueMapper<?> keyValueMapperV4 = fieldMappers.get(key).rowValueMapperV4();
          CassandraRowValueMapper<?> valueValueMapperV4 =
              fieldMappers.get(value).rowValueMapperV4();

          // Add Schema Mapping for the Map Type.
          typeMappingBuilder()
              .put(mapType, UnifiedMappingProvider.getMapping(UnifiedMappingProvider.Type.JSON));
          // Add Value Mapping for the Map Type.
          fieldMappingBuilder()
              .put(
                  mapType,
                  CassandraFieldMapper.create(
                      (row, name) -> row.getMap(name, keyClass, valueClass),
                      (row, name) -> row.getMap(name, keyClassV4, valueClassV4),
                      CassandraRowValueMapMapper.create(
                          keyValueMapper, valueValueMapper, keySchema, valueSchema),
                      CassandraRowValueMapMapper.create(
                          keyValueMapperV4, valueValueMapperV4, keySchema, valueSchema)));
        }
      }
    }
  }
}
