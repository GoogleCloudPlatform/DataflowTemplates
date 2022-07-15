/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.syndeo.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.syndeo.v1.SyndeoV1.ConfiguredSchemaTransform;

public class ProviderUtil {

  public static final Map<String, SchemaTransformProvider> PROVIDERS = loadProviders();

  /** Load providers, including bringing in one for SchemaIOs. */
  private static Map<String, SchemaTransformProvider> loadProviders() {
    ServiceLoader<SchemaTransformProvider> provider =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> list =
        StreamSupport.stream(provider.spliterator(), false).collect(Collectors.toList());
    list.addAll(SchemaIOTransformProviderWrapper.getAll());

    Map<String, SchemaTransformProvider> map = new HashMap<>();
    for (SchemaTransformProvider p : list) {
      // System.out.println(p.identifier());
      map.put(p.identifier(), p);
    }

    return map;
  }

  /** Return a list of all providers. */
  public static Collection<SchemaTransformProvider> getProviders() {
    return PROVIDERS.values();
  }

  /** Get a specific provider. */
  public static SchemaTransformProvider getProvider(String name) {
    return PROVIDERS.getOrDefault(name, null);
  }

  /** A class for storing an io / configuration pair. */
  public static class TransformSpec {
    public String inputId;
    public Row configuration = null;
    public SchemaTransformProvider provider = null;

    public TransformSpec(String inputId, List<Object> configurationAsList) {
      this.inputId = inputId;
      provider = getProvider(inputId);
      if (provider == null) throw new RuntimeException(inputId + " not found ");
      configuration =
          Row.withSchema(provider.configurationSchema()).addValues(configurationAsList).build();
    }

    public TransformSpec(ConfiguredSchemaTransform schemaTransform) {
      inputId = schemaTransform.getTransformUrn();
      provider = getProvider(inputId);
      if (provider == null) throw new RuntimeException(inputId + " not found ");
      // TODO: Test handling of rows with missing fields.
      configuration =
          SchemaUtil.addNullsToMatchSchema(
              (Row)
                  SchemaTranslation.rowFromProto(
                      ProtoTranslation.fromSyndeoProtos(schemaTransform.getConfigurationValues()),
                      FieldType.row(provider.configurationSchema())),
              provider.configurationSchema());
    }

    public ConfiguredSchemaTransform toProto() {
      ConfiguredSchemaTransform.Builder inst = ConfiguredSchemaTransform.newBuilder();
      inst.setConfigurationValues(
          ProtoTranslation.toSyndeoProtos(SchemaTranslation.rowToProto(configuration)));
      inst.setConfigurationOptions(
          ProtoTranslation.toSyndeoProtos(
              SchemaTranslation.schemaToProto(configuration.getSchema(), true)));
      inst.setTransformUrn(inputId);
      return inst.build();
    }
  }

  /** Applies the given configs. */
  public static PCollectionRowTuple applyConfigs(
      Collection<TransformSpec> specs, PCollectionRowTuple tuple) {
    for (TransformSpec spec : specs) {
      SchemaTransform transform = spec.provider.from(spec.configuration);
      // We know we only deal with transforms with either 0 or 1 input so we know how to connect the
      // collections. To sanity check we should confirm the output collections match expected.
      if (tuple.getAll().size() == 1) {
        String input = spec.provider.inputCollectionNames().get(0);
        String priorOutput = tuple.getAll().keySet().stream().findFirst().get();
        tuple = PCollectionRowTuple.of(input, tuple.get(priorOutput));
      }

      tuple = tuple.apply(spec.inputId, transform.buildTransform());
    }
    return tuple;
  }
}
