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
package com.google.cloud.syndeo.common;

import com.google.cloud.syndeo.transforms.SyndeoStatsSchemaTransformProvider;
import com.google.cloud.syndeo.v1.SyndeoV1.ConfiguredSchemaTransform;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

public class ProviderUtil {

  public static final Map<String, SchemaTransformProvider> PROVIDERS = loadProviders();
  public static final String ERRORS_TAG = "errors";
  public static final String OUTPUT_TAG = "output";

  /** Load providers, including bringing in one for SchemaIOs. */
  private static Map<String, SchemaTransformProvider> loadProviders() {
    ServiceLoader<SchemaTransformProvider> provider =
        ServiceLoader.load(SchemaTransformProvider.class);
    List<SchemaTransformProvider> list =
        StreamSupport.stream(provider.spliterator(), false).collect(Collectors.toList());
    list.addAll(SchemaIOTransformProviderWrapper.getAll());

    Map<String, SchemaTransformProvider> map = new HashMap<>();
    for (SchemaTransformProvider p : list) {
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
      if (provider == null) {
        throw new RuntimeException(inputId + " not found ");
      }
      try {
        configuration =
            Row.withSchema(provider.configurationSchema()).addValues(configurationAsList).build();
      } catch (ClassCastException | IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Given config schema %s, and configuration %s, we're unable to configure transform.",
                provider.configurationSchema().getFields().stream()
                    .map(Schema.Field::getName)
                    .collect(Collectors.toList()),
                configurationAsList),
            e);
      }
    }

    public TransformSpec(ConfiguredSchemaTransform schemaTransform) {
      inputId = schemaTransform.getTransformUrn();
      provider = getProvider(inputId);
      if (provider == null) {
        throw new IllegalArgumentException(inputId + " not found ");
      }
      // TODO: Test handling of rows with missing fields.
      configuration =
          SchemaUtil.addNullsToMatchSchema(
              (Row)
                  SchemaTranslation.rowFromProto(
                      SyndeoApiProtoTranslation.fromSyndeoProtos(
                          schemaTransform.getConfigurationValues()),
                      FieldType.row(provider.configurationSchema())),
              provider.configurationSchema());
    }

    public ConfiguredSchemaTransform toProto() {
      ConfiguredSchemaTransform.Builder inst = ConfiguredSchemaTransform.newBuilder();
      inst.setConfigurationValues(
          SyndeoApiProtoTranslation.toSyndeoProtos(SchemaTranslation.rowToProto(configuration)));
      inst.setConfigurationOptions(
          SyndeoApiProtoTranslation.toSyndeoProtos(
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
      if (tuple.getAll().size() == 1 || tuple.getAll().containsKey(OUTPUT_TAG)) {
        String input = spec.provider.inputCollectionNames().get(0);
        // TODO(pabloem): Check this logic.
        String priorOutput =
            tuple.getAll().size() == 1
                ? tuple.getAll().keySet().stream().findFirst().get()
                : OUTPUT_TAG;
        tuple = PCollectionRowTuple.of(input, tuple.get(priorOutput));
      }

      tuple = tuple.apply(spec.inputId, transform.buildTransform());

      if (tuple.getAll().containsKey(ERRORS_TAG)) {
        // TODO(pabloem): Deal with errors.
        PCollectionRowTuple.of("input", tuple.get(ERRORS_TAG))
            .apply(
                new SyndeoStatsSchemaTransformProvider()
                    .from(
                        SyndeoStatsSchemaTransformProvider.SyndeoStatsConfiguration.create(
                            "errors"))
                    .buildTransform());
      }
    }
    return tuple;
  }
}
