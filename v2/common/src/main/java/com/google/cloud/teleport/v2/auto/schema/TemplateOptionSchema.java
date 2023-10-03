/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.auto.schema;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link SchemaProvider} for AutoValue classes. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class TemplateOptionSchema extends GetterBasedSchemaProvider {
  /** {@link FieldValueTypeSupplier} that's based on AutoValue getters. */
  @VisibleForTesting
  public static class AbstractGetterTypeSupplier implements FieldValueTypeSupplier {
    public static final AbstractGetterTypeSupplier INSTANCE = new AbstractGetterTypeSupplier();

    @Override
    public List<FieldValueTypeInformation> get(Class<?> clazz) {

      List<Method> methods =
          ReflectUtils.getMethods(clazz).stream()
              .filter(ReflectUtils::isGetter)
              // All AutoValue getters are marked abstract.
              .filter(m -> Modifier.isAbstract(m.getModifiers()))
              .filter(m -> !Modifier.isPrivate(m.getModifiers()))
              .filter(m -> !Modifier.isProtected(m.getModifiers()))
              .filter(m -> !m.isAnnotationPresent(SchemaIgnore.class))
              .collect(Collectors.toList());
      List<FieldValueTypeInformation> types = Lists.newArrayListWithCapacity(methods.size());
      for (int i = 0; i < methods.size(); ++i) {
        types.add(FieldValueTypeInformation.forGetter(methods.get(i), i));
      }
      types.sort(Comparator.comparing(FieldValueTypeInformation::getNumber));
      validateFieldNumbers(types);
      return types;
    }
  }

  private static void validateFieldNumbers(List<FieldValueTypeInformation> types) {
    for (int i = 0; i < types.size(); ++i) {
      FieldValueTypeInformation type = types.get(i);
      @javax.annotation.Nullable Integer number = type.getNumber();
      if (number == null) {
        throw new RuntimeException("Unexpected null number for " + type.getName());
      }
      Preconditions.checkState(
          number == i,
          "Expected field number "
              + i
              + " for field + "
              + type.getName()
              + " instead got "
              + number);
    }
  }

  @Override
  public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
    return JavaBeanUtils.getGetters(
        targetClass,
        schema,
        AbstractGetterTypeSupplier.INSTANCE,
        new DefaultTypeConversionsFactory());
  }

  @Override
  public List<FieldValueTypeInformation> fieldValueTypeInformations(
      Class<?> targetClass, Schema schema) {
    return JavaBeanUtils.getFieldTypes(targetClass, schema, AbstractGetterTypeSupplier.INSTANCE);
  }

  @Override
  public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
    return params -> {
      List<FieldValueTypeInformation> fieldValueTypeInformations =
          AbstractGetterTypeSupplier.INSTANCE.get(targetClass);

      List<String> args = new ArrayList<>();
      for (int i = 0; i < fieldValueTypeInformations.size(); i++) {
        if (params[i] != null) {
          args.add("--" + fieldValueTypeInformations.get(i).getName() + "=" + params[i].toString());
        }
      }
      return PipelineOptionsFactory.fromArgs(args.toArray(new String[args.size()]));
    };
  }

  @Override
  public <T> @Nullable Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return JavaBeanUtils.schemaFromJavaBeanClass(
        typeDescriptor.getRawType(), AbstractGetterTypeSupplier.INSTANCE);
  }
}
