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

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.util.MetadataUtils;
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.AsmVisitorWrapper;
import net.bytebuddy.asm.MemberAttributeExtension;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
import org.apache.beam.sdk.schemas.utils.FieldValueTypeSupplier;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.beam.sdk.schemas.FieldValueTypeInformation.getNameOverride;
import static org.apache.beam.sdk.schemas.FieldValueTypeInformation.getNumberOverride;

/** A {@link SchemaProvider} for Template Block classes. */
public class TemplateOptionSchema extends AutoValueSchema {
  @Override
  public <T> Schema schemaFor(@NonNull TypeDescriptor<T> typeDescriptor) {
    return null;
  }

  /** {@link TemplateGetterTypeSupplier} that's based on PipelineOptions getters. */
  @VisibleForTesting
  public static class TemplateGetterTypeSupplier implements FieldValueTypeSupplier {
    public static final TemplateGetterTypeSupplier INSTANCE = new TemplateGetterTypeSupplier();

    public @NonNull List<@NonNull FieldValueTypeInformation> get(Class<?> clazz) {

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

//      methods.forEach(method -> {
//        MemberAttributeExtension.ForMethod forMethod = new MemberAttributeExtension.ForMethod()
//            .annotateMethod(method.getAnnotations());
//        if (TemplateFieldValueTypeInformation.hasNullableReturnType(method)) {
//          forMethod = forMethod.annotateMethod(AnnotationDescription.Builder.ofType(Nullable.class).build());
//        }
//
////        ClassReloadingStrategy classReloadingStrategy = ClassReloadingStrategy.fromInstalledAgent();
//
//        new ByteBuddy()
//            .redefine(clazz)
//            .visit(forMethod.on(ElementMatchers.anyOf(method))).make();
////            .load(clazz.getClassLoader(), classReloadingStrategy);
//
////        try {
////          classReloadingStrategy.reset(clazz);
////        } catch (IOException e) {
////          throw new RuntimeException(e);
////        }
//      });

      for (int i = 0; i < methods.size(); ++i) {
        types.add(FieldValueTypeInformation.forGetter(methods.get(i), i));
      }

      try {
        types.add(FieldValueTypeInformation.forGetter(clazz.getMethod("getErrorHandling"), methods.size()));
      } catch (NoSuchMethodException e) {
        if (!clazz.equals(TemplateTransformClass.TemplateBlockOptions.ErrorHandling.class)) {
          throw new RuntimeException(e);
        }
      }
      types.sort(Comparator.comparing(FieldValueTypeInformation::getNumber));
      validateFieldNumbers(types);
      return types;
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
  }

  @Override
  public @NonNull List<@NonNull FieldValueGetter> fieldValueGetters(@NonNull Class<@NonNull ?> targetClass, @NonNull Schema schema) {
    return JavaBeanUtils.getGetters(
        targetClass,
        schema,
        TemplateGetterTypeSupplier.INSTANCE,
        new ByteBuddyUtils.DefaultTypeConversionsFactory());
  }

  @Override
  public @NonNull List<@NonNull FieldValueTypeInformation> fieldValueTypeInformations(@NonNull Class<@NonNull ?> targetClass, @NonNull Schema schema) {
    return JavaBeanUtils.getFieldTypes(targetClass, schema, TemplateGetterTypeSupplier.INSTANCE);
  }

  @Override
  public @NonNull SchemaUserTypeCreator schemaTypeCreator(@NonNull Class<?> targetClass, @NonNull Schema schema) {
    if (targetClass.equals(TemplateTransformClass.TemplateBlockOptions.ErrorHandling.class)) {
      return super.schemaTypeCreator(targetClass, schema);
    }
    return params -> {
      List<FieldValueTypeInformation> TemplateFieldValueTypeInformation = TemplateGetterTypeSupplier.INSTANCE.get(targetClass);

      List<String> args = new ArrayList<>();
      for (int i = 0; i < TemplateFieldValueTypeInformation.size(); i++) {
        if (params[i] != null) {
          args.add("--" + schema.getField(i).getName() + "=" + params[i].toString());
        }
      }
      PipelineOptions result = PipelineOptionsFactory.fromArgs(args.toArray(new String[0]))
          .withValidation()
          .as((Class<? extends PipelineOptions>) targetClass);
      return result;

//      result.
    };
  }

  @AutoValue
  public abstract static class TemplateFieldValueTypeInformation implements Serializable {

    /** Optionally returns the field index. */
    public abstract @Nullable Integer getNumber();

    /** Returns the field name. */
    public abstract String getName();

    /** Returns whether the field is nullable. */
    public abstract boolean isNullable();

    /** Returns the field type. */
    public abstract TypeDescriptor<?> getType();

    /** Returns the raw class type. */
    public abstract Class<?> getRawType();

    public abstract @Nullable Field getField();

    public abstract @Nullable Method getMethod();

    public abstract Map<String, TemplateFieldValueTypeInformation> getOneOfTypes();

    /** If the field is a container type, returns the element type. */
    public abstract @Nullable TemplateFieldValueTypeInformation getElementType();

    /** If the field is a map type, returns the key type. */
    public abstract @Nullable TemplateFieldValueTypeInformation getMapKeyType();

    /** If the field is a map type, returns the key type. */
    public abstract @Nullable TemplateFieldValueTypeInformation getMapValueType();

    /** If the field has a description, returns the description for the field. */
    public abstract @Nullable String getDescription();

    public static <T extends AnnotatedElement & Member> @Nullable String getFieldDescription(
        Method method) {
      ImageSpecParameter parameter = getImageSpecParameter(method);
      return parameter.getHelpText();
    }

    private static boolean hasNullableReturnType(Method method) {
      ImageSpecParameter parameter = getImageSpecParameter(method);
      return parameter.getOptional();
    }

    private static ImageSpecParameter getImageSpecParameter(Method method) {
      Annotation parameterAnnotation = MetadataUtils.getParameterAnnotation(method);
      ImageSpecParameter parameter = new ImageSpecParameter();
      parameter.processParamType(parameterAnnotation);

      return parameter;
    }

    public static TemplateFieldValueTypeInformation forGetter(Method method, int index) {
      String name;
      if (method.getName().startsWith("get")) {
        name = ReflectUtils.stripPrefix(method.getName(), "get");
      } else if (method.getName().startsWith("is")) {
        name = ReflectUtils.stripPrefix(method.getName(), "is");
      } else {
        throw new RuntimeException("Getter has wrong prefix " + method.getName());
      }

      TypeDescriptor<?> type = TypeDescriptor.of(method.getGenericReturnType());
      boolean nullable = hasNullableReturnType(method);
      return new AutoValue_TemplateOptionSchema_TemplateFieldValueTypeInformation.Builder()
          .setName(getNameOverride(name, method))
          .setNumber(getNumberOverride(index, method))
          .setNullable(nullable)
          .setType(type)
          .setRawType(type.getRawType())
          .setMethod(method)
          .setElementType(getIterableComponentType(type))
          .setMapKeyType(getMapKeyType(type))
          .setMapValueType(getMapValueType(type))
          .setOneOfTypes(Collections.emptyMap())
          .setDescription(getFieldDescription(method))
          .build();
    }

    public static TemplateFieldValueTypeInformation from(FieldValueTypeInformation fieldValueTypeInformation) {
      if (fieldValueTypeInformation == null) {
        return null;
      }
      return new AutoValue_TemplateOptionSchema_TemplateFieldValueTypeInformation.Builder()
          .setName(fieldValueTypeInformation.getName())
          .setNumber(fieldValueTypeInformation.getNumber())
          .setNullable(fieldValueTypeInformation.isNullable())
          .setType(fieldValueTypeInformation.getType())
          .setRawType(fieldValueTypeInformation.getRawType())
          .setMethod(fieldValueTypeInformation.getMethod())
          .setElementType(from(fieldValueTypeInformation.getElementType()))
          .setMapKeyType(from(fieldValueTypeInformation.getMapKeyType()))
          .setMapValueType(from(fieldValueTypeInformation.getMapValueType()))
          .setOneOfTypes(fieldValueTypeInformation.getOneOfTypes().entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> from(e.getValue()))))
          .setDescription(fieldValueTypeInformation.getDescription())
          .build();
    }

    private static @Nullable TemplateFieldValueTypeInformation getMapKeyType(
        TypeDescriptor<?> typeDescriptor) {
      return getMapType(typeDescriptor, 0);
    }

    private static @Nullable TemplateFieldValueTypeInformation getMapValueType(
        TypeDescriptor<?> typeDescriptor) {
      return getMapType(typeDescriptor, 1);
    }

    private static @Nullable TemplateFieldValueTypeInformation getMapType(
        TypeDescriptor<?> valueType, int index) {
      TypeDescriptor<?> mapType = ReflectUtils.getMapType(valueType, index);
      return new AutoValue_TemplateOptionSchema_TemplateFieldValueTypeInformation.Builder()
          .setName("")
          .setNullable(false)
          .setType(mapType)
          .setRawType(mapType.getRawType())
          .setElementType(getIterableComponentType(mapType))
          .setMapKeyType(getMapKeyType(mapType))
          .setMapValueType(getMapValueType(mapType))
          .setOneOfTypes(Collections.emptyMap())
          .build();
    }

    private static TemplateFieldValueTypeInformation getIterableComponentType(Field field) {
      return getIterableComponentType(TypeDescriptor.of(field.getGenericType()));
    }

    static @Nullable TemplateFieldValueTypeInformation getIterableComponentType(TypeDescriptor<?> valueType) {
      // TODO: Figure out nullable elements.
      TypeDescriptor<?> componentType = ReflectUtils.getIterableComponentType(valueType);
      if (componentType == null) {
        return null;
      }

      return new AutoValue_TemplateOptionSchema_TemplateFieldValueTypeInformation.Builder()
          .setName("")
          .setNullable(false)
          .setType(componentType)
          .setRawType(componentType.getRawType())
          .setElementType(getIterableComponentType(componentType))
          .setMapKeyType(getMapKeyType(componentType))
          .setMapValueType(getMapValueType(componentType))
          .setOneOfTypes(Collections.emptyMap())
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setNumber(@Nullable Integer number);

      public abstract Builder setName(String name);

      public abstract Builder setNullable(boolean nullable);

      public abstract Builder setType(TypeDescriptor<?> type);

      public abstract Builder setRawType(Class<?> type);

      public abstract Builder setField(@Nullable Field field);

      public abstract Builder setMethod(@Nullable Method method);

      public abstract Builder setOneOfTypes(Map<String, TemplateFieldValueTypeInformation> oneOfTypes);

      public abstract Builder setElementType(@Nullable TemplateFieldValueTypeInformation elementType);

      public abstract Builder setMapKeyType(@Nullable TemplateFieldValueTypeInformation mapKeyType);

      public abstract Builder setMapValueType(@Nullable TemplateFieldValueTypeInformation mapValueType);

      public abstract Builder setDescription(@Nullable String fieldDescription);

      abstract TemplateFieldValueTypeInformation build();
    }
  }
}
