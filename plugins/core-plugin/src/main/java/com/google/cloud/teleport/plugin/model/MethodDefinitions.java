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
package com.google.cloud.teleport.plugin.model;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Map;

/** POJO class that wraps the annotations that represent a Template option. */
public class MethodDefinitions implements Comparable<MethodDefinitions> {
  private Method definingMethod;
  private Annotation templateParameter;
  private Map<Class<?>, Integer> classOrder;

  public MethodDefinitions(
      Method definingMethod, Annotation templateParameter, Map<Class<?>, Integer> classOrder) {
    this.definingMethod = definingMethod;
    this.templateParameter = templateParameter;
    this.classOrder = classOrder;
  }

  public Method getDefiningMethod() {
    return definingMethod;
  }

  public Annotation getTemplateParameter() {
    return templateParameter;
  }

  public Map<Class<?>, Integer> getClassOrder() {
    return classOrder;
  }

  @Override
  public int compareTo(MethodDefinitions o) {
    int orderCompare =
        Integer.compare(
            classOrder.get(this.definingMethod.getDeclaringClass()),
            classOrder.get(o.definingMethod.getDeclaringClass()));
    if (orderCompare != 0) {
      return orderCompare;
    }

    try {
      Integer thisOrder =
          (Integer)
              this.templateParameter
                  .annotationType()
                  .getMethod("order")
                  .invoke(this.templateParameter);
      Integer oOrder =
          (Integer)
              o.templateParameter.annotationType().getMethod("order").invoke(o.templateParameter);

      int annotationCompare = Integer.compare(thisOrder, oOrder);
      if (annotationCompare != 0) {
        return annotationCompare;
      }

      return this.definingMethod.getName().compareTo(o.definingMethod.getName());

    } catch (Exception e) {
      throw new RuntimeException("Annotation does not contain order()", e);
    }
  }
}
