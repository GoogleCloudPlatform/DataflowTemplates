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
package com.google.cloud.teleport.metadata.auto;

import com.google.cloud.teleport.metadata.Template;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** AutoTemplate, the class which builds the pipeline based on the Template blocks. */
public class AutoTemplate {

  private static final Logger LOG = LoggerFactory.getLogger(AutoTemplate.class);

  public static void setup(
      Class<?> templateClass, String[] args, Preprocessor<PipelineOptions> preprocess) {
    LOG.debug("Starting automatic template for template {}...", templateClass);

    try {
      List<ExecutionBlock> orderedBlocks = buildExecutionBlocks(templateClass);

      TemplateBlock dlqInstance = getDlqInstance(templateClass);

      Class<? extends PipelineOptions> newOptionsClass =
          createNewOptionsClass(orderedBlocks, AutoTemplate.class.getClassLoader(), dlqInstance);

      LOG.debug("Created options class {}", newOptionsClass);

      PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(newOptionsClass);

      preprocess.accept(options);

      Pipeline pipeline = Pipeline.create(options);

      LOG.debug("Parsed options {}", options);
      Object input = null;

      ExecutionBlock sourceBlock = orderedBlocks.remove(0);
      TemplateBlock sourceBlockInstance = sourceBlock.blockInstance;
      input =
          sourceBlock.blockMethod.invoke(
              sourceBlock.blockInstance,
              pipeline.begin(),
              options.as(sourceBlockInstance.getOptionsClass()));

      for (ExecutionBlock executionBlock : orderedBlocks) {
        TemplateBlock blockInstance = executionBlock.blockInstance;
        PipelineOptions optionsClass = options.as(blockInstance.getOptionsClass());

        input =
            executionBlock.blockMethod.invoke(executionBlock.blockInstance, input, optionsClass);

        if (dlqInstance != null) {
          ExecutionBlock dlqBlock;
          if (dlqOutputs(executionBlock.getBlockMethod()) != null) {
            dlqBlock =
                buildDlqExecutionBlock(templateClass, dlqOutputs(executionBlock.getBlockMethod()));
            dlqBlock.blockMethod.invoke(
                dlqBlock.blockInstance, input, options.as(dlqInstance.getOptionsClass()));
          }
        }
      }

      pipeline.run();

    } catch (Exception e) {
      throw new RuntimeException("Error instantiating source", e);
    }
  }

  public static List<ExecutionBlock> buildExecutionBlocks(Class<?> templateClass)
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {

    Template annotations = templateClass.getAnnotation(Template.class);
    if (annotations == null) {
      throw new IllegalStateException(
          "Class "
              + templateClass
              + " does not have a @Template annotation, can not use auto template features.");
    }
    Class<?>[] blocks = annotations.blocks();
    if (blocks.length == 0) {
      throw new IllegalStateException(
          "Class "
              + templateClass
              + " does not have a @Template annotation with valid blocks, can not use auto template features.");
    }

    List<Class<?>> chainedClasses = Arrays.asList(blocks);
    LOG.debug("Chaining blocks {}", chainedClasses);

    Class<? extends TemplateTransform> sourceBlockClass =
        chainedClasses.get(0).asSubclass(TemplateTransform.class);
    TemplateTransform<?> templateSourceInstance =
        sourceBlockClass.getDeclaredConstructor().newInstance();

    Method sourceMethod = getReadMethod(sourceBlockClass);
    LOG.debug("Going to read source method {}", sourceMethod);

    List<Class<?>> transformations = chainedClasses.subList(1, chainedClasses.size() - 1);
    LOG.debug("Transformations are {}", transformations);

    Outputs previousType = outputs(sourceMethod);

    List<ExecutionBlock> orderedBlocks = new ArrayList<>();
    orderedBlocks.add(new ExecutionBlock(sourceBlockClass, templateSourceInstance, sourceMethod));

    for (Class<?> transformationClass : transformations) {

      LOG.debug("Returned type from the previous block: {}", previousType);

      Method transformMethod =
          getTransformMethod(transformationClass, previousType.value(), previousType.types());

      LOG.debug("Next method will be {}", transformMethod);

      TemplateTransform<?> transformInstance =
          transformationClass
              .asSubclass(TemplateTransform.class)
              .getDeclaredConstructor()
              .newInstance();

      orderedBlocks.add(
          new ExecutionBlock(transformationClass, transformInstance, transformMethod));
      previousType = outputs(transformMethod);
    }

    Class<?> sinkClass = chainedClasses.get(chainedClasses.size() - 1);
    LOG.debug("Sink is {}", sinkClass);

    Method sinkMethod = getTransformMethod(sinkClass, previousType.value(), previousType.types());
    TemplateTransform<?> sinkInstance =
        sinkClass.asSubclass(TemplateTransform.class).getDeclaredConstructor().newInstance();

    orderedBlocks.add(new ExecutionBlock(sinkClass, sinkInstance, sinkMethod));
    return orderedBlocks;
  }

  public static ExecutionBlock buildDlqExecutionBlock(Class<?> templateClass, DlqOutputs output)
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {

    Template annotations = templateClass.getAnnotation(Template.class);
    if (annotations == null) {
      throw new IllegalStateException(
          "Class "
              + templateClass
              + " does not have a @Template annotation, can not use auto template features.");
    }
    Class<?> dlqBlock = annotations.dlqBlock();
    if (dlqBlock == null) {
      return null;
    }

    LOG.debug("Found DLQ Block {}", dlqBlock);

    Class<? extends TemplateTransform> dlqBlockClass = dlqBlock.asSubclass(TemplateTransform.class);
    TemplateTransform<?> templateDlqInstance = dlqBlockClass.getDeclaredConstructor().newInstance();

    Method dlqMethod = getTransformMethod(dlqBlockClass, output.value(), output.types());

    LOG.debug("Dlq method will be {}", dlqMethod);

    return new ExecutionBlock(dlqBlockClass, templateDlqInstance, dlqMethod);
  }

  public static TemplateTransform<?> getDlqInstance(Class<?> templateClass)
      throws InstantiationException,
          IllegalAccessException,
          InvocationTargetException,
          NoSuchMethodException {
    Template annotations = templateClass.getAnnotation(Template.class);
    if (annotations == null) {
      throw new IllegalStateException(
          "Class "
              + templateClass
              + " does not have a @Template annotation, can not use auto template features.");
    }
    Class<?> dlqBlock = annotations.dlqBlock();
    if (dlqBlock.equals(void.class)) {
      return null;
    }

    Class<? extends TemplateTransform> dlqBlockClass = dlqBlock.asSubclass(TemplateTransform.class);
    TemplateTransform<?> templateDlqInstance = dlqBlockClass.getDeclaredConstructor().newInstance();
    return templateDlqInstance;
  }

  private static Method getReadMethod(Class<? extends TemplateTransform> clazz) {
    for (Method method : clazz.getDeclaredMethods()) {
      if (method.getName().equals("read")) {
        return method;
      }
    }

    throw new IllegalStateException("Class " + clazz + " does not have a read implementation");
  }

  private static Method getTransformMethod(Class<?> clazz, Class<?> input, Class<?>[] types) {
    for (Method method : clazz.getMethods()) {
      Consumes annotation = method.getAnnotation(Consumes.class);
      if (annotation != null) {
        LOG.debug(
            "Class {} has method to consume {} with types {}",
            clazz,
            annotation.value(),
            annotation.types());

        if (annotation.value() == input && Arrays.equals(annotation.types(), types)) {
          return method;
        }
      }
    }

    throw new IllegalStateException(
        "Class " + clazz + " does not have a transform implementation for " + input);
  }

  private static Outputs outputs(Method method) {
    if (method.getAnnotation(Outputs.class) == null) {
      throw new IllegalStateException("Method " + method + " does not implement @Outputs");
    }
    return method.getAnnotation(Outputs.class);
  }

  private static DlqOutputs dlqOutputs(Method method) {
    if (method.getAnnotation(DlqOutputs.class) == null) {
      return null;
    }
    return method.getAnnotation(DlqOutputs.class);
  }

  public static Class<? extends PipelineOptions> createNewOptionsClass(
      Collection<ExecutionBlock> blocks, ClassLoader loader, TemplateBlock dlqInstance) {

    LOG.debug("Creating new options class to implement {}", blocks);

    DynamicType.Builder<PipelineOptions> allOptionsClassBuilder =
        new ByteBuddy().makeInterface(PipelineOptions.class).name("AllOptionsClass");

    for (ExecutionBlock executionBlock : blocks) {
      allOptionsClassBuilder =
          allOptionsClassBuilder.implement(executionBlock.blockInstance.getOptionsClass());
    }
    if (dlqInstance != null) {
      allOptionsClassBuilder = allOptionsClassBuilder.implement(dlqInstance.getOptionsClass());
    }
    return allOptionsClassBuilder.make().load(loader).getLoaded();
  }

  public static class ExecutionBlock {

    private Class<?> blockClass;
    private TemplateBlock<?> blockInstance;
    private Method blockMethod;

    public ExecutionBlock(Class<?> blockClass, TemplateBlock<?> blockInstance, Method blockMethod) {
      this.blockClass = blockClass;
      this.blockInstance = blockInstance;
      this.blockMethod = blockMethod;
    }

    public Class<?> getBlockClass() {
      return blockClass;
    }

    public void setBlockClass(Class<?> blockClass) {
      this.blockClass = blockClass;
    }

    public TemplateBlock<?> getBlockInstance() {
      return blockInstance;
    }

    public void setBlockInstance(TemplateBlock<?> blockInstance) {
      this.blockInstance = blockInstance;
    }

    public Method getBlockMethod() {
      return blockMethod;
    }

    public void setBlockMethod(Method blockMethod) {
      this.blockMethod = blockMethod;
    }
  }
}
