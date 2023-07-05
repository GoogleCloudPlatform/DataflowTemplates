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
package com.google.cloud.teleport.plugin.maven;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/** Base class for all Maven plugin goals used by Templates. */
public abstract class TemplatesBaseMojo extends AbstractMojo {

  @Parameter(defaultValue = "${project}", required = true)
  protected MavenProject project;

  @Parameter(defaultValue = "${session}", required = true)
  protected MavenSession session;

  /** Location of the output directory. */
  @Parameter(defaultValue = "${project.build.directory}", property = "outputDir", required = true)
  protected File outputDirectory;

  @Parameter(
      defaultValue = "${project.build.directory}/classes",
      property = "outputClassesDir",
      required = true)
  protected File outputClassesDirectory;

  @Parameter(
      defaultValue = "${project.build.resources[0].directory}",
      property = "resourcesDir",
      required = true)
  protected File resourcesDirectory;

  @Parameter(defaultValue = "${project.build.directory}", property = "buildDir", required = true)
  protected File targetDirectory;

  @Parameter(defaultValue = "${project.basedir}", property = "baseDir", required = true)
  protected File baseDirectory;

  protected URLClassLoader buildClassloader()
      throws DependencyResolutionRequiredException, MalformedURLException, MojoExecutionException {
    List<String> classpathElements = project.getCompileClasspathElements();

    List<URL> projectClasspathList = new ArrayList<>();
    projectClasspathList.add(
        new File(
                outputDirectory,
                "original-" + project.getArtifactId() + "-" + project.getVersion() + ".jar")
            .toURI()
            .toURL());

    for (String element : classpathElements) {
      try {
        projectClasspathList.add(new File(element).toURI().toURL());
      } catch (MalformedURLException e) {
        throw new MojoExecutionException(element + " is an invalid classpath element", e);
      }
    }

    URLClassLoader loader =
        new URLClassLoader(
            projectClasspathList.toArray(new URL[0]),
            Thread.currentThread().getContextClassLoader());
    return loader;
  }
}
