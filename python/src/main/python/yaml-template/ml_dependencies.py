#
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Additional runtime dependencies needed by ML transforms of YAML pipelines.

The YAML template installs these on the workers, in addition to its default
dependencies (`DEFAULT_DEPENDENCIES` in main.py), unless the caller provides a
`requirements_file` option. See `get_runtime_dependencies`.
"""

import argparse
import logging

import yaml

from apache_beam.yaml import main as yaml_main

# The YAML experimental feature that has to be enabled, using the
# `yaml_experimental_features` pipeline option, to use ML transforms (for
# example, RunInference and MLTransform) in a YAML pipeline.
ML_EXPERIMENTAL_FEATURE = 'ML'

# Additional runtime dependencies needed by RunInference model handlers, keyed
# by the model handler type.
#
# These (and the ones in `ML_TRANSFORM_DEPENDENCIES`) will be installed in
# addition to the `DEFAULT_DEPENDENCIES` of main.py if the ML experimental
# feature is enabled and the pipeline uses the corresponding model handler (or
# transformation). Similar to `DEFAULT_DEPENDENCIES`, these will only be
# installed if the caller does not provide a `requirements_file` option.
#
# These dependencies are only needed at runtime (by the workers), not during job
# submission (see https://github.com/apache/beam/pull/40439 and
# https://github.com/apache/beam/pull/40541), so they do not have to be added to
# the template container.
#
# The versions of these (and of `HUGGING_FACE_TENSORFLOW_DEPENDENCIES`) follow
# the versions that apache-beam declares for the same packages in its extras.
# The `YAML Template ML Dependencies` GitHub workflow checks this every 12
# hours, and when these or the version of apache-beam change (see `BEAM_EXTRAS`
# of python/src/test/python/yaml-template/check_ml_dependencies.py).
RUN_INFERENCE_DEPENDENCIES = {
    # Only needs google-cloud-aiplatform, which is already available in both the
    # template container and the default Beam worker container.
    'VertexAIModelHandlerJSON': [],
    # By default, this handler only uses PyTorch, so TensorFlow is not needed
    # (since https://github.com/apache/beam/pull/40541). Pipelines that use
    # TensorFlow also get `HUGGING_FACE_TENSORFLOW_DEPENDENCIES`. Uses the torch
    # and transformers versions of the `transformers` extra of apache-beam.
    'HuggingFacePipelineModelHandler': [
        'torch>=1.9.0,<2.12.0',
        'transformers>=4.28.0,<4.56.0',
    ],
}

# Additional runtime dependencies needed by HuggingFacePipelineModelHandler if
# it uses TensorFlow, i.e. if `framework: tf` is set in its `load_pipeline_args`
# (which are passed to `transformers.pipeline()`). These are added
# automatically since some callers of the template, for example Dataflow Job
# Builder, cannot provide a `requirements_file`. TensorFlow 2.16+ comes with
# Keras 3, but transformers needs Keras 2 (tf-keras) to run TensorFlow models.
HUGGING_FACE_TENSORFLOW_DEPENDENCIES = [
    'tensorflow==2.21.0',
    'tf-keras==2.21.0',
]

# Additional runtime dependencies needed by MLTransform transformations, keyed
# by the transformation type. See `RUN_INFERENCE_DEPENDENCIES` for details.
ML_TRANSFORM_DEPENDENCIES = {
    # sentence-transformers also brings in torch and transformers.
    'SentenceTransformerEmbeddings': ['sentence-transformers==5.0.0'],
    # These only need google-cloud-aiplatform and pillow, which are already
    # available in both the template container and the default Beam worker
    # container.
    'VertexAITextEmbeddings': [],
    'VertexAIImageEmbeddings': [],
}

# Dependencies needed by the components of ML transforms (see
# `_find_ml_components`), keyed by the ML transform type.
_ML_COMPONENT_DEPENDENCIES = {
    'RunInference': RUN_INFERENCE_DEPENDENCIES,
    'MLTransform': ML_TRANSFORM_DEPENDENCIES,
}


def _add_dependencies(dependencies, new_dependencies):
  """Appends new_dependencies to dependencies, skipping duplicates."""
  for dep in new_dependencies:
    if dep not in dependencies:
      dependencies.append(dep)


def _get_yaml_experimental_features(pipeline_args, pipeline_spec):
  """Returns the YAML experimental features enabled for a pipeline.

  Features can be enabled either through the `yaml_experimental_features`
  pipeline option or through the `options` section of the YAML pipeline.
  """
  parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
  parser.add_argument(
      '--yaml_experimental_features', action='append', default=[])
  known_args, _ = parser.parse_known_args(pipeline_args)
  features = set(known_args.yaml_experimental_features)

  yaml_options = pipeline_spec.get('options')
  if isinstance(yaml_options, dict):
    yaml_features = yaml_options.get('yaml_experimental_features')
    if isinstance(yaml_features, str):
      features.add(yaml_features)
    elif isinstance(yaml_features, list):
      features.update(f for f in yaml_features if isinstance(f, str))

  return features


def _find_ml_components(node, components, visited_ids):
  """Recursively finds the components of ML transforms in a parsed YAML node.

  Components are the model handlers of RunInference transforms and the
  transformations of MLTransform transforms. Since the whole node is searched,
  this also covers ML transforms nested in composite transforms and ML
  transforms used by transforms defined in YAML providers.

  Args:
    node: a parsed YAML node.
    components: a list of (transform type, component type, component config)
      tuples to add found components to.
    visited_ids: ids of already visited nodes. Guards against recursive YAML
      structures, which can be created using YAML aliases.
  """
  if not isinstance(node, (dict, list)) or id(node) in visited_ids:
    return
  visited_ids.add(id(node))

  if isinstance(node, list):
    children = node
  else:
    children = node.values()
    transform_type = node.get('type')
    config = node.get('config')
    if isinstance(config, dict):
      component_specs = []
      if transform_type == 'RunInference':
        component_specs = [config.get('model_handler')]
      elif transform_type == 'MLTransform':
        component_specs = config.get('transforms')
      if isinstance(component_specs, list):
        for spec in component_specs:
          if isinstance(spec, dict) and isinstance(spec.get('type'), str):
            components.append(
                (transform_type, spec['type'], spec.get('config')))

  for child in children:
    _find_ml_components(child, components, visited_ids)


def _uses_tensorflow(model_handler_config):
  """Returns whether a HuggingFacePipelineModelHandler config uses TensorFlow.

  Hugging Face pipelines use TensorFlow if `framework: tf` is set in
  `load_pipeline_args`, which are passed to `transformers.pipeline()`.
  Otherwise, PyTorch is used.

  Args:
    model_handler_config: the parsed `config` of the model handler.
  """
  if not isinstance(model_handler_config, dict):
    return False
  load_pipeline_args = model_handler_config.get('load_pipeline_args')
  return (
      isinstance(load_pipeline_args, dict) and
      load_pipeline_args.get('framework') == 'tf')


def _get_ml_dependencies(pipeline_spec):
  """Returns the runtime dependencies needed by the ML transforms in a pipeline.

  Args:
    pipeline_spec: the parsed YAML pipeline.
  """
  components = []
  _find_ml_components(pipeline_spec, components, set())

  dependencies = []
  for transform_type, component_type, component_config in components:
    dependencies_by_type = _ML_COMPONENT_DEPENDENCIES[transform_type]
    if component_type in dependencies_by_type:
      _add_dependencies(dependencies, dependencies_by_type[component_type])
    else:
      logging.warning(
          'Additional runtime dependencies needed by %s (used by %s) are '
          'unknown, so none were added for it. If needed, dependencies can be '
          'provided using the `requirements_file` option.',
          component_type,
          transform_type)
    if (transform_type == 'RunInference' and
        component_type == 'HuggingFacePipelineModelHandler' and
        _uses_tensorflow(component_config)):
      _add_dependencies(dependencies, HUGGING_FACE_TENSORFLOW_DEPENDENCIES)
  return dependencies


def get_runtime_dependencies(args):
  """Returns the runtime dependencies needed by ML transforms of a pipeline.

  Dependencies are only determined for pipelines that enable the ML YAML
  experimental feature. This is a best effort operation. If the pipeline cannot
  be parsed, no dependencies are returned and parsing errors are left to be
  reported when the pipeline is actually constructed.

  Args:
    args: the arguments passed to the template, which specify the pipeline.
  """
  try:
    # Uses the same logic as Beam YAML to load the pipeline, including Jinja
    # template expansion, so that the parsed pipeline matches the pipeline that
    # will be executed.
    _, pipeline_args, _, pipeline_yaml = (
        yaml_main._build_pipeline_yaml_from_argv(args))
    pipeline_spec = yaml.safe_load(pipeline_yaml)
    if not isinstance(pipeline_spec, dict):
      return []
    if ML_EXPERIMENTAL_FEATURE not in _get_yaml_experimental_features(
        pipeline_args, pipeline_spec):
      return []
    dependencies = _get_ml_dependencies(pipeline_spec)
  except Exception as e:  # pylint: disable=broad-except
    logging.warning(
        'Unable to determine the runtime dependencies needed by ML transforms '
        'of the pipeline: %s',
        e)
    return []

  if dependencies:
    logging.info(
        'Adding runtime dependencies needed by ML transforms of the pipeline: '
        '%s',
        dependencies)
  return dependencies
