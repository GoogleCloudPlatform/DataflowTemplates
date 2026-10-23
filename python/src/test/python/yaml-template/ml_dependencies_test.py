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

import os
import tempfile
import unittest

import yaml

import ml_dependencies

_HUGGING_FACE_PIPELINE_DEPENDENCIES = [
    'torch>=1.9.0,<2.12.0',
    'transformers>=4.28.0,<4.56.0',
]
_HUGGING_FACE_TENSORFLOW_DEPENDENCIES = [
    'tensorflow==2.21.0',
    'tf-keras==2.21.0',
]
_SENTENCE_TRANSFORMER_DEPENDENCIES = ['sentence-transformers==5.0.0']


def _pipeline_yaml(*transforms, options=None):
  """Returns a YAML pipeline that applies the given transforms to a Create."""
  pipeline = {
      'pipeline': {
          'type': 'chain',
          'transforms': [
              {
                  'type': 'Create',
                  'config': {'elements': [{'text': 'I love Apache Beam!'}]},
              },
              *transforms,
          ],
      },
  }
  if options is not None:
    pipeline['options'] = options
  return yaml.safe_dump(pipeline)


def _run_inference(model_handler_type, **model_handler_config):
  return {
      'type': 'RunInference',
      'config': {
          'model_handler': {
              'type': model_handler_type,
              'config': {
                  'preprocess': {'callable': 'lambda row: row.text'},
                  **model_handler_config,
              },
          },
      },
  }


def _ml_transform(*transformation_types):
  return {
      'type': 'MLTransform',
      'config': {
          'write_artifact_location': 'gs://my-bucket/artifacts',
          'transforms': [{
              'type': transformation_type,
              'config': {'columns': ['text']},
          } for transformation_type in transformation_types],
      },
  }


class MlDependenciesTest(unittest.TestCase):

  def test_adds_hugging_face_run_inference_dependencies(self):
    pipeline = _pipeline_yaml(_run_inference('HuggingFacePipelineModelHandler'))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_tensorflow_dependencies_for_hugging_face_tf_framework(self):
    pipeline = _pipeline_yaml(
        _run_inference(
            'HuggingFacePipelineModelHandler',
            load_pipeline_args={'framework': 'tf'}))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(
        dependencies,
        _HUGGING_FACE_PIPELINE_DEPENDENCIES +
        _HUGGING_FACE_TENSORFLOW_DEPENDENCIES)

  def test_does_not_add_tensorflow_dependencies_for_other_frameworks(self):
    for load_pipeline_args in (
        {'framework': 'pt'},
        {'device': 'cpu'},
        # Malformed load_pipeline_args.
        'framework: tf',
        ['framework', 'tf'],
    ):
      with self.subTest(load_pipeline_args=load_pipeline_args):
        pipeline = _pipeline_yaml(
            _run_inference(
                'HuggingFacePipelineModelHandler',
                load_pipeline_args=load_pipeline_args))
        dependencies = ml_dependencies.get_runtime_dependencies(
            [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
        self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_tensorflow_dependencies_once(self):
    pipeline = _pipeline_yaml(
        _run_inference(
            'HuggingFacePipelineModelHandler',
            load_pipeline_args={'framework': 'tf'}),
        _run_inference('HuggingFacePipelineModelHandler'),
        _ml_transform('SentenceTransformerEmbeddings'),
        _run_inference(
            'HuggingFacePipelineModelHandler',
            load_pipeline_args={'framework': 'tf', 'device': -1}))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(
        dependencies,
        _HUGGING_FACE_PIPELINE_DEPENDENCIES +
        _HUGGING_FACE_TENSORFLOW_DEPENDENCIES +
        _SENTENCE_TRANSFORMER_DEPENDENCIES)

  def test_adds_no_dependencies_for_vertex_ai_run_inference(self):
    pipeline = _pipeline_yaml(_run_inference('VertexAIModelHandlerJSON'))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, [])

  def test_adds_sentence_transformer_ml_transform_dependencies(self):
    pipeline = _pipeline_yaml(_ml_transform('SentenceTransformerEmbeddings'))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, _SENTENCE_TRANSFORMER_DEPENDENCIES)

  def test_adds_no_dependencies_for_vertex_ai_ml_transforms(self):
    pipeline = _pipeline_yaml(
        _ml_transform('VertexAITextEmbeddings', 'VertexAIImageEmbeddings'))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, [])

  def test_adds_dependencies_of_all_ml_transforms(self):
    pipeline = _pipeline_yaml(
        _run_inference('HuggingFacePipelineModelHandler'),
        _ml_transform(
            'SentenceTransformerEmbeddings', 'VertexAITextEmbeddings'),
        _run_inference('VertexAIModelHandlerJSON'),
        _run_inference('HuggingFacePipelineModelHandler'))
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(
        dependencies,
        _HUGGING_FACE_PIPELINE_DEPENDENCIES +
        _SENTENCE_TRANSFORMER_DEPENDENCIES)

  def test_does_not_add_ml_dependencies_if_ml_feature_not_enabled(self):
    pipeline = _pipeline_yaml(
        _run_inference('HuggingFacePipelineModelHandler'),
        _ml_transform('SentenceTransformerEmbeddings'))
    for extra_args in ([], ['--yaml_experimental_features=javascript']):
      with self.subTest(extra_args=extra_args):
        dependencies = ml_dependencies.get_runtime_dependencies(
            [f'--yaml_pipeline={pipeline}'] + extra_args)
        self.assertEqual(dependencies, [])

  def test_adds_ml_dependencies_for_ml_feature_flag_variants(self):
    pipeline = _pipeline_yaml(_run_inference('HuggingFacePipelineModelHandler'))
    for feature_args in (
        ['--yaml_experimental_features', 'ML'],
        [
            '--yaml_experimental_features=javascript',
            '--yaml_experimental_features=ML',
        ],
    ):
      with self.subTest(feature_args=feature_args):
        dependencies = ml_dependencies.get_runtime_dependencies(
            [f'--yaml_pipeline={pipeline}'] + feature_args)
        self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_ml_dependencies_if_ml_feature_enabled_in_yaml_options(self):
    for features in ('ML', ['javascript', 'ML']):
      with self.subTest(features=features):
        pipeline = _pipeline_yaml(
            _run_inference('HuggingFacePipelineModelHandler'),
            options={'yaml_experimental_features': features})
        dependencies = ml_dependencies.get_runtime_dependencies(
            [f'--yaml_pipeline={pipeline}'])
        self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_ml_dependencies_for_pipeline_file(self):
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.yaml', delete=False) as f:
      f.write(_pipeline_yaml(_ml_transform('SentenceTransformerEmbeddings')))
    self.addCleanup(os.remove, f.name)

    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline_file={f.name}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, _SENTENCE_TRANSFORMER_DEPENDENCIES)

  def test_adds_ml_dependencies_for_jinja_pipeline(self):
    pipeline = '''
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - {text: "I love Apache Beam!"}
    - type: RunInference
      config:
        model_handler:
          type: {{ model_handler }}
          config:
            task: text-classification
            preprocess:
              callable: 'lambda row: row.text'
'''
    dependencies = ml_dependencies.get_runtime_dependencies([
        f'--yaml_pipeline={pipeline}',
        '--jinja_variables='
        '{"model_handler": "HuggingFacePipelineModelHandler"}',
        '--yaml_experimental_features=ML',
    ])
    self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_ml_dependencies_for_nested_ml_transforms(self):
    pipeline = '''
pipeline:
  transforms:
    - type: Create
      config:
        elements:
          - {text: "I love Apache Beam!"}
    - type: composite
      input: Create
      transforms:
        - type: MLTransform
          input: input
          config:
            write_artifact_location: gs://my-bucket/artifacts
            transforms:
              - type: SentenceTransformerEmbeddings
                config:
                  model_name: all-MiniLM-L6-v2
                  columns: [text]
      output: MLTransform
    - type: MyInference
      input: Create

providers:
  - type: yaml
    transforms:
      MyInference:
        body:
          type: RunInference
          config:
            model_handler:
              type: HuggingFacePipelineModelHandler
              config:
                task: text-classification
                preprocess:
                  callable: 'lambda row: row.text'
'''
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertCountEqual(
        dependencies,
        _SENTENCE_TRANSFORMER_DEPENDENCIES +
        _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_ml_dependencies_for_recursive_yaml(self):
    # YAML aliases can be used to create recursive structures. These should not
    # result in infinite recursion.
    pipeline = '''
pipeline: &pipeline
  type: composite
  transforms:
    - type: RunInference
      config:
        model_handler:
          type: HuggingFacePipelineModelHandler
          config:
            task: text-classification
    - *pipeline
'''
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, _HUGGING_FACE_PIPELINE_DEPENDENCIES)

  def test_adds_no_dependencies_for_unknown_ml_components(self):
    pipeline = _pipeline_yaml(
        _run_inference('MyModelHandler'), _ml_transform('ScaleTo01'))
    with self.assertLogs(level='WARNING') as logs:
      dependencies = ml_dependencies.get_runtime_dependencies(
          [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, [])
    self.assertTrue(any('MyModelHandler' in log for log in logs.output))
    self.assertTrue(any('ScaleTo01' in log for log in logs.output))

  def test_ignores_malformed_ml_transforms(self):
    pipeline = '''
pipeline:
  type: chain
  transforms:
    - type: Create
      config:
        elements:
          - {text: "I love Apache Beam!"}
    - type: RunInference
      config:
        model_handler: HuggingFacePipelineModelHandler
    - type: RunInference
      config:
        model_handler:
          type: [HuggingFacePipelineModelHandler]
    - type: RunInference
      config: HuggingFacePipelineModelHandler
    - type: MLTransform
      config:
        transforms: SentenceTransformerEmbeddings
    - type: MLTransform
      config:
        transforms:
          - SentenceTransformerEmbeddings
'''
    dependencies = ml_dependencies.get_runtime_dependencies(
        [f'--yaml_pipeline={pipeline}', '--yaml_experimental_features=ML'])
    self.assertEqual(dependencies, [])

  def test_returns_no_dependencies_if_pipeline_cannot_be_parsed(self):
    for pipeline_args in (
        # Invalid YAML.
        ['--yaml_pipeline=pipeline: [RunInference'],
        # Undefined Jinja variable.
        ['--yaml_pipeline=pipeline: {{ undefined_variable }}'],
        # Missing pipeline file.
        ['--yaml_pipeline_file=/path/to/missing/pipeline.yaml'],
        # Missing pipeline.
        [],
    ):
      with self.subTest(pipeline_args=pipeline_args):
        with self.assertLogs(level='WARNING') as logs:
          dependencies = ml_dependencies.get_runtime_dependencies(
              pipeline_args + ['--yaml_experimental_features=ML'])
        self.assertEqual(dependencies, [])
        self.assertTrue(
            any('Unable to determine' in log for log in logs.output))


if __name__ == '__main__':
  unittest.main()
