#
# Copyright (C) 2023 Google Inc.
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
#

import argparse
import logging
import re
import os

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, \
  StandardOptions, WorkerOptions
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.ml.inference.pytorch_inference import make_tensor_model_fn
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
import torch
from transformers import AutoConfig
from transformers import AutoModelForSeq2SeqLM
from transformers import AutoTokenizer
from transformers.tokenization_utils import PreTrainedTokenizer

tokenizer = None


def build_tokenizer(model_name: str):
  global tokenizer
  if not tokenizer:
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    if torch.cuda.is_available():
      tokenizer = tokenizer.to('cuda')

  return tokenizer


def to_tensors(input_message: bytes, model_name: str) -> torch.Tensor:
  """Encodes input text into token tensors.
  Args:
      input_text: Input text for the LLM model.
      model_name: Name of the LLM model.
  Returns: Tokenized input tokens.
  """
  input_text = input_message.decode('utf-8')
  logging.info('Converting "%s" to tensors...', input_text)
  tokenizer = build_tokenizer(model_name)
  return tokenizer(input_text, return_tensors="pt").input_ids[
    0]


def from_tensors(result: PredictionResult, model_name: str) -> bytes:
  """Decodes output token tensors into text.
  Args:
      result: Prediction results from the RunInference transform.
      model_name: Name of the LLM model.
  Returns: The model's response as text.
  """
  output_tokens = result.inference
  tokenizer = build_tokenizer(model_name)
  output_text = tokenizer.decode(output_tokens,
                                 skip_special_tokens=True)
  logging.info('Parsed result to "%s"', output_text)
  return output_text.encode('utf-8')


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--max_response_tokens',
      dest='max_response_tokens',
      default=256,
      help='Maximum response tokens.')
  parser.add_argument(
      '--model_name',
      dest='model_name',
      default='google/flan-t5-large',
      help='Model name to use.')
  parser.add_argument(
      '--input_subscription',
      dest='input_subscription',
      help='Subscription to read input.')
  parser.add_argument(
      '--output_topic',
      dest='output_topic',
      help='Topic to read output tokens.')
  parser.add_argument(
      '--temp_location',
      dest='temp_location',
      help='Temporary location to write the model.')
  parser.add_argument(
      '--device',
      dest='device',
      default='CPU',
      help='Device to use (CPU / GPU).')
  known_args, pipeline_args = parser.parse_known_args(argv)

  model_file = "saved_model_" + known_args.model_name.replace('/', '_')
  state_dict_path_local = "/tmp/" + model_file
  state_dict_path_gcs = os.path.join(known_args.temp_location, model_file)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(WorkerOptions).num_workers = 1
  pipeline_options.view_as(WorkerOptions).autoscaling_algorithm = 'NONE'
  pipeline_options.view_as(WorkerOptions).disk_size_gb = 80
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  fs = GCSFileSystem(pipeline_options)
  if not fs.exists(state_dict_path_gcs):
    logging.info('Loading model %s...', known_args.model_name)

    model = AutoModelForSeq2SeqLM.from_pretrained(
        known_args.model_name, torch_dtype=torch.bfloat16
    )
    if torch.cuda.is_available():
      model = model.to('cuda')

    torch.save(model.state_dict(), state_dict_path_local)

    logging.info('Saving the model to %s...', state_dict_path_gcs)

    with fs.create(state_dict_path_gcs) as gcs_file:
      with open(state_dict_path_local, 'rb') as local_file:
        gcs_file.write(local_file.read())
  else:
    logging.info('Model already exists, skipping!')

  # Create an instance of the PyTorch model handler.
  model_handler = PytorchModelHandlerTensor(
      state_dict_path=state_dict_path_gcs,
      model_class=AutoModelForSeq2SeqLM.from_config,
      model_params={
          "config": AutoConfig.from_pretrained(known_args.model_name)},
      device=known_args.device,
      inference_fn=make_tensor_model_fn("generate"),
  )

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:
    _ = (p | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=known_args.input_subscription)
         | "ToTensors" >> beam.Map(to_tensors, known_args.model_name)
         | "RunInference" >> RunInference(
            model_handler,
            inference_args={
                "max_new_tokens": known_args.max_response_tokens},
        )
         | "FromTensors" >> beam.Map(from_tensors, known_args.model_name)
         | "WriteToPubSub" >> WriteToPubSub(known_args.output_topic))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
