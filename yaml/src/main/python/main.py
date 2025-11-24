#
# Copyright (C) 2024 Google Inc.
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
import pprint
import jinja2
import yaml
import re
import datetime

from apache_beam.io.filesystems import FileSystems
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main
from jinja2 import Environment, meta

UNDEFINED_MARKER = "__UNDEFINED_JINJA_VARIABLE__"

class _UndefinedMarker(jinja2.Undefined):
    """A Jinja Undefined class that renders to special marker string."""
    def __str__(self):
        return UNDEFINED_MARKER

def _clean_undefined_recursively(d):
    """
    Recursively traverses the template and remove all keys or items
    that have the value of the UNDEFINED_MARKER.
    """
    if isinstance(d, dict):
        return {
            k: _clean_undefined_recursively(v)
            for k, v in d.items() if v != UNDEFINED_MARKER
        }
    elif isinstance(d, list):
        return [_clean_undefined_recursively(i) for i in d if i != UNDEFINED_MARKER]
    return d


def _get_pipeline_yaml():
    """Reads the pipeline definition from the 'template.yaml' file.

    This function assumes that a 'template.yaml' file is present in the
    current working directory of the execution environment. It opens the file,
    reads its content, decodes it as a string, and returns it. 

    Reminder - The 'template.yaml' file is just the renamed yaml 

    Returns:
        str: The content of the 'template.yaml' file as a string.
    """
    with FileSystems.open("template.yaml") as fin:
        pipeline_yaml = fin.read().decode()
    return pipeline_yaml

def _get_pipeline_with_options(yaml_pipeline):
    """Expands the YAML template by merging options from `options_file` directives.

    Args:
        yaml_pipeline (str): The YAML pipeline content as a string.

    Returns:
        str:  The content of the 'template.yaml' file with options as a string.
    """
    data = yaml.safe_load(yaml_pipeline)
    template_section = data.get('template', {})

    if 'options_file' in template_section:
        options_map = {}
        for name in template_section.get('options_file', []):
            try:
                with FileSystems.open(f"options/{name}.yaml") as f:
                    options_data = yaml.safe_load(f)
                    for opt in options_data.get('options', []):
                        options_map[opt['name']] = opt['parameters']
            except Exception as e:
                logging.warning(f"Could not read options file: options/{name}.yaml. Error: {e}")

        expanded_params = []
        for param in template_section.get('parameters', []):
            if isinstance(param, str) and param in options_map:
                expanded_params.extend(options_map[param])
            else:
                expanded_params.append(param)

        data['template']['parameters'] = expanded_params
        del data['template']['options_file']
        return yaml.dump(data)
    else:
        return yaml_pipeline


def _get_cleaned_pipeline(yaml_template, provided_jinja_vars):
    """Renders the Jinja template and removes unused optional fields."""
    env = Environment(undefined=_UndefinedMarker)
    template = env.from_string(yaml_template)
    rendered_yaml = template.render(datetime=datetime, **provided_jinja_vars)

    data = yaml.safe_load(rendered_yaml)
    cleaned_data = _clean_undefined_recursively(data)
    return yaml.dump(cleaned_data)


def _extract_jinja_variable_names(yaml_pipeline):
    """Parses a YAML pipeline string to extract Jinja variable names.

    This function uses a regular expression to find all occurrences of Jinja-style
    placeholders (e.g., `{{ my_var | default('foo') }}`) and extracts the
    variable names.

    Args:
        yaml_pipeline (str): The YAML pipeline content as a string.

    Returns:
        set: A set of unique Jinja variable names found in the pipeline.
    """
    env = Environment()
    template = env.parse(yaml_pipeline)
    return meta.find_undeclared_variables(template)


def run(argv=None):
    """Runs a Beam YAML pipeline, processing command-line arguments for Jinja
    templating.

    This method orchestrates the execution of a Beam pipeline defined in
    'template.yaml'. It parses command-line arguments, distinguishing between
    standard pipeline options and those intended as Jinja templating variables.
    It then prepares these variables and the YAML pipeline definition for
    processing by `apache_beam.yaml.main.run`.

    Args:
        argv (list, optional): A list of command-line arguments. If None,
            `sys.argv` is used. Defaults to None.

    The method performs the following steps:
    1. Retrieves the YAML pipeline definition from 'template.yaml'.
    2. Extracts Jinja variable names from the YAML pipeline.
    3. Parses command-line arguments.
    4. Filters out standard pipeline arguments, treating the rest as Jinja
    variables.
    5. Reads the YAML pipeline definition from 'template.yaml'.
    6. Constructs the final argument list for `apache_beam.yaml.main.run`.
    7. Caches Beam YAML provider artifacts.
    8. Executes the Beam pipeline.
    """
    yaml_pipeline = _get_pipeline_yaml()
    logging.info("Yaml pipeline: \n%s\n", pprint.pformat(yaml_pipeline, indent=2))

    yaml_pipeline_jinja_variables = _extract_jinja_variable_names(yaml_pipeline)
    logging.info("Jinja variables: \n%s\n", pprint.pformat(yaml_pipeline_jinja_variables,indent=2))

    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)
    logging.info("Original pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))

    # Filter out for only jinja args
    jinja_pipeline_args = \
        [arg for arg in pipeline_args \
            if arg.split('=',1)[0].strip('--') in yaml_pipeline_jinja_variables]
    logging.info("Jinja pipeline args found: \n%s\n", \
                    pprint.pformat(jinja_pipeline_args, indent=2))

    # Remove jinja pipeline args from pipeline args
    pipeline_args = \
        [arg for arg in pipeline_args \
            if arg.split('=',1)[0].strip('--') not in yaml_pipeline_jinja_variables]
    logging.info("Pipeline args without jinja args: \n%s\n", \
                    pprint.pformat(pipeline_args,indent=2))

    # Process args as key value pairs for later jinja processing.
    provided_jinja_vars = {}    
    for arg in jinja_pipeline_args:
        arg_key_value = arg.strip('--').split('=', 1)
        provided_jinja_vars[arg_key_value[0]] = arg_key_value[1]
    logging.info("Jinja variables provided: \n%s\n", \
                    pprint.pformat(provided_jinja_vars,indent=2))

    # Get the cleaned YAML removing unused optional fields.
    cleaned_yaml = _get_cleaned_pipeline(yaml_pipeline, provided_jinja_vars)
    # Get final YAML pipeline with options merged.
    yaml_pipeline = _get_pipeline_with_options(cleaned_yaml)
    logging.info("Final YAML to be executed:\n%s", yaml_pipeline)

    # Save the pipeline yaml template to the appropriate pipeline option
    pipeline_args.append(f'--yaml_pipeline={yaml_pipeline}')
    logging.info("Final pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))

    # Cache provider artifacts
    cache_provider_artifacts.cache_provider_artifacts()

    # Run with updated pipeline args
    main.run(argv=pipeline_args)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
