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
import json
import pprint
import re

from apache_beam.io.filesystems import FileSystems
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main


JINJA_INCOMING_ARG = '--jinjaVariables'
JINJA_OUTGOING_ARG = '--jinja_variables'


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

def _parse_yaml_pipeline(yaml_pipeline):
    """Parses a YAML pipeline string to extract Jinja variable names.

    This function uses a regular expression to find all occurrences of Jinja-style
    placeholders (e.g., `{{ my_var | default('foo') }}`) and extracts the core
    variable name, ignoring filters and surrounding whitespace.

    Args:
        yaml_pipeline (str): The YAML pipeline content as a string.

    Returns:
        set: A set of unique Jinja variable names found in the pipeline.
    """
    pattern = r'\{\{(.*?)\}\}'
    initial_jinja_variables = re.findall(pattern, yaml_pipeline)
    final_jinja_variables = set()
    for jinja_variable in initial_jinja_variables:
        variable = jinja_variable.strip().split('|', 1)[0].strip()
        final_jinja_variables.add(variable)
    return final_jinja_variables


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
    1. Determines if jinja argument present or not and branches off that.
    2. Parses all command-line arguments.
    3. Filters out standard pipeline arguments, treating the rest as Jinja
    variables.
    4. Reads the YAML pipeline definition from 'template.yaml'.
    5. Constructs the final argument list for `apache_beam.yaml.main.run`.
    6. Caches Beam YAML provider artifacts.
    7. Executes the Beam pipeline.
    """
    yaml_pipeline = _get_pipeline_yaml()
    logging.info("Yaml pipeline: \n%s\n", pprint.pformat(yaml_pipeline, indent=2))

    yaml_pipeline_jinja_variables = _parse_yaml_pipeline(yaml_pipeline)
    logging.info("Jinja variables: \n%s\n", pprint.pformat(yaml_pipeline_jinja_variables,indent=2))

    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)
    logging.info("Original pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))
    
    if all([arg.split('=',1)[0] != JINJA_INCOMING_ARG for arg in pipeline_args]):
        logging.info("Jinja variable parameter not found. Compiling individual parameters.")
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
            arg_key_value = arg.strip('--').split('=')
            provided_jinja_vars[arg_key_value[0]] = arg_key_value[1]
        logging.info("Jinja variables provided: \n%s\n", \
                     pprint.pformat(provided_jinja_vars,indent=2))

        # Save jinja vars as pipeline_args command. In theory, there should be
        # at least one unless there are no manadatory arguments.
        if provided_jinja_vars:
            jinja_vars_output =  [f'{JINJA_OUTGOING_ARG}={json.dumps(provided_jinja_vars)}']
            pipeline_args += jinja_vars_output

    else:
        raise Exception(
            f"The argument '{JINJA_INCOMING_ARG}' is not yet supported. ")

        # TODO(#2816):
        # logging.info(f"{JINJA_INCOMING_ARG} parameter found. Replacing it with the " + \
        #              "{JINJA_OUTGOING_ARG} argument.")
        # # Find the jinjaVariables argument
        # jinja_arg_index = -1
        # for i, arg in enumerate(pipeline_args):
        #     if arg.split('=', 1)[0] == JINJA_INCOMING_ARG:
        #         jinja_arg_index = i
        #         break

        # # Switch out jinjaVariables for jinja_variables
        # pipeline_args[jinja_arg_index] = f"{JINJA_OUTGOING_ARG}={pipeline_args[jinja_arg_index].split('=', 1)[1]}"

    # Save the pipeline yaml template to the appropriate pipeline option
    pipeline_args += [f'--yaml_pipeline={yaml_pipeline}']
    logging.info("Final pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))

    # Cache provider artifacts
    cache_provider_artifacts.cache_provider_artifacts()

    # Run with updated pipeline args
    main.run(argv=pipeline_args)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
