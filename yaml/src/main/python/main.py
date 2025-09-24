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

from apache_beam.io.filesystems import FileSystems
from apache_beam.yaml import cache_provider_artifacts
from apache_beam.yaml import main

# These seem to be the common pipeline args that are not jinja pipeline
# parameters.
PIPELINE_ARGS = {
    '--template_location',
    '--service_account_email',
    '--updateCompatibilityVersion',
    '--runner',
    '--job_name',
    '--temp_location',
    '--labels',
    '--project',
    '--region',
    '--staging_location'
}

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
    parser = argparse.ArgumentParser()
    _, pipeline_args = parser.parse_known_args(argv)
    logging.info("Original pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))
    
    if all([arg.split('=',1)[0] != JINJA_INCOMING_ARG for arg in pipeline_args]):
        logging.info("Jinja variable parameter not found. Compiling individual parameters.")
        # Filter out for only jinja args
        # NOTE: If extra arguments get passed to the jinja variables, it is a
        # no-op. This processing is just to keep things as clean as possible.
        jinja_pipeline_args = \
            [arg for arg in pipeline_args \
             if arg.split('=',1)[0] not in PIPELINE_ARGS]
        logging.info("Jinja pipeline args: \n%s\n", \
                     pprint.pformat(jinja_pipeline_args, indent=2))

        # Remove jinja pipeline args from pipeline args
        pipeline_args = \
            [arg for arg in pipeline_args \
             if arg.split('=',1)[0] in PIPELINE_ARGS]
        logging.info("Pipeline args without jinja args: \n%s\n", \
                     pprint.pformat(pipeline_args,indent=2))

        # Process args as key value pairs for later jinja processing.
        jinja_vars = {}    
        for arg in jinja_pipeline_args:
            arg_key_value = arg.strip('--').split('=')
            jinja_vars[arg_key_value[0]] = arg_key_value[1]
        logging.info("Jinja variables: \n%s\n", \
                     pprint.pformat(jinja_vars,indent=2))

        # Save jinja vars as pipeline_args command. In theory, there should be
        # at least one unless there are no manadatory arguments.
        if jinja_vars:
            jinja_vars_output =  [f'{JINJA_OUTGOING_ARG}={json.dumps(jinja_vars)}']
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
    pipeline_args += [f'--yaml_pipeline={_get_pipeline_yaml()}']
    logging.info("Final pipeline args: \n%s\n", \
                 pprint.pformat(pipeline_args,indent=2))

    # Cache provider artifacts
    cache_provider_artifacts.cache_provider_artifacts()

    # Run with updated pipeline args
    main.run(argv=pipeline_args)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
