# Copyright (C) 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import argparse
import re
import subprocess
import sys

from pathlib import Path

import yaml

JAVA_TYPE_BY_YAML_TYPE = {
    'text': 'String',
    'integer': 'Integer',
    'boolean': 'Boolean',
    'double' : 'Double',
    'map': 'String',
    'enum': 'Enum',
    'password': 'String'
}

TEMPLATE_TYPE_BY_YAML_TYPE = {
    'text': 'TemplateParameter.Text',
    'integer': 'TemplateParameter.Integer',
    'boolean': 'TemplateParameter.Boolean',
    'double' : 'TemplateParameter.Double',
    'map': 'TemplateParameter.Text',
    'enum': 'TemplateParameter.Enum',
    'password': 'TemplateParameter.Password'
}

def get_git_root():
    """Gets the root directory of the git repository."""
    try:
        return subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        # Handle cases where git command fails or git is not installed
        print("Error: Git repository not found. Please ensure Git is installed and in your PATH.")
        return e

def run_mvn_spotless():
    """Runs the mvn spotless:apply command in the git repository root."""
    repo_root = get_git_root()
    if not repo_root:
        raise Exception("Could not determine the root of the git repository.")

    try:
        subprocess.run(
            ["mvn", "spotless:apply"],
            check=True,
            cwd=repo_root,
            capture_output=True,
            text=True)
        print("Successfully ran mvn spotless:apply.")
    except FileNotFoundError as e:
        print("Error: 'mvn' command not found. Please ensure Maven is installed and in your PATH.")
        return e
    except subprocess.CalledProcessError as e:
        print(f"Error running mvn spotless:apply: {e}", file=sys.stderr)
        return e

def generate_java_interface(yaml_path, java_path):
    """Generates a Java interface file from a YAML template.

    This function reads a YAML file that defines a Dataflow template, including
    its metadata and parameters. It then uses a Java template file (java.tmpl)
    to generate a corresponding Java interface file with the appropriate
    annotations for a Dataflow Flex Template.

    Args:
        yaml_path (pathlib.Path): The path to the input YAML template file.
        java_path (pathlib.Path): The path where the output Java interface file
            will be written.
    """

    # Read the YAML file and do some replacements
    with open(yaml_path, 'r') as f:
        content = f.read()
        # Remove Jinja variables before parsing
        content = re.sub(r'{{.*?}}', '', content)
        # Remove Jinja control blocks
        content = re.sub(r'{%.*?%}', '', content)
        # Fix set-like syntax for requirements
        content = re.sub(r'(requirements\s*:\s*)\{([^}]+)\}', r'\1[\2]', content, flags=re.DOTALL)
        # Fix set-like syntax for filesToCopy
        content = re.sub(r'(\s*-\s*)\{([^}]+)\}', r'\1[\2]', content)
        data = yaml.safe_load(content)

    template_info = data.get('template', {})
    parameters = template_info.get('parameters', [])
    class_name = java_path.stem

    # Read the Java template
    template_path = Path(__file__).parent / "java.tmpl"
    with open(template_path, 'r') as f:
        java_template = f.read()

    # Build the parameters map from options files
    options_map = {}
    options_files = template_info.get('options_file', [])
    if options_files:
        options_dir = yaml_path.parent.parent / "python" / "options"
        for option_file in options_files:
            option_file_path = options_dir / f"{option_file}.yaml"
            if option_file_path.exists():
                with open(option_file_path, 'r') as f:
                    option_data = yaml.safe_load(f)
                    for option in option_data.get('options', []):
                        options_map[option['name']] = option['parameters']
            else:
                print(f"Warning: Option file {option_file_path} not found.")

    # Flatten parameters
    flat_parameters = []
    for param in parameters:
        if isinstance(param, str):
            if param in options_map:
                flat_parameters.extend(options_map[param])
            else:
                print(f"Warning: Parameter group {param} not found in options.")
        else:
            flat_parameters.append(param)

    # Build the parameters code
    parameters_code = []
    for i, param in enumerate(flat_parameters):
        param_name = param['name']
        java_type = JAVA_TYPE_BY_YAML_TYPE.get(param.get('type', 'text'), 'String')
        template_param_type = TEMPLATE_TYPE_BY_YAML_TYPE.get(param.get('type', 'text'))
        getter_name = "get" + param_name[0].upper() + param_name[1:]
        wrapped_description = str(param.get('description', '')).strip()
        wrapped_help_text = str(param.get('help', '')).strip()
        example = str(param.get('example', '')).strip().replace('"', '\\"')

        param_code = f"""
  @{template_param_type}(
      order = {i + 1},
      name = "{param_name}",
      optional = {str(not param.get('required', False)).lower()},
      description = "{wrapped_description}",
      helpText = "{wrapped_help_text}",
      example = "{example}"
    )
"""
        # required param
        if param.get('required', False):
            param_code += "  @Validation.Required\n"

        # default param
        if 'default' in param:
            if java_type == 'String':
                param_code += f'  @Default.String("{param["default"]}")\n'
            else:        
                param_code += f"  @Default.{java_type}({param['default']})\n"

        # getter name
        param_code += f"  {java_type} {getter_name}();"
        
        parameters_code.append(param_code)

    # Format requirements for Java array
    reqs = template_info.get('requirements', [])
    reqs_formatted = "{}"
    if reqs:
        if isinstance(reqs, str):
            # In case the yaml parser gives us a single string with commas
            reqs = [r.strip() for r in reqs.split(',') if r.strip()]

        req_items = []
        for r in reqs:
            if r:
                req_items.append(f'"{r}"')
        if req_items:
            reqs_formatted = '{' + ',\n      '.join(req_items) + '\n    }'

    description = template_info.get('description', '').strip()

    # Replace placeholders in the template
    java_code = java_template.format(
        template_info_name=template_info.get('name', ''),
        template_info_category=template_info.get('category', 'STREAMING'),
        template_info_display_name=template_info.get('display_name', ''),
        template_info_description=description,
        template_info_flex_container_name=template_info.get('flex_container_name', ''),
        template_info_yamlTemplateFile=template_info.get('yamlTemplateFile', ''),
        template_info_files_to_copy=template_info.get('filesToCopy', '').strip(),
        template_info_documentation=template_info.get('documentation', '').strip(),
        template_info_contactInformation=template_info.get('contactInformation', ''),
        template_info_yaml_template_file=yaml_path.name,
        template_info_requirements=reqs_formatted,
        template_info_streaming=str(template_info.get('streaming', False)).lower(),
        template_info_hidden=str(template_info.get('hidden', False)).lower(),
        class_name=class_name,
        parameters='\n'.join(parameters_code),
    )

    # Write the Java file
    java_path.parent.mkdir(parents=True, exist_ok=True)
    with open(java_path, 'w') as f:
        f.write(java_code)

    print(f"Successfully generated {java_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate Java interfaces for YAML Dataflow templates."
    )
    parser.add_argument(
        "input_dir",
        help="Path to the input directory containing YAML template files or a single YAML file.",
    )
    args = parser.parse_args()
    input_path = Path(args.input_dir)

    # Find all YAML files in the input directory or capture single file path
    yaml_files = []
    if input_path.is_file():
        if input_path.suffix.lower() == ".yaml":
            yaml_files.append(input_path)
    elif input_path.is_dir():
        yaml_files = list(input_path.glob("*.yaml"))
    
    # Sort the list of YAML files
    yaml_files = sorted(list(yaml_files))

    if not yaml_files:
        print(f"No YAML files found in {input_path}", file=sys.stderr)
        sys.exit(1)

    # Convert each YAML file to a Java interface
    try:
        for yaml_path in yaml_files:
            print(f"Processing {yaml_path}")
            # Derive the Java file path
            # e.g., .../yaml/src/main/yaml/MyTemplate.yaml -> .../yaml/src/main/java/.../MyTemplateYaml.java
            class_name = yaml_path.stem + "Yaml"
            java_path = (
                yaml_path.parent.parent
                / "java"
                / "com"
                / "google"
                / "cloud"
                / "teleport"
                / "templates"
                / "yaml"
                / f"{class_name}.java"
            )
            generate_java_interface(yaml_path, java_path)
    except Exception as e:
        print(f"An error occurred when trying to convert yaml blueprint to java template: {e}", file=sys.stderr)
        sys.exit(1)

    # Adding this step so that developer doesn't need to remember this step
    # when pushing a PR and trying to format the template with code will not work
    # as well compared to just running mvn spotless.
    try:
        run_mvn_spotless()
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
