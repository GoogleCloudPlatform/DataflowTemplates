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
import textwrap


def get_java_type(param_type):
    """Maps a YAML parameter type to a Java type."""
    if param_type == 'text':
        return 'String'
    elif param_type == 'integer':
        return 'Integer'
    elif param_type == 'boolean':
        return 'Boolean'
    else:
        return 'String'

def get_template_parameter_type(param_type):
    """Maps a YAML parameter type to a TemplateParameter annotation type."""
    if param_type == 'text':
        return 'TemplateParameter.Text'
    elif param_type == 'integer':
        return 'TemplateParameter.Integer'
    elif param_type == 'boolean':
        return 'TemplateParameter.Boolean'
    else:
        return 'TemplateParameter.Text'

def generate_java_interface(yaml_path, java_path):
    """Generates a Java interface file from a YAML blueprint."""

    # Read the YAML file and do some replacements
    with open(yaml_path, 'r') as f:
        content = f.read()
        # Remove Jinja variables before parsing
        content = re.sub(r'{{.*?}}', '', content)
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

    # Build the parameters string
    parameters_code = []
    for i, param in enumerate(parameters):
        param_name = param['name']
        java_type = get_java_type(param.get('type', 'text'))
        template_param_type = get_template_parameter_type(param.get('type', 'text'))
        getter_name = "get" + param_name[0].upper() + param_name[1:]
        wrapped_description = param.get('description', '').strip()
        wrapped_help_text = param.get('help', '').strip()
        example = param.get('example', '').strip().replace('"', '\\"')

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
            elif java_type == 'Integer':
                param_code += f'  @Default.Integer({param["default"]})\n'
            elif java_type == 'Boolean':
                param_code += f'  @Default.Boolean({param["default"]})\n'

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
        template_info_files_to_copy=template_info.get('filesToCopy', {}).strip(),
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

    # Find all YAML files in the input directory or capture file path
    yaml_files = []
    if input_path.is_file():
        if input_path.suffix.lower() == ".yaml":
            yaml_files.append(input_path)
    elif input_path.is_dir():
        yaml_files = list(input_path.glob("*.yaml"))
    
    yaml_files = sorted(list(set(yaml_files)))

    if not yaml_files:
        print(f"No YAML files found in {input_path}", file=sys.stderr)
        sys.exit(1)

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


if __name__ == "__main__":
    main()
