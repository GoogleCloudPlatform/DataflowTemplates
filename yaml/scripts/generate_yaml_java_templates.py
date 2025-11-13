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

def wrap_for_java_multiline(text: str, width: int, indent: int) -> str:
    """Wraps a string and formats it as a multi-line Java string literal."""
    lines = textwrap.wrap(text.strip().replace('"', '\\"'), width=width, break_long_words=False)
    if not lines:
        return ""
    return ('"\n' + ' ' * indent + '+ "').join(lines)

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
        wrapped_description = wrap_for_java_multiline(param.get('description', ''), width=100, indent=8)
        wrapped_help_text = wrap_for_java_multiline(param.get('help', ''), width=100, indent=8)
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

    # Wrap the main description for better readability in the generated Java file.
    formatted_description = wrap_for_java_multiline(template_info.get('description', ''), width=100, indent=8)

    # Replace placeholders in the template
    java_code = java_template.format(
        template_info_name=template_info.get('name', ''),
        template_info_category=template_info.get('category', 'STREAMING'),
        template_info_display_name=template_info.get('display_name', ''),
        template_info_description=formatted_description,
        template_info_flex_container_name=template_info.get('flex_container_name', ''),
        yamlTemplateFile=template_info.get('yamlTemplateFile', ''),
        files_to_copy=template_info.get('filesToCopy', {}).strip(),
        documentation=template_info.get('documentation', '').strip(),
        contactInformation=template_info.get('contactInformation', ''),
        yaml_path_name=yaml_path.name,
        class_name=class_name,
        parameters='\n'.join(parameters_code),
        requirements=reqs_formatted,
        streaming=str(data.get('options', {}).get('streaming', False)).lower(),
        hidden=str(template_info.get('hidden', False)).lower(),
    )

    # Write the Java file
    java_path.parent.mkdir(parents=True, exist_ok=True)
    with open(java_path, 'w') as f:
        f.write(java_code)

    print(f"Successfully generated {java_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a Java interface for a YAML Dataflow template."
    )
    parser.add_argument(
        "yaml_file",
        help="Path to the input YAML template file.",
    )
    args = parser.parse_args()

    yaml_path = Path(args.yaml_file)
    if not yaml_path.is_file():
        print(f"Error: File not found at {yaml_path}", file=sys.stderr)
        sys.exit(1)

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

    try:
        generate_java_interface(yaml_path, java_path)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
