# Python Dependency Management

This document outlines the process for managing Python dependencies for the Python-based and a few other Dataflow templates in this repository.

## Overview

To ensure reproducible builds and avoid dependency conflicts, we use a system of pinned dependencies. The `requirements.txt` files for each Python template are not meant to be edited manually. Instead, they are generated from a set of base requirement files (e.g., `default_base_python_requirements.txt`).

This process is managed by two main shell scripts:

-   `generate_dependencies.sh`: A low-level script that takes a base requirements file and generates a fully-pinned `requirements.txt` file with hashes using `pip-tools`.
-   `generate_all_dependencies.sh`: The main script that orchestrates the dependency generation for all Python templates in the repository by calling `generate_dependencies.sh` for each one.

## Prerequisites

Before running the dependency generation scripts, you must have `python3.11` and its corresponding `venv` module installed on your system.

On Debian/Ubuntu systems, you can install them with:

```bash
sudo apt-get update
sudo apt-get install python3.11 python3.11-venv
```

For other operating systems or for managing multiple Python versions, please refer to the Apache Beam Python Tips [guide](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95653376#PythonTips-VirtualEnvironmentSetup) for more information.

## Usage

### Regenerating All Dependencies

To regenerate all `requirements.txt` files after making changes to a base file, run the main script from the root of the DataflowTemplates repository:

```bash
sh python/generate_all_dependencies.sh
```

This script will:
1.  Create a temporary Python virtual environment.
2.  Install `pip-tools`.
3.  For each template, run `pip-compile` to resolve and pin all dependencies from its base requirements file.
4.  Copy the newly generated `requirements.txt` files to their correct locations within the template directories.

### Adding or Updating a Dependency

To add or update a dependency for a Python template:

1.  **Identify the base requirements file**: Locate the correct base requirements file that corresponds to your template. The `generate_all_dependencies.sh` script shows which base file maps to which final `requirements.txt`. For most templates, this will be either `python/default_base_python_requirements.txt` or `python/default_base_yaml_requirements.txt`.
2.  **Modify the base file**: Add or change the version specifier for your desired package in the base requirements file. It is recommended to specify a version range or a minimum version.
3.  **Regenerate the pinned dependencies**: Run the `generate_all_dependencies.sh` script as described above.
4.  **Commit the changes**: Commit the changes to both the base requirements file and all the `requirements.txt` files that were updated by the script.
