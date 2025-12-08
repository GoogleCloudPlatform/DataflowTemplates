# Creating a YAML Template

This document outlines the steps required to create a new YAML template for Dataflow.
More detailed instructions on how to run the template can be found in the associated README files for each template.

## Steps

1.  **Contribution Instructions:** Read the code contributions located [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md).

1. **Add YAML Blueprint:** Create the YAML blueprint file that defines the template's structure and parameters.
Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/yaml/src/main/yaml).

1.  **Generate YAML Template (Optional):** If desired, create the YAML template using the generation script.

    ```shell
    python yaml/src/main/python/generate_yaml_java_templates.py yaml/src/main/yaml
    ```

1.  **Create Integration Test:** Develop an integration test to validate the functionality of the new template.
Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/yaml/src/test/java/com/google/cloud/teleport/templates/yaml).
Additional instructions can be located [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/add-integration-or-load-test.md).

1.  **Generate Documentation:** Run the documentation generation script to create the official documentation for the template. You can find instructions on how to do this in the [contributor documentation](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#generated-documentation).
