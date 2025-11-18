# Creating a YAML Template

This document outlines the steps required to create a new YAML template for Dataflow.
More detailed instructions on how to run the template can be found in the associated README files for each template.

## Steps

1.  **Add YAML Blueprint:** Create the YAML blueprint file that defines the template's structure and parameters.
Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/yaml/src/main/yaml).

2.  **Generate YAML Template (Optional):** If desired, create the YAML template using the generation script.

```shell
python yaml/scripts/generate_yaml_java_templates.py yaml/src/main/yaml
```

3.  **Create Integration Test:** Develop an integration test to validate the functionality of the new template.
Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/yaml/src/test/java).

4.  **Generate Documentation:** Run the documentation generation script to create the official documentation for the template. You can find instructions on how to do this in the [contributor documentation](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#generated-documentation).
