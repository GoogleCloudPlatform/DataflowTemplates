# Creating a YAML Template

This document outlines the steps required to create a new YAML template for Dataflow.
More detailed instructions on how to run the template can be found in the associated README files for each template.

## Steps

1.  **Contribution Instructions:** Read the code contributions located [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md).

1. **Add YAML Blueprint:** Create the YAML blueprint file that defines the template's structure and parameters.
Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/yaml/src/main/yaml).

1.  **Generate YAML Template:**

    Create the YAML template in Java and place it under yaml/src/main/java/com/google/cloud/teleport/templates/yaml.

    If desired, you can create the YAML template with the help of the generation script.

    ```shell
    python yaml/src/main/python/generate_yaml_java_templates.py yaml/src/main/yaml
    ```

1.  **Create Integration Test:**

    Develop an integration test to validate the functionality of the new template.
    Place this file in [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/yaml/src/test/java/com/google/cloud/teleport/templates/yaml).

    Additional instructions can be located [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/add-integration-or-load-test.md).

    Run the test from root directory using the following format:
    ```shell
    mvn test -pl yaml -Dtest=IcebergToMySqlYamlIT -Dproject=test-proj -DartifactBucket=my_gcs_bucket
    ```

    Running mvn test will attempt to stage the template before running the actual test.
    If you have trouble staging the template, verify that it can be staged on its own using mvn package
    ```shell
    mvn package -pl yaml -am -PtemplatesStage  \
      -DskipTests \
      -DprojectId="my-project" \
      -DbucketName="my_gcs_bucket" \
      -DstagePrefix="$(date +%Y_%m_%d)_01"
    ```

    You can optionally stage the template first and then run the test with the `-DspecPath=gs://template-location` pipeline option to save time during test development.

    If your test requires external resources such as a database (e.g., MySQLResourceManager), it may not run locally depending on the resource manager used. In such cases, modify the test to use a database accessible by Dataflow. Once testing is complete, revert your changes to align with the standard test structure. Since these tests are designed for GitHub Workflows, they should pass automatically once the PR is submitted.


1.  **Generate Documentation:** Run the documentation generation script to create the official documentation for the template. You can find instructions on how to do this in the [contributor documentation](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#generated-documentation).
