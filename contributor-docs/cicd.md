# CI/CD

This repository uses [GitHub Actions](https://docs.github.com/en/actions) for all CI/CD as well as for some task automation.

All workflows rely on the [setup-env action](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/.github/actions/setup-env/action.yml)
to allow them to invoke CI/CD Golang scripts from the [ci-cd directory](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/cicd).
Any new CI/CD commands should be added to that directory.

## Adding new workflows

All templates tests should be exercised via the [java-pr workflow](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/.github/workflows/java-pr.yml) by default.
This executes on all pull requests and pushes to main, as well as on a twice-daily schedule.
Results from this workflow can be found under the [actions tab](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/java-pr.yml).

Splitting one or more templates' tests into a separate workflow can help workflows run significantly more quickly since they don't require building all modules.
To split a set of templates into their own CI workflow, follow these steps:

1) Add an alias to the templates you would like to build/test together in https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/cicd/internal/flags/common-flags.go#L32
2) Add a new workflow to the `.github/` folder for your tests. This can be a copy of the main [java-pr workflow](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/.github/workflows/java-pr.yml) with a few changes:

- Update the [pull_request trigger paths](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/.github/workflows/java-pr.yml#L23) to point to the templates you want to run your tests against.
- Update all `./cicd/<script>` calls with a pointer to the alias you defined in (1). For example `./cicd/run-build --modules-to-build="<alias>"`

3) Exclude the paths you added in (2) from the main [java-pr pull_request trigger paths](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/.github/workflows/java-pr.yml#L23)

See https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/1573 for an example of splitting out the Spanner tests.

## Runners

There are 2 types of runners that CI/CD runs are performed on: GitHub hosted and self-hosted. These are configured in the `runs-on` section of each workflow. For example, `runs-on: [self-hosted, perf]` means that the workflow is running on a self-hosted runner for perf tests.

For more information on GitHub hosted runners, see https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners

Google self-hosted runners are provisioned using the scripts defined in `https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/.github/scripts`.

### Provision new runners

There are instances where we may need to re-provision self-hosted runners, due to unexpected failures, updating 
dependencies, increasing memory, etc. In these cases, there are helper scripts to aid in redeployment of the GitHub 
actions runners.

There are 3 scripts: [configure-runners.sh](../.github/scripts/configure-runners.sh), 
[startup-script.sh](../.github/scripts/startup-script.sh) and 
[shutdown-script.sh](../.github/scripts/shutdown-script.sh). The first is the main script used to provision the runners 
themselves. The startup script is what will be invoked by the GCE VM as it is booted up for the first time and will 
install all necessary packages needed by IT's, unit tests, Release, etc. as well as link the machine as a runner for the 
repo. Likewise, the shutdown script is run when the VM is shutdown.

To provision GitHub actions runners, there are a couple prerequisites
- Must be a maintainer of the repo
- Must have access to GCP project cloud-teleport-testing

Things to remember:
- Running the script will tear down existing runners and provision new ones. This will kill any actions currently
running on any of the runners. Failure to spin up new runner correctly will block PR's and Releases, so use carefully.
- After running the script, it is likely the old runners will still be listed under
https://github.com/GoogleCloudPlatform/DataflowTemplates/settings/actions/runners. Simply force remove these to keep the
repo clean
- The commands below will demonstrate how to provision runners for use with our workflows as they exist today. If there
arises a need to provision runners in a different manner, feel free to modify the scripts directly and open a PR with 
the necessary changes.

To run the configuration script:

1. Set gcloud project to cloud-teleport-testing if not already set
    ```
    gcloud config set project cloud-teleport-testing
    ```

2. Export the GitHub actions token
    ```
    GITACTIONS_TOKEN=$(gcloud secrets versions access latest --secret=gitactions-runner-secret)
    ```

3. Run the script
   
   * For IT runners:
   
      ```
      ./configure-runners.sh \
        -p cloud-teleport-testing \
        -a 269744978479-compute@developer.gserviceaccount.com \
        -t $GITACTIONS_TOKEN
      ```
   
   * For Performance Test Runner
      ```
      ./configure-runners.sh \
        -p cloud-teleport-testing \
        -a 269744978479-compute@developer.gserviceaccount.com \
        -t $GITACTIONS_TOKEN \
        -S perf \
        -s 1
      ```
   
   * For Release Runner
      ```
      ./configure-runners.sh \
        -p cloud-teleport-testing \
        -a 269744978479-compute@developer.gserviceaccount.com \
        -t $GITACTIONS_TOKEN \
        -S release \
        -s 1
      ```

**Note**: To see optional configurable parameters, run `./configure-runners.sh -h`
