# Maintainers Guide

This guide is meant for maintainers of the repo who have write access
and are [Code Owners](./code-owners.md) of some portion of the repository.

If you are looking for guidance on general code contribution, see
[Code Contributions](./code-contributions.md).

## Maintainer Expectations

Maintainers of this repository are expected to own their templates completely.

This includes:
- Being responsive to code review requests
- Fixing recurring bugs/pain points
- Keeping tests and any template-specific infrastructure healthy
- Abiding by the repository standards mentioned below

The core Dataflow infrastructure team will manage the following:

- All shared infrastructure (including releases, Beam version upgrades, etc...)
- Repository governance (managing area ownership, responding to any general repository issues)
- Core Dataflow Templates infrastructure (beyond this repository)

## Repository Standards

Generally, templates in this repository are managed/governed by the owning teams. The following standards
must be maintained across all Templates:

1) Red or flaky tests must be dealt with promptly. These are P1 bugs and should be treated with similar SLOs.
2) Code owners must be consulted or review PRs before they are merged for a given area.
3) Documentation must be kept up to date and consistent using built-in tooling (for more information see [Code Contributions](./code-contributions.md))

### Introducing New Templates

If you are interested in introducing a new template, please file an issue using the [Google Issue Tracker](https://issuetracker.google.com/issues/new?component=187168&template=0) before doing so. Any new templates must be flex templates in the v2 directory.

For documentation on adding new templates, see the [code contribution guide](./code-contributions.md).

### Forking Beam Code

Templates **should not** fork existing Beam I/Os or other code in order to accelerate development.
This leads to significant pain around keeping versions consistent, avoiding conflicts, and avoiding bugs.

Existing Templates with forked Beam code may keep that code forked, but the owning teams are responsible
for handling any issues that arise as a result of this setup.

## Merging PRs

This repo's code currently is mirrored in Google's internal source control system. Merging a PR should use the following flow:

1) PR author creates a PR. If they are an external contributor, checks won't be automatically run.
2) Code reviewers review/iterate with Author until PR is ready to be approved. After reviewing and verifying there is no malicious code, but before step 3 make sure to click "approve and run" to allow their workflows to run.
3) PR reviewer approves PR
4) PR reviewer adds "Google LGTM" tag to PR. Google tooling will now automatically create an internal CL.
5) After a minute or two, click the "import/copybara" check in the checks section. This will navigate to an internal UI.
6) Approve the change internally. At this point, the change will get automatically merged internally and externally.

We are actively working to deprecate this process and use GitHub as the only source of truth.
If you encounter unresolvable issues with this flow, please reach out to the Dataflow team directly.

## GitHub actions

There are several workflows that leverage GitHub actions to keep the repo healthy. Of these workflows, there are 
currently 2 that are run on self-hosted runners on GCP - [Java PR](../.github/workflows/java-pr.yml) which is used to 
test PR's and [Release](../.github/workflows/release.yml) which is the workflow used for releasing new templates each 
week.

### Provision new resources

#### GitHub actions runners

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

    **Note**: To see optional configurable parameters, run `./configure-runners.sh -h`

#### CloudSQL proxy server VM

Datastream integration tests (IT's) that connect to a CloudSQL database rely on the 
[Cloud auth proxy](https://cloud.google.com/sql/docs/mysql/sql-proxy) to make a connection profile. Due to connection
issues related to which port the proxy server is hosted on, there is a separate workflow for provisioning the
dedicated proxy server VM.

1. Run the helper script
    ```
    ./configure-proxy.sh \
      -p cloud-teleport-testing \
      -a 269744978479-compute@developer.gserviceaccount.com
    ```
    **Note**: To see optional configurable parameters, run `./configure-runners.sh -h`

2. Navigate to Datastream and try to make connection profiles using the internal IP of the proxy VM as
    the hostname, and the port which can be found for each SQL flavor in
    [proxy-startup-script.sh](../.github/scripts/proxy-startup-script.sh).

If there are issues connecting to the Proxy server from Datastream, follow the instructions below:
1. SSH into the newly created VM
    * This can be done using Cloud Console UI, or using gcloud CLI. The name of the machine will
        be `nokill-gitactions-proxy-temp` appended with a random suffix, unless the base name was overridden 
        when running th helper script.
2. Try hosting the proxy server on a different port using the command
    ```
   cd /home/runner && ./cloud-sql-proxy --address 0.0.0.0 --port ${PORT} ${CLOUD_SQL_PREFIX}-postgres
   ```
   where `CLOUD_SQL_PREFIX` is  
   `${PROJECT}:us-central1:nokill-gitactions-{mysql | postgres | oracle}`,  
    For example, `cloud-teleport-testing:us-central1:nokill-gitactions-mysql`
3. It will take some trial and error to find a port that works, but just keep trying to create Datastream connection
    profiles until it connects. Once connected, run the working command in the background.  
    With the working command still running, press `ctrl-Z` then run the following:
    ```
   disown -h %1
   bg 1
   ```
4. Update the commands in [proxy-startup-script.sh](../.github/scripts/proxy-startup-script.sh) to use the updated 
   ports.