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

Merging a PR should use the following flow:

1) PR author creates a PR. If they are an external contributor, checks won't be automatically run.
2) Code reviewers review/iterate with Author until PR is ready to be approved. After reviewing and verifying there is no malicious code, but before step 3 make sure to click "approve and run" to allow their workflows to run.
3) PR reviewer approves PR
4) A maintainer will squash and merge PR once checks are passing.
5) If there are issues and/or merge conflicts, repeat steps 1-4 until conflicts are resolved and checks are passing

Note: All PRs require an approval from at least 1 reviewer and can only be merged by a maintainer.

## GitHub actions

This repository uses GitHub Actions for all CI/CD needs. Please do not introduce other methods of building/running code without consulting with the core Dataflow team.
For information on GitHub Actions and CI/CD workflows, see [CI/CD](./cicd.md).

## Validating and upgrading Beam versions

With each new release of [Apache Beam](https://github.com/apache/beam), the Beam version for the repo needs to be 
upgraded to match. This way, our templates can take advantage of the greatest and latest features of the Beam SDK.

To get ahead of possible failures with each Beam release, we validate the templates by running all the tests against
the Beam release candidate before it is released.

The Beam SDK version(s) are defined in the root [pom.xml](../pom.xml) file. Locate the lines below
```
<beam.version>X.XX.X</beam.version>
<beam-python.version>X.XX.X</beam-python.version>
<beam-maven-repo></beam-maven-repo>
```

The version of Beam Python installed in the template Docker containers are separately defined in various requirements.txt files
in the repo. This allows us to perform safe installs using [hash checking mode](https://pip.pypa.io/en/stable/topics/secure-installs/).
These requirements.txt files are automatically generated using the [generate_all_dependencies.sh](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/python/generate_all_dependencies.sh) script.
This script uses a small set of base_requirements files, which must be manually updated.
To identify the files which must be manually updated, look for all file paths used to generate dependencies in [generate_all_dependencies.sh](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/python/generate_all_dependencies.sh).
For example, if you see the following lines:

```
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/../python/src/main/python/streaming-llm/base_requirements.txt $SCRIPTPATH/../python/src/main/python/streaming-llm/requirements.txt
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/../python/default_base_python_requirements.txt $SCRIPTPATH/__build__/default_python_requirements.txt
sh $SCRIPTPATH/generate_dependencies.sh $SCRIPTPATH/../python/default_base_yaml_requirements.txt $SCRIPTPATH/__build__/default_yaml_requirements.txt
```

That indicates that the following files need to be updated:

```
python/src/main/python/streaming-llm/base_requirements.txt 
python/default_base_python_requirements.txt
python/default_base_yaml_requirements.txt
```

The remaining files can then be generated by running `sh python/generate_all_dependencies.sh` from the root of the repo. 

### Validating the release candidate

When _validating_ the templates, all of these values need to be updated. 
- `<beam.version>` will just take the form of the expected release version
- `<beam-python.version>` will append `rcX` where `X` is the version of the latest release candidate (rc0, rc1, etc.)
- `<beam-maven-repo>` will take the form `https://repository.apache.org/content/repositories/orgapachebeam-XXXX` where 
`XXXX` can be found in the release vote email sent by the release manager for Apache Beam
- All `*_base_*.requirements.txt` files must be updated, after which you must run `sh python/generate_all_dependencies.sh` from the root of the repo.

The `-PvalidateCandidate` profile also needs to be passed to the validation command, or set the `activeByDefault` value
to `true`.
https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/pom.xml#L592

For example, when validating 2.57.0:
```
<beam.version>2.57.0</beam.version>
<beam-python.version>2.57.0rc0</beam-python.version>
<beam-maven-repo>https://repository.apache.org/content/repositories/orgapachebeam-1379</beam-maven-repo>
```
and
```
<profile>
  <id>validateCandidate</id>
    <activation>
      <activeByDefault>true</activeByDefault>
    </activation>
    ...
</profile>
```

Once all the tests have been validated against the release candidate, make sure to send a +1 to the release manager for
Beam to let them know that the latest version works against this repo.

### Upgrading the repo's Beam version

Once a new candidate is released, make sure to edit the `<beam.version>` _only_. The other 2 properties mentioned 
above are for validation _only_.

For example, when upgrading from 2.56.0 to 2.57.0:
```
<beam.version>2.57.0</beam.version>
<beam-python.version>2.57.0</beam-python.version>
<beam-maven-repo></beam-maven-repo>
```
