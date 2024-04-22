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

If you are interested in introducing a new template, please send an email to the core Dataflow Templates
team before doing so. Any new templates must be flex templates in the v2 directory

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