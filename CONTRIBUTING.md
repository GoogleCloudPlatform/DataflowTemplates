# Contributing

Thanks for your interest in contributing to the Google Cloud Dataflow Templates
repo!

To get started contributing:

1. Sign a Contributor License Agreement (see details below).
1. Fork the repo, develop and test your code changes.
1. Ensure that your code adheres to the existing style.
1. Ensure that your code has an appropriate set of unit tests which all pass.
1. Run `mvn verify` to verify all tests and checks pass.
1. Submit a pull request.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## New Templates/Features

This repo is generally not accepting contributions or feature requests for new
templates or large chunks of new functionality unless a partner organization is
willing to sponsor building/maintaining/supporting those templates in a formal
agreement with Google.

If you are part of an organization interested in this, please reach out to
the Dataflow team directly using the [Google Issue Tracker](https://issuetracker.google.com/issues/new?component=187168&template=0).

Existing Templates will continue to receive bug fixes and minor feature upgrades.
Examples of features that will continue to be considered include:

- Exposing new parameters/features added to existing I/Os in Beam
- Performance improvements that don't dramatically alter the template
- Small quality of life improvements that make templates more usable

## Making Code or Documentation Changes

For information on making code contributions, see our
[code contribution guide](./contributor-docs/code-contributions.md).

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

Some PRs will receive automatic reviews, while some will require requesting
review manually. For information on this process, see
[Code Owners](./contributor-docs/code-owners.md).
