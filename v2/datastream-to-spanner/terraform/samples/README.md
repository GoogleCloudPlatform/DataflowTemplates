## Terraform samples for live migration

This repository provides samples for common scenarios users might have while trying to run a live migration to Spanner.

Pick a sample that is closest to your use-case, and use it as a starting point, tailoring it to your own specific needs.

## List of examples

1. [MySQL source to Spanner end-to-end](all-infrastructure/README.md)
2. [Pre-configured Datastream connection profiles](pre-configured-conn-profiles/README.md)

## How to add a new sample

It is strongly recommended to copy an existing sample and modify it according to the scenario you are trying to cover.
This ensures uniformity in the style in which terraform samples are written.

```shell
mkdir my-new-sample
cp -r all-infrastructure/* my-new-sample/
```