## Terraform samples for bulk migration

This repository provides samples for common scenarios users might have while trying to run a bulk migration to Spanner.

Pick a sample that is closest to your use-case, and use it as a starting point, tailoring it to your own specific needs.

## List of examples

1. [Launching multiple bulk migration jobs](multiple-jobs/README.md)

## How to add a new sample

It is strongly recommended to copy an existing sample and modify it according to the scenario you are trying to cover.
This ensures uniformity in the style in which terraform samples are written.

```shell
mkdir my-new-sample
cd my-new-sample
cp -r multiple-jobs/
```