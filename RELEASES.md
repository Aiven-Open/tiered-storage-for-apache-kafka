# Releases and Kafka compatibility

`main` builds against the newest Apache Kafka version it supports and is tested against the older versions down to the oldest one it supports. A release branch keeps the Kafka version that `main` had when the branch was cut, and keeps its test range.

## Branches

| Branch | Builds against | Also tested against (CI) |
|--------|----------------|---------------------|
| `main` | 4.1.0          | 3.8, 3.9, 4.0       |

No release branch exists yet. Add a row when a branch is cut, and change the row of `main` when it moves to a newer Kafka version.

## Rules

- Land every change on `main` first. Cherry-pick fixes that apply to a release branch.
- Do not cherry-pick features, or changes that use APIs missing in the branch's Kafka version.
- A release branch is cut from `main` before `main` moves to a Kafka version that breaks the older ones.
- Release from the branch for the Kafka version of the broker. `release_pr_workflow` checks out `main` only, so change it before the first release from a branch.
- When `main` moves to a new Kafka version, run `make docs` and commit `docs/configs.rst` and `docs/metrics.rst`. Their text depends on the Kafka version.
- The build workflow of each branch runs the build, unit tests and integration tests on the Kafka version it builds against. The legs for the other versions of its range run the build and unit tests only.

## Build workflow

The workflow `main_push_and_pull_request_workflow.yml` is the same on every branch except for one list: the extra Kafka legs in `matrix.include`.

- The build version is the default of `kafkaVersion` in `build.gradle`. A leg overrides it with `ORG_GRADLE_PROJECT_kafkaVersion`.
- The workflow runs on push to `main` and `release/**`, and on every pull request.
- Default legs run the build, unit tests and integration tests. Extra legs run the build and unit tests only, and are experimental until they pass.
- Compile against the newest version of the range. Do not use an API that the oldest version lacks. The extra legs fail if you do.
- To adapt a branch, change the default in `build.gradle` and the `include` list. Change nothing else.

## Add a Kafka version

1. Add the version as an extra leg in the build workflow of the branch that should support it.
2. If the new version fails to compile or test, fix the cause on `main`, or cut a new branch if the fix cannot work on the older Kafka version.
3. Update the table above.

## Verification level

CI compiles and runs unit tests against each Kafka version. This checks source compatibility. It does not run a jar built against the base version on a broker of a newer version.
