# Releases and Kafka compatibility

`main` builds against a baseline Apache Kafka version and is tested against every version in its supported compatibility range. A release branch keeps the baseline version that `main` had when the branch was cut, and keeps its test range.

## Branches

| Branch | Build baseline | Also tested against (CI) |
|--------|----------------|--------------------------|
| `main` | 4.1.0          | 3.8, 3.9, 4.0, 4.2, 4.3, 4.4 |

No release branch exists yet. Add a row when a branch is cut, and change the row of `main` when it moves to a newer Kafka version.

## Rules

- Land every change on `main` first. Cherry-pick fixes that apply to a release branch.
- Do not cherry-pick features, or changes that use APIs missing in the branch's Kafka version.
- A release branch is cut from `main` before `main` moves to a Kafka version that breaks the older ones.
- Release from a branch whose compatibility range includes the broker's Kafka version. `release_pr_workflow` checks out `main` only, so change it before the first release from a branch.
- When `main` moves to a new Kafka version, run `make docs` and commit `docs/configs.rst` and `docs/metrics.rst`. Their text depends on the Kafka version.
- The build workflow of each branch runs the build, unit tests and integration tests on the Kafka version it builds against. The legs for the other versions of its range run the build and unit tests only.

## Build workflow

The workflow `main_push_and_pull_request_workflow.yml` is the same on every branch except for one list: the extra Kafka legs in `matrix.include`.

- The build version is the default of `kafkaVersion` in `build.gradle`. A leg overrides it with `ORG_GRADLE_PROJECT_kafkaVersion`.
- The workflow runs on push to `main` and `release/**`, and on every pull request.
- Default legs run the build, unit tests and integration tests. Extra legs run the build and unit tests only. A failed extra leg fails the workflow.
- Code must compile against every version in the declared range. The extra legs fail if it does not.
- To adapt a branch, change the default in `build.gradle` and the `include` list. Change nothing else.

## Add a Kafka version

1. Add the version as an extra leg in the build workflow of the branch that should support it.
2. If the new version fails to compile or test, fix the cause on `main`, or cut a new branch if the fix cannot work on the older Kafka version.
3. Update the table above.

## Verification level

CI compiles and runs unit tests against each Kafka version. This checks source compatibility. Integration and e2e tests use the 4.1.0 build baseline.
