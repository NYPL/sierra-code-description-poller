# SierraCodeDescriptionPoller

This repository contains the code used by the [SierraCodeDescriptionPoller-qa](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/SierraCodeDescriptionPoller-qa?tab=code) and [SierraCodeDescriptionPoller-production](https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/SierraCodeDescriptionPoller-production?tab=code) AWS lambda functions. The poller periodically checks Sierra for descriptions of various codes, compares these codes to what's stored in the [BIC](https://github.com/NYPL/BIC), and updates the BIC appropriately for new/updated/deprecated codes.

## Running locally
* `cd` into this directory
* Install all the required packages in `devel_requirements.txt` in a virtual environment
* Add your `AWS_PROFILE` to `config/devel.yaml`
  * Alternatively, you can manually export it (e.g. `export AWS_PROFILE=nypl-digital-dev`)
* Run `ENVIRONMENT=<env> python main.py`
  * `<env>` should be the config filename without the `.yaml` suffix
  * `make run` will run the poller using the development environment

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

[`main`](https://github.com/NYPL/sierra-code-description-poller/tree/main) has the latest and greatest commits, [`qa`](https://github.com/NYPL/sierra-code-description-poller/tree/qa) has what's in our QA environment, and [`production`](https://github.com/NYPL/sierra-code-description-poller/tree/production) has what's in our production environment.

### Ideal Workflow
- Cut a feature branch off of `main`
- Commit changes to your feature branch
- File a pull request against `main` and assign a reviewer
  - In order for the PR to be accepted, it must pass all unit tests, have no lint issues, and update the CHANGELOG (or contain the Skip-Changelog label in GitHub)
- After the PR is accepted, merge into `main`
- Merge `main` > `qa`
- Deploy app to QA and confirm it works
- Merge `qa` > `production`
- Deploy app to production and confirm it works

## Deployment
CI/CD is not enabled. To deploy a new version of this function, first modify the code in the git repo and open a pull request to the appropriate environment branch. Then run `source deployment_script.sh` and upload the resulting zip. Note that if any files are added or deleted, this script must be modified. For more information, see the directions [here](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html).

## Environment variables
For non-devel environments, these variables are set in the Lambda.

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the KMS and Kinesis clients. |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_HOST` | Encrypted Sierra host (either local, QA, or prod) |
| `SIERRA_DB_USER` | Encrypted Sierra user. There is only one user for this application, so this is always the same. |
| `SIERRA_DB_PASSWORD` | Encrypted Sierra password for the user. There is only one user for this application, so this is always the same. |
| `REDSHIFT_DB_NAME` | Which Redshift database to query (either `dev`, `qa`, or `production`) |
| `REDSHIFT_DB_HOST` | Encrypted Redshift cluster endpoint |
| `REDSHIFT_DB_USER` | Encrypted Redshift user |
| `REDSHIFT_DB_PASSWORD` | Encrypted Redshift password for the user |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `info` by default. |