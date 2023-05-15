# SierraCodePoller

The SierraCodePoller periodically checks Sierra for descriptions of various codes and sends the results to Kinesis streams for ingest into the [BIC](https://github.com/NYPL/BIC)

## Running locally
* `cd` into this directory
* Add your `AWS_PROFILE` to the config file for the environment you want to run
  * Alternatively, you can manually export it (e.g. `export AWS_PROFILE=nypl-digital-dev`)
* Run `ENVIRONMENT=<env> python3 main.py`
  * `<env>` should be the config filename without the `.yaml` suffix
  * `make run` will run the poller using the development environment
* Alternatively, to build and run a Docker container, run:
```
docker image build -t sierra-code-poller:local .

docker container run -e ENVIRONMENT=<env> -e AWS_PROFILE=<aws_profile> sierra-code-poller:local
```

## Git workflow
This repo uses the [Main-QA-Production](https://github.com/NYPL/engineering-general/blob/main/standards/git-workflow.md#main-qa-production) git workflow.

[`main`](https://github.com/NYPL/sierra-code-poller/tree/main) has the latest and greatest commits, [`qa`](https://github.com/NYPL/sierra-code-poller/tree/qa) has what's in our QA environment, and [`production`](https://github.com/NYPL/sierra-code-poller/tree/production) has what's in our production environment.

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
N/A

## Environment variables
Note that the QA and production env files are actually read by the deployed service, so do not change these files unless you want to change how the service will behave in the wild -- these are not meant for local testing.

| Name        | Notes           |
| ------------- | ------------- |
| `AWS_REGION` | Always `us-east-1`. The AWS region used for the KMS and Kinesis clients. |
| `SIERRA_DB_PORT` | Always `1032` |
| `SIERRA_DB_NAME` | Always `iii` |
| `SIERRA_DB_HOST` | Encrypted Sierra host (either local, QA, or prod) |
| `SIERRA_DB_USER` | Encrypted Sierra user. There is only one user for this application, so this is always the same. |
| `SIERRA_DB_PASSWORD` | Encrypted Sierra password for the user. There is only one user for this application, so this is always the same. |
| `BASE_SCHEMA_URL` | Base URL for the Platform API endpoint from which to retrieve the Avro schemas |
| `KINESIS_BATCH_SIZE` | How many records should be sent to Kinesis at once. Kinesis supports up to 500 records per batch. |
| `BASE_KINESIS_STREAM_ARN` | Encrypted base ARN for the Kinesis streams the poller sends the encoded data to |
| `IGNORE_KINESIS` (optional) | Whether sending records to Kinesis should not be done. If this is `True`, then the `BASE_KINESIS_STREAM_ARN` variable is not necessary. |
| `LOG_LEVEL` (optional) | What level of logs should be output. Set to `info` by default. |