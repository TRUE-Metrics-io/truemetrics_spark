# truemetrics_spark
All code for truemetrics AWS Glue jobs.

## Running a Glue Job Locally
If you want the AWS Glue IAM credentials, ask @benfeifke.

### MacOS
Update `docker-run-local.sh` according to your needs:
```bash
#### truemetrics_spark/docker-run-local.sh ####
WORKSPACE_LOCATION="./src"
JOB_NAME="filter_merge_glue_job"  # The Job Name here.
docker run -it \
    -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_REGION=$AWS_GLUE__REGION \  # Your AWS credentials here...
    -e AWS_ACCESS_KEY_ID=$AWS_GLUE__ACCESS_KEY_ID \  # ...here...
    -e AWS_SECRET_ACCESS_KEY=$AWS_GLUE__SECRET_ACCESS_KEY \  # ...and here.
    -e DISABLE_SSL=true --rm \
    -p 4040:4040 \
    -p 18080:18080 \
    --name glue_spark_submit amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    spark-submit /home/glue_user/workspace/$JOB_NAME.py --JOB_NAME $JOB_NAME
```
Then copy-paste the contents of this file into terminal and run.

### Windows
...

## Contributing
### Adding a New Glue Job
Want to add a new Glue Job? Add a new file to `truemetrics_spark/src/<job-name>_glue_job.py` that implements a class called `<JobName>GlueJob(BaseGlueJob)` (i.e. the new Glue job should inherit the base glue job class).

## Pre-Commit
To enforce code quality, this repo uses pre-commit hooks. In particular, [`ruff`](https://github.com/astral-sh/ruff) is used as a code formatter/linter by way of [the `ruff` pre-commit hook](https://github.com/astral-sh/ruff-pre-commit).
To use this, please take the following steps to set up `pre-commit` locally:
1. `pip install pre-commit` to install `pre-commit` to your environment.
2. `pre-commit install` to set up the `pre-commit` hooks.

## References
Helpful links go here:
- https://stackoverflow.com/questions/75455266/how-to-pass-job-parameters-to-aws-glue-libs-docker-container
- https://tedspence.com/using-aws-single-sign-on-within-your-docker-containers-eb95d0cc377a
