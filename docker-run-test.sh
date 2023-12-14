PROFILE_NAME="default"
AWS_REGION="eu-central-1"
SCRIPT_FILE_NAME="filter-merge-glue-job.py"
WORKSPACE_LOCATION="/Users/ben/code/truemetrics_spark/jobs"
docker run -it \
    -v ~/.aws:/home/glue_user/.aws \
    -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_PROFILE=$PROFILE_NAME \
    -e DISABLE_SSL=true --rm \
    -p 4040:4040 \
    -p 18080:18080 \
    --name glue_spark_submit amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    spark-submit /home/glue_user/workspace/$SCRIPT_FILE_NAME
