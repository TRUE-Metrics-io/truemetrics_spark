SCRIPT_FILE_NAME="filter_merge_glue_job.py"
WORKSPACE_LOCATION="./src"
docker run -it \
    -v ~/.aws:/home/glue_user/.aws \
    -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_REGION=$AWS_GLUE__REGION \
    -e AWS_ACCESS_KEY_ID=$AWS_GLUE__ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_GLUE__SECRET_ACCESS_KEY \
    -e DISABLE_SSL=true --rm \
    -p 4040:4040 \
    -p 18080:18080 \
    --name glue_spark_submit amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    spark-submit /home/glue_user/workspace/$SCRIPT_FILE_NAME --JOB_NAME filter_merge_glue_job
