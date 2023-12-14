import sys
from functools import reduce
from operator import getitem
import datetime as dt
import math

from pyspark.context import SparkContext
from pyspark.sql.functions import lit, explode, arrays_zip, col, array_contains, udf, coalesce, expr, posexplode, first, count, from_unixtime, year, month, dayofmonth, hour, input_file_name

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


def run_job():
    true_v2_input_dyf = (
        glueContext
        .create_sample_dynamic_frame_from_catalog(
            database='true-schema-input-v2',
            table_name='true_v2_input',
            num=10,
        )
        # .create_dynamic_frame_from_options(
        #     connection_type="s3",
        #     connection_options={
        #         "paths": [
        #             # "s3://true-v2-input/data/",
        #             "s3://true-v2-input-subset-for-testing-filter-merge-glue-job",
        #             # "s3://true-v2-input/data/2023/09/10/17/",
        #             # "s3://true-v2-input/data/2023/11/10/15/",
        #             # "s3://true-v2-input/data/2023/11/13/11/",
        #             # "s3://true-v2-input/data/2023/11/15/10/",
        #             # "s3://true-v2-input/data/2023/11/30/11/",
        #             # "s3://true-v2-input/data/2023/11/30/19/",
        #         ],
        #         "recurse": True,
        #     },
        #     format="json",
        #     format_options={
        #         "attachFilename": "input_file_name",
        #     },
        # )
        # .map(add_has_invalid_schema_column)
    )
    
    true_v2_input_dyf.printSchema()


# if __name__ == "__main__":
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext(appName="testing-testing")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

run_job()

job.commit()
