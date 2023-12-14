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
    data = [("John", 25), ("Alice", 30), ("Bob", 22)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Show the original DataFrame
    print(df.collect())

    # Perform a simple transformation (selecting only the "Name" column)
    transformed_df = df.select(col("Name"))

    # Show the transformed DataFrame
    print(transformed_df.collect())


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext(appName="testing-testing")
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    run_job()

    job.commit()
