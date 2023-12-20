from functools import cached_property
import sys
from typing import Union
import datetime as dt

from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import base_utils


class BaseGlueJob:
    app_name: str = None

    def __init__(self) -> None:
        argv = sys.argv
        if "--JOB_NAME" not in argv:
            argv += ["--JOB_NAME", "test-job"]
        self.job_args = getResolvedOptions(argv, ["JOB_NAME"])
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
        self.sc = self.spark.sparkContext
        self.glueContext = GlueContext(self.sc)
        self.glue_job = Job(self.glueContext)

    @cached_property
    def run_timestamp(self) -> str:
        return str(int(dt.datetime.utcnow().timestamp()))

    def load_data(self) -> Union[RDD, DataFrame, DynamicFrame]:
        raise NotImplementedError

    def process_data(self, data: base_utils.GlueJobDataObjectType) -> DynamicFrame:
        raise NotImplementedError

    def write_data(self, data: DynamicFrame) -> None:
        raise NotImplementedError

    def execute(self) -> None:
        self.glue_job.init(self.job_args["JOB_NAME"], self.job_args)
        data = self.load_data()
        data = self.process_data(data)
        self.write_data(data)
        print("Successfully completed job. Committing...")
        self.glue_job.commit()
        print("Successfully committed job.")
