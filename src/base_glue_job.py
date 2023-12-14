import sys
from typing import Union
from pyspark import RDD

from pyspark.context import SparkContext
from pyspark.sql import DataFrame

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


class BaseGlueJob:
    app_name: str = None

    def __init__(self) -> None:
        self.job_args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        self.sc = SparkContext(appName=self.app_name)
        self.glueContext = GlueContext(self.sc)
        self.glue_job = Job(self.glueContext)

    def load_data(self) -> Union[RDD, DataFrame, DynamicFrame]:
        raise NotImplementedError

    def process_data(
        self, data: Union[RDD, DataFrame, DynamicFrame]
    ) -> Union[RDD, DataFrame, DynamicFrame]:
        raise NotImplementedError

    def write_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> None:
        raise NotImplementedError

    def execute(self) -> None:
        self.glue_job.init(self.job_args["JOB_NAME"], self.job_args)
        data = self.load_data()
        data = self.process_data(data)
        self.write_data(data)
        print("Successfully completed job. Committing...")
        self.glue_job.commit()
        print("Successfully committed job.")
