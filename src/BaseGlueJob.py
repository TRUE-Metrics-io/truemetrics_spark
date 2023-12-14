import sys
from functools import reduce
from operator import getitem
import datetime as dt
import math
from typing import Union
from pyspark import RDD

from pyspark.context import SparkContext
from pyspark.sql.functions import lit, explode, arrays_zip, col, array_contains, udf, coalesce, expr, posexplode, first, count, from_unixtime, year, month, dayofmonth, hour, input_file_name
from pyspark.sql import DataFrame

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


class BaseGlueJob:
  app_name: str = None

  def __init__(self) -> None:
    self.job_args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    self.sc = SparkContext(appName=self.app_name)
    self.glueContext = GlueContext(self.sc)
    self.glue_job = Job(self.glueContext)
  
  def load_data(self) -> Union[RDD, DataFrame, DynamicFrame]:
    raise NotImplementedError

  def process_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> Union[RDD, DataFrame, DynamicFrame]:
    raise NotImplementedError
  
  def write_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> None:
    raise NotImplementedError
  
  def execute_job(self) -> None:
    self.glue_job.init(self.job_args['JOB_NAME'], self.job_args)
    data = self.load_data()
    data = self.process_data(data)
    self.write_data(data)
    self.glue_job.commit()
