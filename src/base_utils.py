from typing import Union

from pyspark import RDD
from pyspark.sql import DataFrame
from awsglue.dynamicframe import DynamicFrame


GlueJobDataObjectType = Union[RDD, DataFrame, DynamicFrame]
