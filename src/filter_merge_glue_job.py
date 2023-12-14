from typing import Union
from pyspark import RDD

from pyspark.sql import DataFrame
from base_glue_job import BaseGlueJob

from awsglue.dynamicframe import DynamicFrame


class FilterMergeGlueJob(BaseGlueJob):
    app_name = 'filter-merge-glue-job'
  
    def load_data(self) -> Union[RDD, DataFrame, DynamicFrame]:
        data = (
            self.glueContext
            .create_sample_dynamic_frame_from_catalog(
                database='true-schema-input-v2',
                table_name='true_v2_input',
                num=10,
            )
        )
        return data

    def process_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> Union[RDD, DataFrame, DynamicFrame]:
        return data
    
    def write_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> None:
        data.printSchema()


if __name__ == "__main__":
    job = FilterMergeGlueJob()
    job.execute()
