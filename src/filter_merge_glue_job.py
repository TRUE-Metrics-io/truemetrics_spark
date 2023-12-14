from typing import Union
from pyspark import RDD

from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, arrays_zip, col, expr, first, from_unixtime, year, month, dayofmonth, hour
from base_glue_job import BaseGlueJob

from awsglue.dynamicframe import DynamicFrame
import filter_merge_glue_job_utils


class FilterMergeGlueJob(BaseGlueJob):
    app_name = 'filter-merge-glue-job'

    def load_data(self) -> Union[RDD, DataFrame, DynamicFrame]:
        print("Loading data...")
        data = (
            self.glueContext
            .create_dynamic_frame_from_options(
                connection_type="s3",
                connection_options={
                    "paths": [
                        # "s3://true-v2-input/data/2023/11/30/22/",
                        "s3://truemetrics-spark-test-data/filter-merge-glue-job/true-v2-input-files-correct-schema-all-dtypes-values/"
                    ],
                    "recurse": True,
                },
                format="json",
                format_options={
                    "attachFilename": "input_file_name",
                },
            )
        )
        print(f"Successfully loaded data. {data.count()} total rows in data (i.e. files).")
        print("Schema of loaded data:")
        data.printSchema()
        return data

    def process_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> Union[RDD, DataFrame, DynamicFrame]:
        data_with_has_invalid_schema_column = (
            data
            .map(filter_merge_glue_job_utils.add_has_invalid_schema_column)
        )
        print(
            f"""All bad source files and their badness reason: \n{
                data_with_has_invalid_schema_column
                .filter(lambda x: x['has_invalid_schema'] != '')
                .select_fields(['has_invalid_schema', 'input_file_name'])
                .show(100)
            }"""
        )
        data_w_valid_schema = (
            data_with_has_invalid_schema_column
            .filter(lambda x: x["has_invalid_schema"] == "")
        )
        print(f"Total rows in data with valid schema: {data_w_valid_schema.count()}")
        print("Schema of data with valid schema:")
        data_w_valid_schema.printSchema()

        valid_data_sensor_reading_timestamp_level = (
            data_w_valid_schema
                .toDF()
    
                # Keep only some fields
                .select(*[
                    "input_file_name",
                    'body.metadata.id_phone',
                    'body.data',
                    'id_api_key',
                ])

                # Update schema
                .withColumnRenamed('body.metadata.id_phone', 'id_phone')
                .withColumnRenamed('body.data', 'data')
                
                # Flatten data packets column.
                .withColumn("data", explode("data"))
    
                # Break up the data struct into separate columns.
                .withColumn("sensor_names", col("data.names"))
                .withColumn("sensor_readings", col("data.values"))
                .withColumn("sensor_reading_timestamps", col("data.t_utc"))
                .drop("data")
                
                # DEBUG: Reduce lengths of data.values and data.t_utc to just 3 elements so as to process less data.
                .withColumn("sensor_readings", expr("slice(sensor_readings, 1, 4)"))  # col("data.values"))
                .withColumn("sensor_reading_timestamps", expr("slice(sensor_reading_timestamps, 1, 4)"))  # col("data.t_utc"))
                
                # Zip the sensor_reading_timestamps array with the sensor_readings array and explode it so that one row of the dataframe corresponds to one sensor-reading timestamp.
                .withColumn("tmp", arrays_zip("sensor_reading_timestamps", "sensor_readings"))
                .withColumn("tmp", explode("tmp"))
                .select(
                    "*",
                    col("tmp.sensor_reading_timestamps").alias("sensor_reading_timestamp"),
                    col("tmp.sensor_readings").alias("sensor_reading")
                )
                .drop("sensor_reading_timestamps", "sensor_readings", "tmp")
                
                # Zip the sensor_names array with the sensor_reading array.
                .withColumn("tmp", arrays_zip("sensor_names", "sensor_reading"))
                .withColumn("tmp", explode("tmp"))
                .select(
                    "*",
                    col("tmp.sensor_names").alias("sensor_name"),
                    col("tmp.sensor_reading").alias("individual_sensor_reading")
                )
                .drop("tmp", "sensor_names", "sensor_reading")
                .withColumnRenamed("individual_sensor_reading", "sensor_reading")
                
                # Pivot the data to the desired schema, of each row as its own timestamp, and one column for each sensor.
                .groupBy(
                    'input_file_name',
                    'id_api_key',
                    'id_phone',
                    'sensor_reading_timestamp',
                )
                .pivot("sensor_name")
                .agg(first("sensor_reading"))  # There's one sensor reading per sensor-name/timestamp, so any agg function (first, min, max, random) works here.
        )

        print("Now that conversion to tabular format is done, check sensor_reading_timestamp dtype before proceeding:")
        sensor_reading_timestamp_dtype = valid_data_sensor_reading_timestamp_level.select("sensor_reading_timestamp").dtypes[0][1]
        sensor_reading_timestamp_dtype_is_good = sensor_reading_timestamp_dtype == "double"
        if not sensor_reading_timestamp_dtype_is_good:
            print(f"Problem with t_utc dtype: {sensor_reading_timestamp_dtype}")
            print(f"The problem is in these files:")
            try:
                files_with_bad_t_utc_dtype = valid_data_sensor_reading_timestamp_level.filter(col("sensor_reading_timestamp.double").isNull()).select("input_file_name").distinct().collect()
                for file_with_bad_t_utc_dtype in files_with_bad_t_utc_dtype:
                    print(file_with_bad_t_utc_dtype)
            except:
                print("Something went wrong when checking the `t_utc` dtype.")
        else:
            print("No problem with t_utc dtype.")
        
        try:
            current_schema = dict(valid_data_sensor_reading_timestamp_level.dtypes)
            sensor_target_schema_conversion_mapping = {
                field: {"current_dtype_is_struct": current_schema[field].startswith("struct"), "desired_dtype": target_dtype}
                for field, target_dtype in filter_merge_glue_job_utils.TARGET_SENSOR_SCHEMA.items()
                if field in current_schema.keys()
            }
            print("sensor_target_schema_conversion_mapping:")
            print(sensor_target_schema_conversion_mapping)
        except:
            print("Something went wrong with the creation of the schema conversion mapping.")
        
        result_data = (
            valid_data_sensor_reading_timestamp_level
    
            # Coerce the values of each sensor column to the target data type.
            # If the table contains mixed datatypes (i.e. resulting in a struct column), then the correct datatype must be selected from the struct; otherwise
            # the column can be directly selected.
            .withColumns({
                sensor_name: col(f"{sensor_name}.{data_type_conversion_dict['desired_dtype']}") if data_type_conversion_dict["current_dtype_is_struct"] else col(sensor_name)
                for sensor_name, data_type_conversion_dict in sensor_target_schema_conversion_mapping.items()
                if sensor_name in valid_data_sensor_reading_timestamp_level.columns
            })
            
            # Revert the column name from `sensor_reading_timestamp` back to `t_utc`, and extract from struct if necessary.
            .withColumnRenamed("sensor_reading_timestamp", "t_utc")
            .withColumn("t_utc", col("t_utc") if sensor_reading_timestamp_dtype_is_good else col("t_utc.double"))

            # Add formatted timestamp column, for partitioning the dataframe upon writing.
            .withColumn("timestamp", from_unixtime(col("t_utc")))
            
            # Add the time partitioning columns and drop timestamp column.
            .withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .drop("timestamp")
        )

        try:
            fraction_of_t_utc_values_that_are_null = (
                result_data
                .select(col("t_utc").isNull().cast("float").alias("fraction_nulls"))
                .agg({"fraction_nulls": "avg"})
                .collect()[0][0]
            )
            print(f"Fraction of t_utc values in final true_v2_good_data_merged_df result that are null: {fraction_of_t_utc_values_that_are_null}")
        except:
            print("Error measuring share of bad `t_utc` values.")

        print("true_v2_good_data_merged_df schema:")
        result_data.printSchema()
        return result_data
    
    def write_data(self, data: Union[RDD, DataFrame, DynamicFrame]) -> None:
        data.printSchema()


if __name__ == "__main__":
    job = FilterMergeGlueJob()
    job.execute()
