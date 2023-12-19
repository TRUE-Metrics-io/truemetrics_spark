from pyspark.sql.functions import (
    explode,
    arrays_zip,
    col,
    first,
    from_unixtime,
    year,
    month,
    dayofmonth,
    hour,
    lit,
)
from awsglue.dynamicframe import DynamicFrame

from base_glue_job import BaseGlueJob
import filter_merge_glue_job_utils
import base_utils


class FilterMergeGlueJob(BaseGlueJob):
    app_name = "filter-merge-glue-job"

    def load_data(self) -> base_utils.GlueJobDataObjectType:
        print("Loading data...")
        # TEST_ID_API_KEY = "bz4k1nou12"
        # TEST_ID_PHONE = "9aa3f129-5093-498c-8326-ddbafdd246fc"
        data = self.glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options={
                "paths": [
                    # "s3://true-v2-input/data",
                    "s3://true-v2-input/data/2023/11/30/22",
                ],
                "recurse": True,
            },
            format="json",
            format_options={
                "attachFilename": "input_file_name",
            },
            # ).filter(
            #     lambda x: (x["id_api_key"] == TEST_ID_API_KEY)
            #     and (x["body"]["metadata"]["id_phone"] == TEST_ID_PHONE)
        )
        print(
            f"Successfully loaded data. {data.count()} total rows in data (i.e. files)."
        )
        print("Schema of loaded data:")
        data.printSchema()
        return data

    def process_data(self, data: base_utils.GlueJobDataObjectType) -> DynamicFrame:
        data_with_has_invalid_schema_column = data.map(
            filter_merge_glue_job_utils.add_has_invalid_schema_column
        )
        print(
            f"""All bad source files and their badness reason: \n{
                data_with_has_invalid_schema_column
                .filter(lambda x: x['has_invalid_schema'] != '')
                .select_fields(['has_invalid_schema', 'input_file_name'])
                .show(100)
            }"""
        )
        data_w_valid_schema = data_with_has_invalid_schema_column.filter(
            lambda x: x["has_invalid_schema"] == ""
        )
        # print(f"Total rows in data with valid schema: {data_w_valid_schema.count()}")
        # print("Schema of data with valid schema:")
        # data_w_valid_schema.printSchema()

        valid_data_sensor_reading_timestamp_level = (
            data_w_valid_schema.toDF()
            # Keep only some fields
            .select(
                *[
                    "input_file_name",
                    "body.metadata.id_phone",
                    "body.data",
                    "id_api_key",
                ]
            )
            # Update schema
            .withColumnRenamed("body.metadata.id_phone", "id_phone")
            .withColumnRenamed("body.data", "data")
            # Flatten data packets column.
            .withColumn("data", explode("data"))
            # Break up the data struct into separate columns.
            .withColumn("sensor_names", col("data.names"))
            .withColumn("sensor_readings", col("data.values"))
            .withColumn("sensor_reading_timestamps", col("data.t_utc"))
            .drop("data")
            # Zip the sensor_reading_timestamps array with the sensor_readings array and explode it so that one row of the dataframe corresponds to one sensor-reading timestamp.
            .withColumn(
                "tmp", arrays_zip("sensor_reading_timestamps", "sensor_readings")
            )
            .withColumn("tmp", explode("tmp"))
            .select(
                "*",
                col("tmp.sensor_reading_timestamps").alias("sensor_reading_timestamp"),
                col("tmp.sensor_readings").alias("sensor_reading"),
            )
            .drop("sensor_reading_timestamps", "sensor_readings", "tmp")
            # Zip the sensor_names array with the sensor_reading array.
            .withColumn("tmp", arrays_zip("sensor_names", "sensor_reading"))
            .withColumn("tmp", explode("tmp"))
            .select(
                "*",
                col("tmp.sensor_names").alias("sensor_name"),
                col("tmp.sensor_reading").alias("individual_sensor_reading"),
            )
            .drop("tmp", "sensor_names", "sensor_reading")
            .withColumnRenamed("individual_sensor_reading", "sensor_reading")
            # Pivot the data to the desired schema, of each row as its own timestamp, and one column for each sensor.
            .groupBy(
                "input_file_name",
                "id_api_key",
                "id_phone",
                "sensor_reading_timestamp",
            )
            .pivot("sensor_name")
            .agg(
                first("sensor_reading")
            )  # There's one sensor reading per sensor-name/timestamp, so any agg function (first, min, max, random) works here.
        )

        sensor_reading_timestamp_dtype_is_good = (
            self.validate_and_report_sensor_reading_timestamp_dtype(
                valid_data_sensor_reading_timestamp_level
            )
        )

        sensor_target_schema_conversion_mapping = (
            self.get_sensor_target_schema_conversion_mapping(
                valid_data_sensor_reading_timestamp_level
            )
        )

        result_data = (
            valid_data_sensor_reading_timestamp_level
            # Coerce the values of each sensor column to the target data type.
            # If the table contains mixed datatypes (i.e. resulting in a struct column), then the correct datatype must be selected from the struct; otherwise
            # the column can be directly selected.
            .withColumns(
                {
                    sensor_name: col(
                        f"{sensor_name}.{data_type_conversion_dict['desired_dtype']}"
                    )
                    if data_type_conversion_dict["current_dtype_is_struct"]
                    else col(sensor_name)
                    for sensor_name, data_type_conversion_dict in sensor_target_schema_conversion_mapping.items()
                    if sensor_name in valid_data_sensor_reading_timestamp_level.columns
                }
            )
            # Revert the column name from `sensor_reading_timestamp` to `t_utc`, and extract from struct if necessary.
            .withColumnRenamed("sensor_reading_timestamp", "t_utc")
            .withColumn(
                "t_utc",
                col("t_utc")
                if sensor_reading_timestamp_dtype_is_good
                else col("t_utc.double"),
            )
            # Add formatted timestamp column, for partitioning the dataframe upon writing.
            .withColumn("timestamp", from_unixtime(col("t_utc")))
            # Add the time partitioning columns and drop timestamp column.
            .withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .drop("timestamp")
            # Add a column to indicate the creation timestamp of the row.
            .withColumn("row_created_at_timestamp_utc", lit(self.run_timestamp))
        )

        # self.report_t_utc_null_percentage(result_data)

        print("true_v2_good_data_merged_df schema:")
        result_data.printSchema()

        result_data_dyf = DynamicFrame.fromDF(
            result_data, self.glueContext, "result_data_dyf"
        )
        return result_data_dyf

    def write_data(self, processed_data: DynamicFrame) -> None:
        print("Writing data...")
        self.glueContext.write_dynamic_frame.from_options(
            frame=processed_data,
            connection_type="s3",
            connection_options={
                "path": "s3://benfeifke-temp-query-results/filter_merge_glue_job_partition_overwrite_test/",
                # "path": f"s3://benfeifke-temp-query-results/{self.run_timestamp}_test_filter_merge_glue_job_result_data/",
                "partitionKeys": ["year", "month", "day", "hour"],
            },
            format="parquet",
        )
        print("Finished writing data.")

    def report_t_utc_null_percentage(self, result_data):
        try:
            fraction_of_t_utc_values_that_are_null = (
                result_data.select(
                    col("t_utc").isNull().cast("float").alias("fraction_nulls")
                )
                .agg({"fraction_nulls": "avg"})
                .collect()[0][0]
            )
            print(
                f"Fraction of t_utc values in final true_v2_good_data_merged_df result that are null: {fraction_of_t_utc_values_that_are_null}"
            )
        except (KeyError, ValueError, NameError, AttributeError) as e:
            print(f"Error measuring share of bad `t_utc` values: {e}.")

    def get_sensor_target_schema_conversion_mapping(
        self, valid_data_sensor_reading_timestamp_level
    ):
        try:
            current_schema = dict(valid_data_sensor_reading_timestamp_level.dtypes)
            sensor_target_schema_conversion_mapping = {
                field: {
                    "current_dtype_is_struct": current_schema[field].startswith(
                        "struct"
                    ),
                    "desired_dtype": target_dtype,
                }
                for field, target_dtype in filter_merge_glue_job_utils.TARGET_SENSOR_SCHEMA.items()
                if field in current_schema.keys()
            }
            print("sensor_target_schema_conversion_mapping:")
            print(sensor_target_schema_conversion_mapping)
        except (KeyError, ValueError, NameError, AttributeError) as e:
            print(
                f"Something went wrong with the creation of the schema conversion mapping: {e}"
            )

        return sensor_target_schema_conversion_mapping

    def validate_and_report_sensor_reading_timestamp_dtype(
        self, valid_data_sensor_reading_timestamp_level
    ):
        print(
            "Now that conversion to tabular format is done, check sensor_reading_timestamp dtype before proceeding:"
        )
        sensor_reading_timestamp_dtype = (
            valid_data_sensor_reading_timestamp_level.select(
                "sensor_reading_timestamp"
            ).dtypes[0][1]
        )
        sensor_reading_timestamp_dtype_is_good = (
            sensor_reading_timestamp_dtype == "double"
        )
        if not sensor_reading_timestamp_dtype_is_good:
            print(f"Problem with t_utc dtype: {sensor_reading_timestamp_dtype}")
            print("The problem is in these files:")
            files_with_bad_t_utc_dtype = (
                valid_data_sensor_reading_timestamp_level.filter(
                    col("sensor_reading_timestamp.double").isNull()
                )
                .select("input_file_name")
                .distinct()
                .collect()
            )
            for file_with_bad_t_utc_dtype in files_with_bad_t_utc_dtype:
                print(file_with_bad_t_utc_dtype)
        else:
            print("No problem with t_utc dtype.")
        return sensor_reading_timestamp_dtype_is_good


if __name__ == "__main__":
    job = FilterMergeGlueJob()
    job.execute()
