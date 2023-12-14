import math
from typing import Union
from pyspark import RDD

from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, arrays_zip, col, expr, first, from_unixtime, year, month, dayofmonth, hour
from base_glue_job import BaseGlueJob

from awsglue.dynamicframe import DynamicFrame


target_sensor_schema = {
    "acc_x_sen": "double",
    "acc_y_sen": "double",
    "acc_z_sen": "double",
    "angvel_x_sen": "double",
    "angvel_y_sen": "double",
    "angvel_z_sen": "double",
    "mag_x_sen": "double",
    "mag_y_sen": "double",
    "mag_z_sen": "double",
    "log": "string",
    "confidence": "int",
    "pres_abs": "double",
    "ang_lat": "double",
    "accu_ang_bearing": "double",
    "vel": "double",
    "ang_bearing": "double",
    "accu_ver": "double",
    "accu_hor": "double",
    "accu_vel": "double",
    "alt": "double",
    "ang_lon": "double",
    "ang_lat_gnss": "double",
    "accu_ang_bearing_gnss": "double",
    "vel_gnss": "double",
    "ang_bearing_gnss": "double",
    "accu_ver_gnss": "double",
    "accu_hor_gnss": "double",
    "accu_vel_gnss": "double",
    "alt_gnss": "double",
    "ang_lon_gnss": "double",
    "customermetadata": "string",
    "status": "string",
    "svid": "int",
    "type_constellation": "int",
    "time_offset_ns": "double",
    "state": "int",
    "t_received_sv_ns": "double",
    "t_received_sv_unc_ns": "double",
    "cn0_db": "double",
    "baseband_cn0_db": "double",
    "pseudorange": "double",
    "unc_pseudorange": "double",
    "state_d_range_acc": "int",
    "d_range_acc": "double",
    "d_range_unc_acc": "double",
    "f_carrier": "double",
    "cycles_carrier": "double",
    "phase_carrier": "double",
    "unc_phase_carrier": "double",
    "indicator_multipath": "int",
    "snr_in_db": "double",
    "auto_gain_ctrl_level": "double",
    "type_code": "string",
    "bias_full_inter_signal_ns": "double",
    "unc_bias_full_inter_signal_ns": "double",
    "bias_sat_inter_signal_ns": "double",
    "unc_bias_sat_inter_signal_ns": "double",
    "bias_code_phase_ns": "double",
    "unc_bias_code_phase_ns": "double",
    "bias_carrier_phase_ns": "double",
    "unc_bias_carrier_phase_ns": "double",
    "multipath_indic": "int",
    "snr_in_db_gnss": "double",
    "auto_gain_ctrl_level_gnss": "double",
    "type_code_gnss": "int",
    "bias_full_inter_signal_ns_gnss": "double",
    "unc_bias_full_inter_signal_ns_gnss": "double",
    "bias_sat_inter_signal_ns_gnss": "double",
    "unc_bias_sat_inter_signal_ns_gnss": "double",
    "level_battery": "double",
    "status_battery": "string",
    "health_battery": "string",
    "count_step": "double",
    "status_connectivity": "string",
    "activity": "string",
    "transition": "string",
    "network_type": "string",
    "type_network": "string",
    "type_constellations": "int",
    "level": "int",
    "network": "string",
    "dbm": "int",
    "rssi": "int",
    "level_wifi": "int",
    "level_mobile": "int",
}

ALLOWABLE_SENSOR_VALUE_DTYPES = ["double", "int", "long", "string"]

def has_invalid_schema(dynamic_record):
    """Returns a `string` describing the failure reason if invalid, otherwise returns empty string."""
    
    # # When reading from s3 rather than from catalog, there are not explicit partitions, and this
    # # check on partitions must be excluded.
    # if not type(dynamic_record["partition_0"]) == str:
    #     return False
    # if not type(dynamic_record["partition_1"]) == str:
    #     return False
    # if not type(dynamic_record["partition_2"]) == str:
    #     return False
    # if not type(dynamic_record["partition_3"]) == str:
    #     return False
    # if not type(dynamic_record["partition_4"]) == str:
    #     return False
    
    # Metadata that is always present must be checked.
    if not type(dynamic_record["id_api_key"]) == str:
        return "id_api_key bad dtype"
    if not type(dynamic_record["body"]["metadata"]["id_phone"]) == str:
        return "id_phone bad dtype"
    if not type(dynamic_record["body"]["metadata"]["version_sw"]) == str:
        return "version_sw bad dtype"
    
    # Not all metadata fields were present in all versions of the SDK;
    # when they are present, its datatype must be checked; when they are present, the file can pass.
    if "type_device" in dynamic_record["body"]["metadata"]:
        if not type(dynamic_record["body"]["metadata"]["type_device"]) == str:
            return "type_device bad dtype"
    if "version_android" in dynamic_record["body"]["metadata"]:
        if not type(dynamic_record["body"]["metadata"]["version_android"]) == str:
            return "version_android bad dtype"
    if "sensors_available" in dynamic_record["body"]["metadata"]:
        field = dynamic_record["body"]["metadata"]["sensors_available"]
        if isinstance(field, list):
            for element in field:
                if not isinstance(element, str):
                    return "sensors_available bad dtype"
        else:
            return "sensors_available bad dtype"
    if "permissions_granted" in dynamic_record["body"]["metadata"]:
        field = dynamic_record["body"]["metadata"]["permissions_granted"]
        if isinstance(field, list):
            for element in field:
                if not isinstance(element, str):
                    return "permissions_granted bad dtype"
        else:
            return "permissions_granted bad dtype"
    
    # Check the schema of the data.
    data_packet_list = dynamic_record["body"]["data"]
    for data_packet in data_packet_list:
        
        field = data_packet["names"]
        if isinstance(field, list):
            for element in field:
                if not isinstance(element, str):
                    return "names bad dtype"
        else:
            return "names bad dtype"

        field = data_packet["t_utc"]
        if isinstance(field, list):
            for element in field:
                if not isinstance(element, float):
                    return "t_utc bad dtype"
                if (int(math.log10(int(element))) + 1) != 10:  # confirm that timestamps are valid format
                    return "t_utc bad dtype"
        else:
            return "t_utc bad dtype"

        field = data_packet["values"]
        if not isinstance(field, list):
            return "values bad dtype"
        else:
            for element in field:
                if isinstance(element, list):
                    for subelement in element:
                        # Data values can be null.
                        if (subelement is not None) and (type(subelement) not in {int, float, str}):
                            return "values bad dtype"
                else:
                    return "values bad dtype"

    return ""

def add_has_invalid_schema_column(dynamic_record):
    try:
        dynamic_record_is_valid_schema = self.has_invalid_schema(dynamic_record)
    except:
        dynamic_record_is_valid_schema = ""
    dynamic_record["has_invalid_schema"] = dynamic_record_is_valid_schema
    return dynamic_record

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
                        "s3://true-v2-input/data/2023/11/30/22/",
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
            .map(add_has_invalid_schema_column)
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
                for field, target_dtype in target_sensor_schema.items()
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
