import math
from typing import Union

from awsglue.dynamicframe import DynamicFrame
from pyspark import RDD
from pyspark.sql import DataFrame

GlueJobDataObjectType = Union[RDD, DataFrame, DynamicFrame]

TARGET_SENSOR_SCHEMA = {
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


def has_invalid_schema(dynamic_record) -> str:
    """Returns a `string` describing the failure reason if invalid; if the schema is valid, returns empty string."""

    try:
        # Metadata that is always present must be checked.
        if not isinstance(dynamic_record["id_api_key"], str):
            return "id_api_key bad dtype"
        if not isinstance(dynamic_record["body"]["metadata"]["id_phone"], str):
            return "id_phone bad dtype"
        if not isinstance(dynamic_record["body"]["metadata"]["version_sw"], str):
            return "version_sw bad dtype"

        # Not all metadata fields were present in all versions of the SDK;
        # when they are present, its datatype must be checked; when they are present, the file can pass.
        if "type_device" in dynamic_record["body"]["metadata"]:
            if not isinstance(dynamic_record["body"]["metadata"]["type_device"], str):
                return "type_device bad dtype"
        if "version_android" in dynamic_record["body"]["metadata"]:
            if not isinstance(
                dynamic_record["body"]["metadata"]["version_android"], str
            ):
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
                    if (
                        int(math.log10(int(element))) + 1
                    ) != 10:  # confirm that timestamps are valid format
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
                            if (subelement is not None) and (
                                type(subelement) not in {int, float, str}
                            ):
                                return "values bad dtype"
                    else:
                        return "values bad dtype"

        return ""
    except (KeyError, TypeError, ValueError, NameError) as e:
        return e


def add_has_invalid_schema_column(dynamic_record):
    dynamic_record["has_invalid_schema"] = has_invalid_schema(dynamic_record)
    return dynamic_record
