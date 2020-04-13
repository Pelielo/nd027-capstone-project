from __future__ import absolute_import, division, print_function

import helpers
import operators
from airflow.plugins_manager import AirflowPlugin


# Defining the plugin class
class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.LoadS3,
        operators.DownloadAndUnzip,
        operators.CopyToRedshiftOperator,
        operators.DataQualityOperator
    ]
    # helpers = [
    #     helpers.SqlQueries
    # ]
