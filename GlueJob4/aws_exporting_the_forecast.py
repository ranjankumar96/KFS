# Exporting the forecast
#
# Python Version: 3.6.10
#
# Input : 
#         1) region_name
#         2) service_name
#         3) run_time_stamp
#         4) forecast_arn
#         5) forecast_export_job_name
#         6) s3_data_path
#         7) role_arn
#
#
# Output : Returns 0 after Forecasted results are exported to s3 as csv files 
#           successfully. Else, error message is passed
#
# Description : Exporting the forecasted results of the predicted model
#
# Coding Steps :
#               1. Setup
#               2. Create forecast export
#
# Created Date: 16-SEP-2020
#
# Modified Date: 19-FEB-2021
#
# Reviewed By: Sushanth Nalinaksh
#
# Reviewed Date: 22-FEB-2021
#
# List of called programs: None
#
# Approximate time to execute the code: 3 mins

# Loading libraries
from warnings import simplefilter
from time import sleep
# Library for creating session to AWS forecast platfrom
from boto3 import Session 
import notebook_utils as util
from error_logging import create_and_insert_error
simplefilter("ignore")


# Create forecast export


def create_forecast_export(
    region_name, service_name, run_time_stamp,
    forecast_arn, forecast_export_job_name, s3_data_path, role_arn
):

    """Creating forecast export job
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id        
    argument4 (forecast_arn): AWS resource name (unique identifier)
    argument5 (forecast_export_job_name): Name of the dorecast export
    argument6 (s3_data_path): Location of the S3 where the exports reside
    argument7 (role_arn): Forecast role name for connecting AWS forecast to S3
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call following methods..
        create_forecast_export_response = forecast.create_forecast_export_job(
            ForecastExportJobName=forecast_export_job_name,
            ForecastArn=forecast_arn,
            Destination={"S3Config": {"Path": s3_data_path, "RoleArn": \
                                      role_arn}},
        )

        # Retrieving forecast export arn to check the status
        forecast_export_arn = create_forecast_export_response\
            ["ForecastExportJobArn"]
        status_indicator = util.StatusIndicator()

        while True:
            status = forecast.describe_forecast_export_job(
                ForecastExportJobArn=forecast_export_arn
            )["Status"]
            status_indicator.update(status)
            if status in ("ACTIVE", "CREATE_FAILED"):
                break
            sleep(10)

        status_indicator.end()

        return 0

    except Exception as create_forecast_export_exception:
        print ("Exception caught in the create_forecast_export function.\n",
                                       str(create_forecast_export_exception))                
        create_and_insert_error(run_time_stamp)
        return str(create_forecast_export_exception)
