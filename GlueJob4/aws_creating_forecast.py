# AWS_forecast
#
# Python Version: 3.6.10
#
# Input : 
#        1) region_name
#        2) service_name
#        3) run_time_stamp
#        4) forecast_name
#        5) predictor_arn
#
# Output : Model forecast
#
# Description : Generating the forecast on the trained model 
#
# Coding Steps :
#               1. Setup
#               2. Create Forecast
#
# Created By: Buddha Swaroop
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
# Approximate time to execute the code: 20 mins

# Loading libraries

from warnings import simplefilter
from time import sleep
# Library for creating session to AWS forecast platfrom
from boto3 import Session 
import notebook_utils as util
from error_logging import create_and_insert_error
simplefilter("ignore")


# Create Forecast

def create_forecast_function(region_name, service_name, run_time_stamp, 
                             forecast_name, predictor_arn, forecast_types):  

    """Creating forecast
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id
    argument4 (forecast_name): Name of the forecast
    argument5 (predictor_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:

        # Session set up

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call create forecast method
        
        create_forecast_response = forecast.create_forecast(
            ForecastName=forecast_name, PredictorArn=predictor_arn, ForecastTypes=forecast_types
        )
        forecast_arn = create_forecast_response["ForecastArn"]

        # Checking status
        status_indicator = util.StatusIndicator()

        while True:
            status = forecast.describe_forecast(ForecastArn=forecast_arn)\
                                                                ["Status"]
            status_indicator.update(status)
            if status in ("ACTIVE", "CREATE_FAILED"):
                break
            sleep(10)

        status_indicator.end()

        return 0

    except Exception as forecast_exception:
        print ("Exception caught in the create_forecast_function.\n",
                                       str(forecast_exception))        
        create_and_insert_error(run_time_stamp)
        return str(forecast_exception)
