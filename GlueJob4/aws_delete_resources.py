# AWS delete resources
#
# Python Version: 3.6.10
#
# Input : AWS arn's that are used to identify the resource
#         1) region_name
#         2) service_name
#         3) run_time_stamp
#         4) Forecast export Job
#         5) Forecast
#         6) Predictor
#         7) Dataset import Job
#         8) Dataset
#         9) Dataset Group
#
# Output : returns 0 on successfully deletion of the resource.
#           Else, error message is passed.
#
# Description :  Cleaning up AWS resources to free the quota limits
#
# Deletion Sequence :
#               1. Forecast export Job
#               2. Forecast
#               3. Predictor
#               4. Dataset import Job
#               5. Dataset
#               6. Dataset Group
#
# Created By: Buddha Swaroop
#
#
# Modified Date: 19-FEB-2021
#
# Reviewed By: Sushanth Nalinaksh
#
# Reviewed Date: 22-FEB-2021
#
# List of called programs: None
#
# Approximate time to execute each function : 5 mins

# Loading libraries

from warnings import simplefilter
# Library for creating session to AWS forecast platfrom
from boto3 import Session 
from fcst_utils import wait_till_delete
from error_logging import create_and_insert_error
simplefilter("ignore")



# Delete forecast export job


def delete_forecast_export_job(region_name, service_name, run_time_stamp, 
                               forecast_export_job_arn):
    """Deletion of forecast export job
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id        
    argument4 (forecast_export_job_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete forecast export method
        
        wait_till_delete(
            lambda: forecast.delete_forecast_export_job(
                ForecastExportJobArn=forecast_export_job_arn
            )
        )
        return 0
    except Exception as delete_forecast_export_exception:
        print ("Exception caught in the delete_forecast_export_job function\n",
                                       str(delete_forecast_export_exception))        
        create_and_insert_error(run_time_stamp)
        return str(delete_forecast_export_exception)


# Delete forecast


def delete_forecast(region_name, service_name, run_time_stamp, forecast_arn):
    """Deletion of forecast
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                
    argument4 (forecast_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete forecast method
    
        wait_till_delete(lambda: forecast.delete_forecast(ForecastArn=\
                                                          forecast_arn))
        return 0
    except Exception as delete_forecast_exception:
        print ("Exception caught in the delete_forecast function\n",
                                       str(delete_forecast_exception))                
        create_and_insert_error(run_time_stamp)
        return str(delete_forecast_exception)


# Delete predictor


def delete_predictor(region_name, service_name, run_time_stamp, predictor_arn):
    """Deletion of predictor
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                        
    argument4 (predictor_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete predictor method
    
        wait_till_delete(lambda: forecast.delete_predictor(PredictorArn=\
                                                           predictor_arn))
        return 0
    except Exception as delete_predictor_exception:
        print ("Exception caught in the delete_predictor function\n",
                                       str(delete_predictor_exception))                        
        create_and_insert_error(run_time_stamp)
        return str(delete_predictor_exception)


# Delete dataset import job


def delete_dataset_import_job(region_name, service_name, run_time_stamp,
                              dataset_import_job_arn):
    """Deletion of dataset import job
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                
    argument4 (dataset_import_job_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete dataset import method
    
        wait_till_delete(
            lambda: forecast.delete_dataset_import_job(
                DatasetImportJobArn=dataset_import_job_arn
            )
        )
        return 0
    except Exception as delete_dataset_import_job_exception:
        print ("Exception caught in the delete_dataset_import_job function\n",
                                     str(delete_dataset_import_job_exception))        
        create_and_insert_error(run_time_stamp)
        return str(delete_dataset_import_job_exception)


# Delete dataset


def delete_dataset(region_name, service_name, run_time_stamp, dataset_arn):
    """Deletion of dataset
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                        
    argument4 (dataset_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete dataset  method
    
        wait_till_delete(lambda: forecast.delete_dataset(DatasetArn=\
                                                         dataset_arn))
        return 0
    except Exception as delete_dataset_exception:
        print ("Exception caught in the delete_dataset function\n",
                                     str(delete_dataset_exception))                
        create_and_insert_error(run_time_stamp)
        return str(delete_dataset_exception)


# Delete dataset group


def delete_dataset_group(region_name, service_name, run_time_stamp,
                         dataset_group_arn):
    """Deletion of dataset group
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                                
    argument4 (dataset_group_arn): AWS resource name (unique identifier)
    Returns: Nothing
    """
    try:
        # Session setup

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call delete dataset dataset group method
    
        wait_till_delete(
            lambda: forecast.delete_dataset_group(DatasetGroupArn=\
                                                  dataset_group_arn)
        )
        return 0
    except Exception as delete_dataset_group_exception:
        print ("Exception caught in the delete_dataset_group function\n",
                                     str(delete_dataset_group_exception))                        
        create_and_insert_error(run_time_stamp)
        return str(delete_dataset_group_exception)
