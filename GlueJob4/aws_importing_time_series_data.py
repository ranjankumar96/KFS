# AWS_importing_time_series_data - ASHLAND
#
# Python Version: 3.6.10
#
# Input : 
#        1) dataset_group_name
#        2) target_dataset_name
#        3) dataset_frequency
#        4) target_schema
#        5) related_dataset_name
#        6) related_schema
#        7) target_dataset_import_job_name
#        8) target_dataset_arn
#        9) target_s3_data_path
#        10) role_arn
#        11) timestamp_format
#        12) related_dataset_import_job_name
#        13) related_dataset_arn
#        14) related_s3_data_path
#        15) item meta dataset name
#        16) item meta data set import job name
#        17) item meta dataset_arn
#        18) item s3_data_path
#
# Output : Returns 0 if function blocks execution is successful. 
#           Else, error message is passed
#
# Description : Importing the training time series data to Amazon Forecast 
#               for model building
#
# Coding Steps :
#               1. Setup
#               2. Creating the Dataset Group, Schema and Dataset
#               3. Create Data Import Job
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
# Approximate time to execute the code: Depends on the volume of the data.
# On average, it takes 10-15 mins.

# Loading libraries

from time import sleep
# Library for creating session to AWS forecast platfrom
from boto3 import Session 
from warnings import simplefilter
import notebook_utils as util
from error_logging import create_and_insert_error
simplefilter("ignore")


# Creating the Dataset Group, Dataset and Import Job

# Create the Dataset Group


def create_dataset_group_function(region_name, service_name, run_time_stamp,
                         dataset_group_name):

    """Creating Dataset group
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                
    argument4 (dataset_group_name): Name of the dataset group
    Returns: Nothing
    """
    try:    
        # Session set up

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
        
        # Using forecast api to call create dataset group method
        create_dataset_group_response = forecast.create_dataset_group(
            DatasetGroupName=dataset_group_name, Domain="CUSTOM"
        )
        create_dataset_group_response["DatasetGroupArn"]
        return 0
    except Exception as create_dataset_group_exception:
        print ("Exception caught in the create_dataset_group_function.\n",
                                       str(create_dataset_group_exception))                        
        create_and_insert_error(run_time_stamp)
        return str(create_dataset_group_exception)


# Create the Dataset


def create_dataset_function(
    region_name, 
    service_name,
    run_time_stamp,
    target_dataset_name,
    dataset_frequency,
    target_schema,
    dataset_group_arn,
    related_dataset_name=None,
    related_schema=None,
    item_meta_dataset_name=None,
    item_schema=None
):

    """Creating Dataset
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                        
    argument4 (target_dataset_name): Name of the target dataset
    argument5 (dataset_frequency):  Data frequency H: Hourly; M: Monthly
    argument6 (target_schema): target schema definition (json syntax)
    argument6 (target_schema): target schema definition (json syntax)
    argument7 (related_dataset_name[optional]): Name of the related dataset
    argument8 (related_schema[optional]): related schema definition 
                                                            (json syntax)
    argument7 (item meta datset name[optional]): Name of the item dataset
    argument8 (item_schema[optional]): item schema definition (json syntax)

    Returns: Nothing
    """
    try:
        # Session set up

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call following methods..
    
        # creating target dataset
        response_target = forecast.create_dataset(
            Domain="CUSTOM",
            DatasetType="TARGET_TIME_SERIES",
            DatasetName=target_dataset_name,
            DataFrequency=dataset_frequency,
            Schema=target_schema,
        )

        # Creating related dataset
        if (related_dataset_name is not None) & (related_schema is not None):
            response_related = forecast.create_dataset(
                Domain="CUSTOM",
                DatasetType="RELATED_TIME_SERIES",
                DatasetName=related_dataset_name,
                DataFrequency=dataset_frequency,
                Schema=related_schema,
            )

        # Creating item meta dataset
        if (item_meta_dataset_name is not None) & (item_schema is not None):
            response_item = forecast.create_dataset(
                Domain="CUSTOM",
                DatasetType="ITEM_METADATA",
                DatasetName=item_meta_dataset_name,
                DataFrequency=dataset_frequency,
                Schema=item_schema,
            )
            
        # Add Dataset to Dataset Group
        target_dataset_arn = response_target["DatasetArn"]
        if (item_meta_dataset_name is not None) & (item_schema is not None) & \
            (related_dataset_name is not None) & \
        (related_schema is not None):
            related_dataset_arn = response_related["DatasetArn"]
            item_meta_dataset_arn = response_item["DatasetArn"]
            forecast.update_dataset_group(
                DatasetGroupArn=dataset_group_arn,
                DatasetArns=[target_dataset_arn, item_meta_dataset_arn, \
                             related_dataset_arn],
            )
        elif (related_dataset_name is not None) & (related_schema is not None):
            related_dataset_arn = response_related["DatasetArn"]
            forecast.update_dataset_group(
                DatasetGroupArn=dataset_group_arn,
                DatasetArns=[target_dataset_arn, related_dataset_arn],
            )
            
        else:
            forecast.update_dataset_group(
                DatasetGroupArn=dataset_group_arn, DatasetArns=\
                    [target_dataset_arn]
            )

        return 0
    except Exception as create_dataset_exception:
        print ("Exception caught in the create_dataset_function.\n",
                                       str(create_dataset_exception))                                
        create_and_insert_error(run_time_stamp)
        return str(create_dataset_exception)


# Create Data Import Job


def import_aws_dataset(
    region_name, 
    service_name,
    run_time_stamp,
    target_dataset_import_job_name,
    target_dataset_arn,
    target_s3_data_path,
    role_arn,
    timestamp_format,
    related_dataset_import_job_name=None,
    related_dataset_arn=None,
    related_s3_data_path=None,
    item_meta_dataset_import_job_name=None,
    item_meta_dataset_arn=None,
    item_s3_data_path=None  
):

    """Importing data to AWS
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                
    argument4 (target_dataset_import_job_name): Name of the target dataset 
                                                import job
    argument5 (target_dataset_arn):  AWS resource name (unique identifier)
    argument6 (target_s3_data_path): S3 location where target input file is 
                                    present
    argument7 (role_arn): Role name for communincating between S3 and AWS 
                            forecast
    argument8 (timestamp_format): related schema definition (json syntax)
    argument9 (related_dataset_import_job_name[optinal]): Name of the related 
                                                        dataset import job
    argument10 (related_dataset_arn[optinal]):  AWS resource name 
                                                        (unique identifier)
    argument11 (related_s3_data_path[optinal]): S3 location where related input
                                                file is present
    argument12 (item_meta_dataset_import_job_name [optional]): Name of the 
                                                    target dataset import job                                                
    argument13 (item_meta_dataset_arn[optinal]):  AWS resource name 
                                                        (unique identifier)
    argument14 (item_s3_data_path[optinal]): S3 location where related input
                                                file is present

    Returns: Nothing
    """
    try:
        # Session set up

        session = Session(region_name=region_name)
        forecast = session.client(service_name=service_name)
    
        # Using forecast api to call following methods..
    
        # Target dataset import Job
        target_ds_import_job_response = forecast.create_dataset_import_job(
            DatasetImportJobName=target_dataset_import_job_name,
            DatasetArn=target_dataset_arn,
            DataSource={"S3Config": {"Path": target_s3_data_path, "RoleArn": \
                                                     role_arn}},
            TimestampFormat=timestamp_format,
        )
        # Related dataset import Job
        if (
            (related_dataset_import_job_name is not None)
            & (related_dataset_arn is not None)
            & (related_s3_data_path is not None)
        ):
            related_ds_import_job_response =forecast.create_dataset_import_job(
                DatasetImportJobName=related_dataset_import_job_name,
                DatasetArn=related_dataset_arn,
                DataSource={
                    "S3Config": {"Path": related_s3_data_path, "RoleArn": \
                                 role_arn}
                },
                TimestampFormat=timestamp_format,
            )
           
        # Item meta dataset import Job
        if (
            (item_meta_dataset_import_job_name is not None)
            & (item_meta_dataset_arn is not None)
            & (item_s3_data_path is not None)
        ):
            item_ds_import_job_response = forecast.create_dataset_import_job(
                DatasetImportJobName=item_meta_dataset_import_job_name,
                DatasetArn=item_meta_dataset_arn,
                DataSource={
                    "S3Config": {"Path": item_s3_data_path, "RoleArn":role_arn}
                },
                TimestampFormat=timestamp_format,
            )
            
            
        # Retrieving arns of target, item and related import jobs
        target_ds_import_job_arn = target_ds_import_job_response\
            ["DatasetImportJobArn"]
        
        if (
            (item_meta_dataset_import_job_name is not None)
            & (item_meta_dataset_arn is not None)
            & (item_s3_data_path is not None)
            
        ):
            item_ds_import_job_arn = item_ds_import_job_response[
                "DatasetImportJobArn"
            ]

        if (
            (related_dataset_import_job_name is not None)
            & (related_dataset_arn is not None)
            & (related_s3_data_path is not None)
        ):
            related_ds_import_job_arn = related_ds_import_job_response[
                "DatasetImportJobArn"
            ]
            
        # Status check for target import Job
        status_indicator_target = util.StatusIndicator()

        while True:
            status = forecast.describe_dataset_import_job(
                DatasetImportJobArn=target_ds_import_job_arn
            )["Status"]
            status_indicator_target.update(status)
            if status in ("ACTIVE", "CREATE_FAILED"):
                break
            sleep(10)

        # Status check for item import Job
        if (
            (item_meta_dataset_import_job_name is not None)
            & (item_meta_dataset_arn is not None)
            & (item_s3_data_path is not None)
        ):
            status_indicator_item = util.StatusIndicator()

            while True:
                status = forecast.describe_dataset_import_job(
                    DatasetImportJobArn=item_ds_import_job_arn
                )["Status"]
                status_indicator_item.update(status)
                if status in ("ACTIVE", "CREATE_FAILED"):
                    break
                sleep(10)

            status_indicator_item.end()

        # Status check for related import Job
        if (
            (related_dataset_import_job_name is not None)
            & (related_dataset_arn is not None)
            & (related_s3_data_path is not None)
        ):
            status_indicator_related = util.StatusIndicator()

            while True:
                status = forecast.describe_dataset_import_job(
                    DatasetImportJobArn=related_ds_import_job_arn
                )["Status"]
                status_indicator_related.update(status)
                if status in ("ACTIVE", "CREATE_FAILED"):
                    break
                sleep(10)

            status_indicator_related.end()
            
        return 0
    except Exception as import_aws_dataset_exception:
        print ("Exception caught in the import_aws_dataset function.\n",
                                       str(import_aws_dataset_exception))                                        
        create_and_insert_error(run_time_stamp)
        return str(import_aws_dataset_exception)
