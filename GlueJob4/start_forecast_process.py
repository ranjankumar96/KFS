# CODE HAS BEEN CHANGED FOR OBTAINING ALL THE 3 PERCENTILE (P30, P40, P50) FORECASTS

# AWS Forecasting - ASHLAND
#
# Python Version: 3.6.10
#
# Input : algorithm name as a string and forecast_configuration.py file,  
#           run_time_stamp and AWS_Target,AWS_Related,
#            AWS_Item_metadata csv files.
#
# Output : forecast output is loaded to s3 and enterprise schema
#
# Description : Executes entire AWS forecasting pipeline
#
# Coding Steps :
#               1. Dataset group creation
#               2. Dataset creation
#               3. Import Dataset
#               4. Build Predictor
#               5. Create Forecast
#               6. Export Forecast
#               7. Delete Forecast
#               8. Delete Dataset Import
#               9. Calculate Mape
#               10. Write to S3 and Database table 
#
#
# List of called programs: aws_importing_time_series_data, 
#                           aws_creating_predictor, aws_creating_forecast, 
#                           aws_exporting_the_forecast, aws_delete_resources, 
#                           creating_mape_for_selected_sku
#
# Approximate time to execute the code: 210 - 500 mins approx.

# Loading libraries

# loading functions of `aws_importing_time_series_data.py` python script
from aws_importing_time_series_data import create_dataset_group_function, \
    create_dataset_function, import_aws_dataset
# loading functions of `aws_creating_predictor.py` python script
from aws_creating_predictor import create_predictor_function
# loading functions of `aws_creating_forecast.py` python script
from aws_creating_forecast import create_forecast_function
# loading functions of `aws_exporting_the_forecast.py` python script
from aws_exporting_the_forecast import create_forecast_export
# loading functions of `aws_delete_resources.py` python script
from aws_delete_resources import  delete_dataset_import_job, delete_forecast, \
delete_forecast_export_job
from create_target_ts import create_target_ts_data 
# Loading functions `create_related_ts.py` python script
from create_related_ts import create_related_ts_data 

# forecast configuration file
import processed_configuration as con 
from error_logging import create_and_insert_error 
from snowflake_db_connection import connect_to_db

import sys
from time import sleep
from boto3 import client, resource
from pandas import DataFrame, read_csv, to_datetime, concat, merge
from numpy import where
from creating_mape_for_selected_sku import calculating_mape
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

# Added
from creating_mape_for_selected_sku_p30 import calculating_mape_p30
from creating_mape_for_selected_sku_p40 import calculating_mape_p40
# Added

from math import ceil
import awswrangler as wr

# Dataset group creation..

def create_dataset_group(region_name, service_name,run_time_stamp,
                         project, algorithm_name):
    
    """Creating Dataset group
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id  
    argument4 (project): project name as a string
    argument5 (algorithm_name): algorithm short name as a string
    Returns: 0 on successful creation of the dataset group. Else, error.
    """
    
    try:        
        # Preparing the AWS arn (dataset group creation) to pass onto AWS \
        # forecast api's create dataset group method
        dataset_group = project.replace('-','_') + '_' + algorithm_name + \
                                            '_' + con.dataset_group_name
        flag = create_dataset_group_function(region_name, service_name,
                                             run_time_stamp,
                                             dataset_group)
        return str(flag)
    except Exception as create_dataset_group_exception:
        print("Exception occured in the create dataset group calling function.\
                The details are as follows.\n",\
                    str(create_dataset_group_exception))
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)
        return (str(create_dataset_group_exception))

# Dataset creation..    

def create_dataset(region_name, service_name,run_time_stamp,
                    algorithm_name, project, target_dataset_name, 
                   related_dataset_name,
                   item_meta_dataset_name=None):
    
    """Creating Dataset 
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                                
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (project): project name as a string
    argument6 (target_dataset_name): target dataset  name as a string
    argument7 (related_dataset_name): related dataset name as a string
    argument8 (item_meta_dataset_name[OPTIONAL]): item meta dataset name-string
    argument9 (run_time_stamp): current job's run id as a string
    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
    
    try:            
        # Preparing the AWS arn (dataset creation) to pass onto AWS forecast \
        # api's create dataset method
        dataset_group_arn = con.aws_common_arn_prefix + 'dataset-group/' + \
                            project.replace('-','_') + '_' + algorithm_name + \
                                '_' + con.dataset_group_name
        
        # Creating datasets for target, related and item meta data
        if (item_meta_dataset_name is not None) & (related_dataset_name !="") \
                                            & (con.related_schema !=""):
            
            target_dataset = project.replace('-','_') + '_' + algorithm_name +\
                                '_' + target_dataset_name
            related_dataset = project.replace('-','_') + '_' + algorithm_name \
                                + '_' + related_dataset_name  
            item_dataset = project.replace('-','_') + '_' + algorithm_name +\
                                '_' + item_meta_dataset_name
            
            flag = create_dataset_function(region_name, service_name,
                                           run_time_stamp,
                                        target_dataset, con.dataset_frequency, 
                                  con.target_schema,dataset_group_arn, 
                                  related_dataset, con.related_schema,
                                  item_dataset,con.item_schema)
        
        # Creating datasets for target and related
        elif (related_dataset_name !="") & (con.related_schema !=""):
            
            target_dataset = project.replace('-','_') + '_' + algorithm_name +\
                                '_' + target_dataset_name
            related_dataset = project.replace('-','_') + '_' + algorithm_name\
                                + '_' + related_dataset_name  
            flag = create_dataset_function(region_name, service_name,
                                           run_time_stamp,
                                         target_dataset, con.dataset_frequency, 
                                  con.target_schema,dataset_group_arn,
                                  related_dataset, con.related_schema)
        # Creating datasets for target    
        else:
            
            target_dataset = project.replace('-','_') + '_' + algorithm_name \
                                + '_' + target_dataset_name
            
            flag = create_dataset_function(region_name, service_name,
                                           run_time_stamp,
                                        target_dataset, con.dataset_frequency, 
                                  con.target_schema, dataset_group_arn)
                    
        return str(flag)
    except Exception as create_dataset_exception:
        print(" Exception occured in the create dataset calling function.\
                The details are as follows.\n",str(create_dataset_exception))    
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)  
        return (str(create_dataset_exception))

def import_dataset(region_name, service_name,run_time_stamp,
                   algorithm_name, project,aws_common_arn_prefix,
                   target_dataset_name, target_dataset_import_job_name,
                   target_file_name,processed_folder_name, bucket_name,
                   timestamp_format, 
                   related_dataset_name = None,
                   related_file_name = None, 
                   related_dataset_import_job_name = None,
                   item_metadataset_name = None,
                   item_metadata_file_name = None,
                   item_meta_dataset_import_job_name = None):

    """Creating Dataset 
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                        
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (project): project name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (target_dataset_name): target dataset  name as a string
    argument8 (target_dataset_import_job_name): target dataset import job name\
                                                            as a string
    argument9 (target_file_name): target data file name as a string
    argument10 (processed_folder_name): analytical output csvs folder in S3
    argument11 (bucket_name): S3 bucket name
    argument12 (timestamp_format): timestamp format of the data points
    argument13 (related_dataset_name [OPTIONAL]): related dataset name as a \
                                                                    string
    argument14 (related_file_name [OPTIONAL]): related data file name as a \
                                                                    string
    argument15 (related_dataset_import_job_name [OPTIONAL]): related dataset \
                                            import job name as a string
    argument16 (item_meta_dataset_name [OPTIONAL]): item meta dataset name as \
                                                                a string
    argument17 (item_metadata_file_name [OPTIONAL]): item meta data file name \
                                                                as a string
    argument18 (item_meta_dataset_import_job_name [OPTIONAL]): item meta \
                                            dataset import job name as a string
    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
        
    try:
            
        # Preparing the AWS arn (importing dataset) to pass onto AWS forecast \
        # api's importing dataset method
        target_dataset_import_job = project.replace('-','_') + '_' + \
                                    target_dataset_import_job_name
        if related_dataset_import_job_name != None : 
            related_dataset_import_job = project.replace('-','_') + '_' + \
                                    related_dataset_import_job_name
        if item_meta_dataset_import_job_name != None : 
            item_meta_dataset_import_job = project.replace('-','_') + '_' + \
                                    item_meta_dataset_import_job_name
                
        if related_dataset_name != None : 
            related_dataset_arn = aws_common_arn_prefix + 'dataset/' + \
                                    project.replace('-','_') + '_' \
                                + algorithm_name + '_' + related_dataset_name 
        if item_metadataset_name != None : 
            item_meta_dataset_arn = aws_common_arn_prefix + 'dataset/' + \
                                    project.replace('-','_') + '_' \
                                + algorithm_name + '_' + item_metadataset_name 
            
        target_dataset_arn = aws_common_arn_prefix + 'dataset/' + \
                        project.replace('-','_') + '_' + algorithm_name + '_' \
                               + target_dataset_name
#         target_s3_data_path = 's3://' + bucket_name + '/' + \
#                                 processed_folder_name+ '/' + project + '_' + \
#                                 algorithm_name + '_'+ target_file_name
        target_s3_data_path = 's3://' + bucket_name + '/' + 'GlueScripts4/'+ \
                                processed_folder_name+ '/' + project + '_' + \
                                algorithm_name + '_'+ target_file_name
    
#     s3://nvs3dvdf01.dfa.data/GlueScripts4/
    
#         if related_file_name != None: related_s3_data_path = 's3://' + \
#                             bucket_name + '/' + processed_folder_name+ '/' \
#                     + project + '_' + algorithm_name + '_'+related_file_name
        if related_file_name != None: related_s3_data_path = 's3://' + \
                            bucket_name + '/' + 'GlueScripts4/' + processed_folder_name+ '/' \
                    + project + '_' + algorithm_name + '_'+related_file_name
        if item_metadata_file_name != None: 
#             item_s3_data_path = 's3://' + bucket_name + '/' + \
#                         con.source_folder+ '/' + item_metadata_file_name
            item_s3_data_path = 's3://' + bucket_name + '/' + 'GlueScripts4/' + \
                        con.source_folder+ '/' + item_metadata_file_name
        
        # Creating importing jobs for target, related and item meta data
        if (related_dataset_import_job_name != None) and \
                            (item_meta_dataset_import_job_name != None):
            flag = import_aws_dataset(region_name, service_name,run_time_stamp,
                                        target_dataset_import_job, 
                                      target_dataset_arn, target_s3_data_path,
                                      con.role_arn, timestamp_format, 
                                      related_dataset_import_job, 
                                    related_dataset_arn, related_s3_data_path, 
                                    item_meta_dataset_import_job, 
                                    item_meta_dataset_arn, item_s3_data_path)
        
        # Creating importing jobs for target, related data
        elif (related_dataset_import_job_name != None) :            
            flag = import_aws_dataset(region_name, service_name,run_time_stamp,
                                      target_dataset_import_job, 
                                      target_dataset_arn, target_s3_data_path,
                                      con.role_arn, timestamp_format, 
                                      related_dataset_import_job, 
                                     related_dataset_arn, related_s3_data_path)
        
        # Creating importing job for target
        else:
            flag = import_aws_dataset(region_name, service_name,run_time_stamp,
                                      target_dataset_import_job, 
                                      target_dataset_arn, target_s3_data_path,
                                      con.role_arn, timestamp_format)
            # If dataset import job is successfull, flag holds value 0 else 
            # unsuccessful.
        print("Dataset importing function completed")
        return (str(flag))
        
    except Exception as import_aws_dataset_exception:
        print(" Exception occured in the Dataset importing calling function.\
              The details are as follows.\n",str(import_aws_dataset_exception))    
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)
        return (str(import_aws_dataset_exception))

def create_predictor(region_name, service_name,run_time_stamp,
                     hpo_flag,algorithm_name,aws_common_arn_prefix,
                     project,
                     dataset_group_name,
                     predictor_name,
                     forecast_horizon,
                     forecast_frequency,
                     short_name_of_the_algorithm,
                     number_of_back_test_windows,
                     back_test_window_offset):

    """Creating predictor 
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id        
    argument4 (hpo_flag): flag variable to enable /disable hpo option - boolean
    argument5 (algorithm_name): algorithm short name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (project): project name as a string
    argument8 (dataset_group_name): dataset group name as a string
    argument9 (predictor_name): predictor name as a string
    argument10 (forecast_horizon): Future forecast period  as a integer
    argument11 (forecast_frequency): forecast frequency (months/day) as string
    argument12 (short_name_of_the_algorithm): algorithm short name 
                                            (AWS forecast method) as a string
    argument13 (number_of_back_test_windows): number of backtest windows as a 
                                                                    integer
    argument14 (back_test_window_offset): back test windows ( slicing window 
                                            (test data period) ) as a integer

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
    
    
    try:            
        # Preparing the AWS arn to pass onto AWS forecast api's create 
        # predictor method
        dataset_group_arn = aws_common_arn_prefix + 'dataset-group/' + \
                            project.replace('-','_') + '_' + algorithm_name +\
                            '_' + dataset_group_name
        predictor_model_name = project.replace('-','_') + '_' + predictor_name

        # If Auto ml is true, then all forecast algorithm models are created 
        # and the model with best mape is selected.
        if con.Auto_ml==True:
            flag = create_predictor_function(region_name, service_name,
                                             run_time_stamp,
                                    hpo_flag,predictor_model_name, 
                                    forecast_horizon, dataset_group_arn, 
                                    forecast_frequency)
            
        # If Auto ml is false, then specific algorithm model is created
        else:
            algorithm_arn = 'arn:aws:forecast:::algorithm/'+\
                            short_name_of_the_algorithm
            flag = create_predictor_function(region_name, service_name,
                                             run_time_stamp,
                                             hpo_flag,predictor_model_name, 
                                    forecast_horizon, dataset_group_arn, 
                                    forecast_frequency, algorithm_arn, 
                                    number_of_back_test_windows, 
                                    back_test_window_offset)

        # If model building is successfull, flag holds value 0 else 
        # unsuccessful.
        print("predictor building completed")
        return (str(flag))
    except Exception as create_predictor_exception:
        print(" Exception occured in the Model Building calling function.\
                The details are as follows.\n",str(create_predictor_exception))        
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)            
        print("predictor building error "+str(create_predictor_exception))
        return (str(create_predictor_exception))

                   
def create_forecast(region_name, service_name, run_time_stamp,
        algorithm_name,aws_common_arn_prefix,project,
                    predictor_name,forecast_name,forecast_types):  #Added forecast_types

    """Creating forecast 
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id        
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument6 (project): project name as a string
    argument7 (predictor_name): predictor name as a string
    argument8 (forecast_name): predictor name as a string

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
            
    try:
        # Preparing the AWS arn to pass onto AWS forecast api's create forecast
        # method
        predictor_arn = aws_common_arn_prefix + 'predictor/' +  \
                        project.replace('-','_')  + '_' + predictor_name
        forecast_model_name = project.replace('-','_') + '_' + forecast_name
        forecast_types=["0.3", "0.4", "0.5"]  #Added forecast_types
        
        # Calling the AWS forecast create forecast method
        flag = create_forecast_function(region_name, service_name, 
                                        run_time_stamp,
            forecast_model_name, predictor_arn,forecast_types)  #Added forecast_types
        
        # If forecast building is successfull, flag holds value 0 else 
        # unsuccessful.   
        print("forecast creation completed")
        return (str(flag))    
    except Exception as create_forecast_exception:
        print(" Exception occured in the Forecast creation calling function.\
                The details are as follows.\n",str(create_forecast_exception))        
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                
        return (str(create_forecast_exception))        

def forecast_export(region_name, service_name, run_time_stamp,
        algorithm_name,role_arn,aws_common_arn_prefix,project,
                    bucket_name,forecast_name,
                    forecast_export_name,forecast_output_folder_name):

    """Creating forecast export
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (role_arn): AWS forecast role name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (project): project name as a string
    argument8 (bucket_name): S3 bucket name as a string
    argument9 (forecast_name): predictor name as a string
    argument10 (forecast_export_name): forecast export name as a string
    argument11 (forecast_output_folder_name): S3 folder location where the 
                    forecast output csv files are generated - string

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
            
    
    try:
        # Preparing the AWS arn to pass onto AWS forecast api's create 
        # predictor method
        forecast_arn = aws_common_arn_prefix + 'forecast/' + \
                        project.replace('-','_')  + '_' + forecast_name
        forecast_export_job_name = project.replace('-','_') + '_' + \
                                    forecast_export_name
#         directory = forecast_output_folder_name + '/'+ \
#                     project.replace('-','_') + '_' + algorithm_name + '/'
        directory = 'GlueScripts4/' + forecast_output_folder_name + '/'+ \
                    project.replace('-','_') + '_' + algorithm_name + '/'
                
        s3 = client('s3')
        s3.put_object(Bucket=bucket_name, Key=(directory + \
                                               forecast_export_name+'/'))
        
        s3_data_path = 's3://' + bucket_name + '/' + directory + \
                        forecast_export_name
        
        
        # Calling AWS forecast create forecast export method
        flag = create_forecast_export(region_name, service_name,
                                      run_time_stamp,
                                      forecast_arn,forecast_export_job_name,
                                      s3_data_path,role_arn)
        
        # If the exporting is successful, flag holds value 0 else unsuccessful.
        print("forecast export completed")
        return (str(flag))
    except Exception as forecast_export_exception: 
        print(" Exception occured in the forecast export calling function.\
                The details are as follows.\n",str(forecast_export_exception))        
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                    
        return (str(forecast_export_exception))

def target_append(run_time_stamp,algorithm_name,project,bucket_name,
                      forecast_output_folder_name,
                    forecast_export_name,processed_folder_name,
                    target_file_name,
                    aws_skun):
    
    """Creating Bdays adjustment / appending target data with current forecast 
                                                            data points
    argument1 (run_time_stamp): current job's run id                                                             
    argument2 (algorithm_name): algorithm short name as a string
    argument3 (project): project name as a string
    argument4 (bucket_name): S3 bucket name as a string
    argument5 (forecast_output_folder_name): S3 folder location where the 
                       forecast output csv files are generated - string
    argument6 (forecast_export_name): forecast export name as a string
    argument7 (processed_folder_name): Analytical ouptut folder in S3 as string
    argument8 (target_file_name): target data file name as a string
    argument9 (aws_skun): input data item id's(PF/TOS) as a dataframe
    
    Returns: 0 on successful creation of the dataset(s). Else, error.
    """

    
    try:
    
        # Reading the forecast op files
        s3 = resource("s3")
        s3_bucket = s3.Bucket(bucket_name)
#         directory = forecast_output_folder_name +'/'+ project.replace('-','_')\
#                     + '_' + algorithm_name +'/' + forecast_export_name +'/'
        directory = 'GlueScripts4/'+ forecast_output_folder_name +'/'+ project.replace('-','_')\
                    + '_' + algorithm_name +'/' + forecast_export_name +'/'
        
        files_in_s3 = [f.key.split(directory)[1] \
                    for f in s3_bucket.objects.filter(Prefix=directory).all()]
        csv_file_names = [csv for csv in files_in_s3 if csv.endswith('csv')]
        updated_target_df = DataFrame(columns=['item_id', 'date', 'p50'])

        # mape input directory creation
        s3 = client('s3')
#         mape_input_directory = forecast_output_folder_name + '/'+ \
#                         project.replace('-','_') + '_' + algorithm_name +'/'
        mape_input_directory = 'GlueScripts4/'+ forecast_output_folder_name + '/'+ \
                        project.replace('-','_') + '_' + algorithm_name +'/'
        
        try: 
            s3.put_object(Bucket=bucket_name, \
                Key=(mape_input_directory + 'mape_input/'))
        except Exception as mape_input_directory_exception: 
            print ("Exception occured at mape input directory creation.\n",
                   str(mape_input_directory_exception))

        # Iterating over each forecast csv file and creating corresponding
        # business days adjusted csv file
        for file_name in csv_file_names:
            forecast_file_name = 's3://' + bucket_name + '/' + directory + \
                                                                file_name
            fcst_file_name = 's3://' + bucket_name + '/' + \
                mape_input_directory + 'mape_input/' + \
                file_name.split('.')[0]+'.csv'
            try: 
                #forecast_df = read_csv(forecast_file_name)
                forecast_df = wr.s3.read_csv(forecast_file_name)
            except Exception as mape_input_function_csv_file_read_exception: 
                if 'No columns to parse from file' in \
                        str(mape_input_function_csv_file_read_exception): 
                    print("there's forecast ouput file which is empty")
                    continue
                else: 
                    print ("Exception ocured at mape input csv file reading\n",
                           str(mape_input_function_csv_file_read_exception))
                    create_and_insert_error(run_time_stamp)
                    
            # the below line is commented to avoid business days adjustment
            #forecast_df.to_csv(fcst_file_name,index=False)
            wr.s3.to_csv(df=forecast_df, path=fcst_file_name, index=False)
            # Appedning to original target data
            forecast_df['p50'] = forecast_df['p50'].round().astype(int)
            updated_target_df = updated_target_df.append(\
                                forecast_df[['item_id', 'date', 'p50']])
            
        updated_target_df['p50'] = where(\
                                updated_target_df['p50'].astype(int) < 0, 0, 
                                updated_target_df['p50']).astype(int)    
        updated_target_df['date'] = updated_target_df['date'].str[:10]
        
        # Ensuring the appended item ids are matching with the font in the 
        # original
        input_skus ={}
        for item in aws_skun['item_id'].unique(): input_skus[item]=item.lower()
        input_item_ids=list(input_skus.keys())
        input_item_ids_lowercase=list(input_skus.values())
        def input_item_id_case_convert(x): return input_item_ids\
            [input_item_ids_lowercase.index(x)]
        updated_target_df['item_id'] = updated_target_df['item_id'].apply(\
                                                input_item_id_case_convert)
        updated_target_df['item_id'] = updated_target_df['item_id'].astype(str)
        
        # rearranging columns
        updated_target_df = updated_target_df[['date','p50','item_id']] 
        updated_target_df = updated_target_df.rename(\
                        columns = {'p50':'target_value','date':'timestamp'})
        
#         initial_target_file = 's3://' + bucket_name + '/' + \
#             processed_folder_name + '/' + project + '_' + \
#                 algorithm_name + '_'+ target_file_name
        initial_target_file = 's3://' + bucket_name + '/' + 'GlueScripts4/'+ \
            processed_folder_name + '/' + project + '_' + \
                algorithm_name + '_'+ target_file_name
        
        #target_data = read_csv(initial_target_file)
        target_data = wr.s3.read_csv(initial_target_file)
        final_data = target_data.append(updated_target_df)
        final_data["timestamp"] = to_datetime(final_data['timestamp'], \
                                                 format = "%Y-%m-%d")
        final_data = final_data.sort_values(by=['timestamp',"item_id"])
        #final_data.to_csv(initial_target_file,index=False)
        wr.s3.to_csv(df=final_data, path=initial_target_file, index=False)
        print("target_append completed")
        return 0
    except Exception as target_append_exception:
        print(" Exception occured in the target_append function.\
                The details are as follows.\n",str(target_append_exception))        
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                            
        return str(target_append_exception)

def delete_forecast_export_job_calling_function(region_name, service_name, 
                                                run_time_stamp,
        algorithm_name, project, aws_common_arn_prefix,
                               forecast_name,forecast_export_name):

    """Deleting forecast export
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (project): project name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (forecast_name): predictor name as a string
    argument8 (forecast_export_name): forecast export name as a string

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
    
    try:
        # Preparing the AWS arn to pass onto AWS forecast api's Delete forecast
        # export job method
        forecast_export_job_arn = aws_common_arn_prefix+'forecast-export-job/'\
            + project  + '_' + forecast_name + '/' + forecast_export_name
        flag = delete_forecast_export_job(region_name, service_name, 
                                          run_time_stamp,
                                          forecast_export_job_arn)
        # If the deletion is successful, flag holds value 0 else unsuccessful.
        print("delete forecast export completed")
        return (str(flag))
    except Exception as delete_forecast_export_job_calling_function_exception: 
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                                
        print(" Exception occured in the \
              delete_forecast_export_job_calling_function.\
                The details are as follows.\n",
                str(delete_forecast_export_job_calling_function_exception))        
        return (str(delete_forecast_export_job_calling_function_exception))

def delete_forecast_calling_function(region_name, service_name, run_time_stamp,
        algorithm_name, project, aws_common_arn_prefix,
                    forecast_name):

    """Deleting forecast 
    Parameters:
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                        
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (project): project name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (forecast_name): predictor name as a string

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
    try:
        # Preparing the AWS arn to pass onto AWS forecast api's Delete forecast
        # method    
        forecast_arn = aws_common_arn_prefix + 'forecast/' + project  + '_' +\
                        forecast_name
        flag = delete_forecast(region_name, service_name, run_time_stamp,
                               forecast_arn)
        # If the deletion is successful, flag holds value 0 else unsuccessful.
        print("delete forecast completed")
        return str(flag)
    except Exception as delete_forecast_calling_function_exception: 
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                                
        print(" Exception occured in the delete_forecast_calling_function.\
                The details are as follows.\n",
                str(delete_forecast_calling_function_exception))        
        return (str(delete_forecast_calling_function_exception))

def delete_dataset_import_job_calling_function(region_name, service_name, 
                                       run_time_stamp,
                                        algorithm_name, project, 
                                       aws_common_arn_prefix, 
                        target_dataset_name, target_dataset_import_job_name,
        related_dataset_name = None, related_dataset_import_job_name = None):

    """Deleting dataset import job 
    Parameters: 
    argument1 (region_name): Amazon region name (service availability)
    argument2 (service_name): Type of amazon service (forecast)
    argument3 (run_time_stamp): current job's run id                                
    argument4 (algorithm_name): algorithm short name as a string
    argument5 (project): project name as a string
    argument6 (aws_common_arn_prefix): Generic AWS arn prefix as a string
    argument7 (target_dataset_name): target dataset name as a string
    argument8 (target_dataset_import_job_name): target dataset name as a string
    argument9 (related_dataset_name [OPTIONAL): target dataset name as a string
    argument10 (related_dataset_import_job_name [OPTIONAL]): target datasetname
                                                                as a string

    Returns: 0 on successful creation of the dataset(s). Else, error.
    """
    
    try:
        # Preparing the AWS arn to pass onto AWS forecast api's Delete dataset 
        # import method        
        target_dataset_import_job_arn = aws_common_arn_prefix + \
            'dataset-import-job/' + project  + '_' + algorithm_name +\
                                        '_' + target_dataset_name + '/' + \
                            project  + '_' + target_dataset_import_job_name
        
        # Deleting both related and target dataset import jobs
        if related_dataset_name != None and \
                                related_dataset_import_job_name != None:
            related_dataset_import_job_arn = aws_common_arn_prefix + \
                'dataset-import-job/' + project  + '_' + algorithm_name + \
            '_' + related_dataset_name + '/' + project  + '_' + \
                related_dataset_import_job_name
            target_flag = delete_dataset_import_job(region_name, service_name,
                                                    run_time_stamp,
                                    target_dataset_import_job_arn)
            related_flag = delete_dataset_import_job(region_name, service_name,
                                                     run_time_stamp,
                                        related_dataset_import_job_arn)
            print ("delete dataset import -target and related completed")
            return (target_flag, related_flag)
            
        # Deleting only target dataset import job    
        # If the deletion is successful, flag holds value 0 else unsuccessful.        
        target_flag = delete_dataset_import_job(region_name, service_name,
                                                run_time_stamp,
            target_dataset_import_job_arn)
        print("delete dataset import - target completed")
        return str(target_flag)
    except Exception as delete_dataset_import_job_calling_function_exception: 
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                                    
        print(" Exception occured in the \
              delete_dataset_import_job_calling_function.\
                The details are as follows.\n",
                str(delete_dataset_import_job_calling_function_exception))        
        return (str(delete_dataset_import_job_calling_function_exception))
    
def forecast_process(run_data):

    """Starting AWS forecast process
    Parameters:
    argument1 (run_data): 
            1) algorithm short name as substring - string
            2) run(project) name as a string
            3) run_timestamp as string
    Returns: NA
    """

    print("***",run_data)
    run_time_stamp = run_data[2]
    project = run_data[1]
    alg = run_data[0]
    
    # Calling dataset group and dataset functions
    try:
        print("Start forecasting process on algorithm :"+str(alg)+" \
                        !!!!!!!!!!!!!!")
        predictor_name = alg
        short_name_of_the_algorithm = (alg.upper()).replace('_','-')
        if short_name_of_the_algorithm =='PROPHET': 
            short_name_of_the_algorithm = 'Prophet'
        if short_name_of_the_algorithm =='DEEP-AR-PLUS': 
            short_name_of_the_algorithm = 'Deep_AR_Plus'    

        target_file_name = con.target_file_name
        related_file_name = con.related_file_name
        item_file_name = con.item_file_name
        
        # Reading Analytical output csv files as input 
        try:
#             target_file = 's3://' + con.bucket_name + '/' + \
#                         con.source_folder + '/' + target_file_name
            target_file = 's3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                        con.source_folder + '/' + target_file_name
#             related_file = 's3://' + con.bucket_name + '/' + \
#                         con.source_folder + '/' + related_file_name
            related_file = 's3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                        con.source_folder + '/' + related_file_name
            if con.item_metadataset_name !="": 
                item_meta_dataset_import_job_name = 'item_meta_data_import'
            #aws_skun = read_csv(target_file)
            #aws_rel_skus = read_csv(related_file)
            aws_skun = wr.s3.read_csv(target_file)
            aws_rel_skus = wr.s3.read_csv(related_file)
            aws_skun['item_id'] = aws_skun['item_id'].str.strip()
            aws_skun['item_id'] = aws_skun['item_id'].astype(str)
            aws_rel_skus['item_id'] = aws_rel_skus['item_id'].str.strip()
            aws_rel_skus['item_id'] = aws_rel_skus['item_id'].astype(str)
        except Exception as forecast_process_reading_analytical_csvs_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                    
            print("ERROR while reading processed input csv files \
                         @@@@@@@@@@@@@@@@@@@\n",
                    str(forecast_process_reading_analytical_csvs_exception))
            #sys.exit(1)
            raise

        # Vaildating if the current model is deep_ar_plus and the data points
        # are not sufficient, we are aborting the process as we cannot move
        # forward to train.
        try:
            if short_name_of_the_algorithm == 'Deep_AR_Plus':
                assert(len(aws_skun['item_id']) >=300 and \
                       len(aws_rel_skus['item_id']) >=300),\
                    "The DeepAR+ model needs minimum 300 data points to train."
        except Exception as forecast_process_deep_ar_criteria_exception:
            print("The DeepAR+ model needs minimum 300 data points to train.",
                  str(forecast_process_deep_ar_criteria_exception))
            create_and_insert_error( run_time_stamp)
            raise
            #sys.exit(1)

        # Dataset Group and Dataset creations
        
        print ("Dataset group creation #######################")
        dgc_flag = create_dataset_group(con.region_name, con.service_name,
                                        run_time_stamp,project,alg)

        if dgc_flag!=str(0): 
            print("Dataset group creation ERROR @@@@@@@@@@@@@@@@@@@\n"+\
                         str(dgc_flag))
            
            raise Exception('error :'+str(dgc_flag))
        else:
            print ("Dataset creation  #######################")
            if ('cnn_qr' in alg or 'deep_ar_plus' in alg) and (\
                                    con.item_metadataset_name !=""): 
                dc_flag = create_dataset(con.region_name, con.service_name,
                                         run_time_stamp,
                                         alg, project, 
                                         con.target_dataset_name, 
                            con.related_dataset_name,con.item_metadataset_name)
                
            elif 'cnn_qr' in alg or 'prophet' in alg or 'deep_ar_plus' in alg: 
                dc_flag = create_dataset(con.region_name, con.service_name,
                                         run_time_stamp,
                                           alg, project, 
                            con.target_dataset_name, con.related_dataset_name)
                
            else: dc_flag = create_dataset(con.region_name, con.service_name,
                                           run_time_stamp,
                                            alg, project, 
                        con.target_dataset_name, related_dataset_name="")
            
            if dc_flag!=str(0):
                print ("Dataset creation ERROR @@@@@@@@@@@@@@@@@@@\n"+\
                             str(dc_flag))
                print ('error :'+str(dc_flag))
                sys.exit(1)
        
    except Exception as forecast_process_before_for_loop_exception:
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                                    
        print ("ERROR before for loop @@@@@@@@@@@@@@@@@@@\n",
               str(forecast_process_before_for_loop_exception))
        raise
        
    
    no_of_rolling_forecasts = \
                ceil(con.total_forecast_period / con.forecast_horizon)
    
    # Iterate for long forecasting
    for i in range(1,no_of_rolling_forecasts+2):
        
        print ("Begin for loop #######################")
        print ('### ROUND '+str(i)+' ###')

        target_dataset_import_job_name = 'target_data_import'+'r'+str(i)
        related_dataset_import_job_name = 'related_data_import'+'r'+str(i)
        
        # Preparing the target and related data for 1st round of model building.
        if i==1: 
            print ("######### i=1")
            predictor_name = alg + 'r'+str(i)
            print ("predictor name -"+predictor_name)
            forecast_name = alg + '_fct'+'r'+str(i)
            forecast_export_name = alg + '_fct_exp'+'r'+str(i)
            
            try:
                
                target_ts_data = create_target_ts_data(aws_skun,\
                         con.forecast_horizon,i,run_time_stamp)
#                 wr.s3.to_csv(df=target_ts_data,
#                                  path='s3://' + con.bucket_name + \
#                                 '/' + con.processed_folder_name + '/' \
#                                  + project + '_' + alg + '_'+ \
#                                  target_file_name,
#                                  index=False)
                wr.s3.to_csv(df=target_ts_data,
                                 path='s3://' + con.bucket_name + \
                                '/' + 'GlueScripts4/' + con.processed_folder_name + '/' \
                                 + project + '_' + alg + '_'+ \
                                 target_file_name,
                                 index=False)
                
                #target_ts_data.to_csv('s3://' + con.bucket_name + \
                #            '/' + con.processed_folder_name + '/' \
                #             + project + '_' + alg + '_'+ \
                #             target_file_name,index=False)
                
                print ("target data preparation completed")
    
                if 'cnn_qr'in alg or 'prophet' in alg or 'deep_ar_plus' in alg:
                    related_ts_data = create_related_ts_data(aws_rel_skus,\
                         aws_skun, con.forecast_horizon,i,run_time_stamp)
                    print ("related data preparation completed")
                    
                    #related_ts_data.to_csv('s3://' + con.bucket_name + '/' \
                    #                    + con.processed_folder_name + '/' \
                    #                   + project + '_' + alg + '_'+ \
                    #                    related_file_name,index=False)
#                     wr.s3.to_csv(df=related_ts_data,
#                                      path='s3://' + con.bucket_name + '/' \
#                                             + con.processed_folder_name + '/' \
#                                             + project + '_' + alg + '_'+ \
#                                             related_file_name,
#                                      index=False)
                    wr.s3.to_csv(df=related_ts_data,
                                     path='s3://' + con.bucket_name + '/' + 'GlueScripts4/' \
                                            + con.processed_folder_name + '/' \
                                            + project + '_' + alg + '_'+ \
                                            related_file_name,
                                     index=False)
                    print ("target and related data written to files")
    
            except Exception as for_loop_it1_exception:
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Data preparation error in iteration 1 ",
                             str(for_loop_it1_exception))
                raise
                
        
        # Preparing target and related data for 2nd round of model building
        if i==2:
            print ("######### i=2")
            predictor_name = alg + 'r'+str(i)
            print ("predictor name -"+predictor_name)
            forecast_name = alg + '_fct'+'r'+str(i)
            forecast_export_name = alg + '_fct_exp'+'r'+str(i)
            
            try:
                
                target_ts_data = create_target_ts_data(aws_skun,
                             con.forecast_horizon,i,run_time_stamp)
#                 wr.s3.to_csv(df=target_ts_data,
#                                  path='s3://' + con.bucket_name + '/' + \
#                                         con.processed_folder_name + '/' \
#                                         + project + '_' + alg + '_'+ \
#                                         target_file_name,
#                                  index=False)
                wr.s3.to_csv(df=target_ts_data,
                                 path='s3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                                        con.processed_folder_name + '/' \
                                        + project + '_' + alg + '_'+ \
                                        target_file_name,
                                 index=False)
                
                #target_ts_data.to_csv('s3://' + con.bucket_name + '/' + \
                #                            con.processed_folder_name + '/' \
                #                            + project + '_' + alg + '_'+ \
                #                           target_file_name,index=False)
                print ("target data preparation completed")
                
                if 'cnn_qr' in alg or 'prophet' in alg \
                    or 'deep_ar_plus' in alg:
                    related_ts_data = create_related_ts_data(aws_rel_skus,\
                          aws_skun, con.forecast_horizon,i,run_time_stamp)
                    print ("related data preparation completed")
#                     wr.s3.to_csv(df=related_ts_data,
#                                      path='s3://' + con.bucket_name + '/' + \
#                                             con.processed_folder_name + '/' \
#                                             + project + '_' + alg + '_'+ \
#                                             related_file_name,
#                                      index=False)
                    wr.s3.to_csv(df=related_ts_data,
                                     path='s3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                                            con.processed_folder_name + '/' \
                                            + project + '_' + alg + '_'+ \
                                            related_file_name,
                                     index=False)
                  
                    #print (related_ts_data)
                    #related_ts_data.to_csv('s3://' + con.bucket_name + '/' + \
                    #                        con.processed_folder_name + '/' \
                    #                        + project + '_' + alg + '_'+ \
                    #                        related_file_name,index=False)
                    print ("target and related data written to files")
                    
            except Exception as for_loop_it2_exception:
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Data preparation error in iteration 2 ",
                       str(for_loop_it2_exception))
                raise
                
        
        # Prepares related data
        if i >= 3:
            try:
                print ("######### i= "+str(i))
                forecast_name = alg + '_fct'+'r'+str(i)
                forecast_export_name = alg + '_fct_exp'+'r'+str(i)
                
                if 'cnn_qr' in alg or 'prophet' in alg \
                    or 'deep_ar_plus' in alg:
                    related_ts_data = create_related_ts_data(aws_rel_skus,\
                            aws_skun,con.forecast_horizon,i,run_time_stamp)
                    print ("related data preparation completed")
                    #print ("##############",related_ts_data)
#                     wr.s3.to_csv(df=related_ts_data,
#                                      path='s3://' + con.bucket_name + '/' + \
#                                            con.processed_folder_name + '/' \
#                                            + project + '_' + alg + '_'+\
#                                            related_file_name,
#                                      index=False)
                    wr.s3.to_csv(df=related_ts_data,
                                     path='s3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                                           con.processed_folder_name + '/' \
                                           + project + '_' + alg + '_'+\
                                           related_file_name,
                                     index=False)
                    
                    #related_ts_data.to_csv('s3://' + con.bucket_name + '/' + \
                    #                       con.processed_folder_name + '/' \
                    #                         + project + '_' + alg + '_'+\
                    #                             related_file_name,index=False)
                    print ("related data written to file")
                
            except Exception as for_loop_it3_exception:
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("data preparation error ",
                             str(for_loop_it3_exception))
                raise
                
        
        # Calling AWS dataset import method
        try:
            print ("Importing #######################")
            if ('cnn_qr' in alg or 'deep_ar_plus' in alg) and \
                (con.item_metadataset_name !="") and (i==1) :
                flag = import_dataset(con.region_name, con.service_name,
                                      run_time_stamp,
                                alg, project,
                               con.aws_common_arn_prefix,
                               con.target_dataset_name,
                               target_dataset_import_job_name,
                               target_file_name,
                               con.processed_folder_name,
                               con.bucket_name,
                               con.timestamp_format,
                               con.related_dataset_name,
                               related_file_name,
                               related_dataset_import_job_name,
                               con.item_metadataset_name,
                               item_file_name,
                               item_meta_dataset_import_job_name
                               )
                
            elif 'cnn_qr' in alg or 'prophet' in alg or 'deep_ar_plus' in alg:
                flag = import_dataset(con.region_name, con.service_name,
                                      run_time_stamp,
                                      alg, project,
                               con.aws_common_arn_prefix,
                               con.target_dataset_name,
                               target_dataset_import_job_name,
                               target_file_name,
                               con.processed_folder_name,
                               con.bucket_name,
                               con.timestamp_format,
                               con.related_dataset_name,
                               related_file_name,
                               related_dataset_import_job_name
                               )
                
            else:
                flag = import_dataset(con.region_name, con.service_name,
                                      run_time_stamp,
                                      alg, project,
                               con.aws_common_arn_prefix,
                               con.target_dataset_name,
                               target_dataset_import_job_name,
                               target_file_name,
                               con.processed_folder_name,
                               con.bucket_name,
                               con.timestamp_format
                               )
                
            
            if flag!=str(0): 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Datset Import FLAG Error @@@@@@@@@@@@@@@@@@\
                             "+str(flag))
                raise
        except Exception as import_dataset_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Datset Import Error @@@@@@@@@@@@@@@@@@ ",
                   str(import_dataset_function_exception))
            raise

        # Calling AWS Create predictor method
        try:
            print ("Modelling #######################")
            if i < 3:
                if "npts" in alg or "ets" in alg or "arima" in alg or \
                    "prophet" in alg: hpo_flag = False
                else: hpo_flag = True
                
                model_flag = create_predictor(con.region_name,con.service_name,
                                              run_time_stamp,
                                              hpo_flag, alg, 
                                              con.aws_common_arn_prefix,
                                              project, 
                                              con.dataset_group_name,
                                              predictor_name,
                                              con.forecast_horizon,
                                              con.forecast_frequency,
                                              short_name_of_the_algorithm,
                                              con.number_of_back_test_windows,
                                              con.back_test_window_offset)

                print ("############",model_flag)
                if 'Status : IN_PROGRESS' in str(model_flag): 
                    print ("Modelling Error -- Status : IN_PROGRESS\
                                 @@@@@@@@@@@@@@@@@@ "+str(model_flag))
                    sleep(1200)
                    print ("Running Modelling again #######################")
                    
                    model_flag = create_predictor(con.region_name,
                                                con.service_name,
                                                run_time_stamp,
                                                hpo_flag, alg, 
                                                con.aws_common_arn_prefix, 
                                                project, 
                                                con.dataset_group_name,\
                                                predictor_name, 
                                                con.forecast_horizon, 
                                                con.forecast_frequency,\
                                             short_name_of_the_algorithm, 
                                             con.number_of_back_test_windows, 
                                             con.back_test_window_offset)
                print ("############",model_flag)    
                if model_flag!=str(0): 
                    # writing the exception/error to the error log table 
                    create_and_insert_error(run_time_stamp)                                
                    print ("Modelling FLAG Error @@@@@@@@@@@@@@@@@@ "+\
                                 str(model_flag))
                    raise
                
        except Exception as create_predictor_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Modelling Error @@@@@@@@@@@@@@@@@@ ",
                   str(create_predictor_function_exception))
            raise

            
        # Calling AWS Create forecast method
        try:
            print ("Forecasting #######################")
            
            forecast_types=["0.3", "0.4", "0.5"]  #Added forecast_types
            
            fct_flag = create_forecast(con.region_name,con.service_name,
                                       run_time_stamp,
                                       alg, con.aws_common_arn_prefix,
                                       project,
                                       predictor_name,
                                       forecast_name,forecast_types) #Added forecast_types
            print ("############",fct_flag)
            if 'Status : IN_PROGRESS' in str(fct_flag): 
                print ("Forecast Error -- Status : IN_PROGRESS @@@@@@@\
                             @@@@@@@@@@@ "+str(fct_flag))
                sleep(1200)
                print ("Running Forecasting again #######################")
                print ("Running Forecasting again ####################")
                
                forecast_types=["0.3", "0.4", "0.5"]  #Added forecast_types
                fct_flag = create_forecast(con.region_name,con.service_name,
                                           run_time_stamp,
                    alg, con.aws_common_arn_prefix,
                                project, predictor_name, forecast_name,forecast_types) #Added forecast_types
            print ("############",fct_flag)    
            if fct_flag!=str(0): 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Forecast FLAG Error @@@@@@@@@@@@@@@@ "+str(fct_flag))
                raise
            
        except Exception as create_forecast_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Forecasting Error @@@@@@@@@@@@@@@@@@ ",
                         str(create_forecast_function_exception))
            raise
        
        # Calling AWS Create forecast export method
        try:
            print ("Exporting #######################")
            
            export_flag = forecast_export(con.region_name,con.service_name,
                            run_time_stamp,              
                            alg,con.role_arn,
                            con.aws_common_arn_prefix,
                            project,
                            con.bucket_name,
                            forecast_name,
                            forecast_export_name,
                            con.forecast_output_folder_name)
            
            if export_flag!=str(0): 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Export FLAG Error @@@@@@@@@@@@@@@ "+str(export_flag))
                raise
            
        except Exception as create_forecast_export_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Exporting Error @@@@@@@@@@@@@@@@@@ ",
                         str(create_forecast_export_function_exception))
            raise
        
        # Calling Business days adjustment / append current forecast to target\
        # data for next round
        try:
            print ("target_append #######################")
            
            target_append_flag = target_append(run_time_stamp,alg,project,
                                           con.bucket_name,
                                           con.forecast_output_folder_name,
                                           forecast_export_name,
                                           con.processed_folder_name,
                                           target_file_name,
                                           aws_skun)
            print ("############",target_append_flag)
            if str(target_append_flag)!=str(0):
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("target_append_flag Error @@@@@@@@@@@@@@@@@@ ",
                       str(target_append_flag))
                raise
            
        except Exception as target_append_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("target_append Error @@@@@@@@@@@@@@@@@@ ",
                   str(target_append_function_exception))
            raise
        
        # Calling AWS Delete forecast export method
        try:
            print ("Delete Fct Export #######################")
            
            delete_fct_export_flag = \
                delete_forecast_export_job_calling_function(con.region_name, 
                                    con.service_name,run_time_stamp,alg, 
                                    project, con.aws_common_arn_prefix,
                                   forecast_name,
                                   forecast_export_name)
            print ("############",delete_fct_export_flag)
            if delete_fct_export_flag!=str(0): 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Delete Forecast Export FLAG Error @@@@@@@@@@@@@@@@ "+\
                             str(delete_fct_export_flag))
                raise
            
        except Exception as delete_forecast_export_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Delete Fct Export Error @@@@@@@@@@@@@@@@@@ ",
                   str(delete_forecast_export_function_exception))
            raise
        
        # Calling AWS Delete forecast method
        try:
            print ("Delete Fct #######################")
            
            delete_fct_flag = delete_forecast_calling_function(con.region_name,
                            con.service_name, run_time_stamp,alg, project,\
                                    con.aws_common_arn_prefix,forecast_name)
            print ("############",delete_fct_flag)
            if delete_fct_flag!=str(0): 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Delete Forecast FLAG Error @@@@@@@@@@@@@@@@@@ "+\
                             str(delete_fct_flag))
                raise
            
        except Exception as delete_forecast_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Delete Fct Error @@@@@@@@@@@@@@@@@@ ",
                   str(delete_forecast_function_exception))
            raise

        # Calling AWS Delete dataset import job method 
        try:
            print ("Delete Dataset Import #######################")
            
            if i > 2:
                if 'cnn_qr' in alg or 'prophet' in alg or 'deep_ar_plus' in alg:
                    delete_dataset_target_flag, delete_dataset_related_flag  =\
                            delete_dataset_import_job_calling_function(
                                    con.region_name, con.service_name,                        
                                    run_time_stamp, alg, project, \
                                       con.aws_common_arn_prefix, 
                                       con.target_dataset_name, 
                                       target_dataset_import_job_name, 
                                       con.related_dataset_name, 
                                       related_dataset_import_job_name)
                    print ("############",delete_dataset_target_flag, 
                           delete_dataset_related_flag)
                    if delete_dataset_target_flag!=str(0) or \
                                delete_dataset_related_flag!=str(0): 
                        print ("Delete dataset_flag Error@@@ "+\
                                     str(delete_dataset_target_flag)+"\n"+\
                                         str(delete_dataset_related_flag))
                        #sys.exit(1)
                else:
                    delete_dataset_target_flag  = \
                            delete_dataset_import_job_calling_function(
                                    con.region_name, con.service_name,                        
                                    run_time_stamp, alg, project, \
                                       con.aws_common_arn_prefix, 
                                       con.target_dataset_name, 
                                       target_dataset_import_job_name,)
                    print ("############",delete_dataset_target_flag)
                    if delete_dataset_target_flag!=str(0): 
                        print ("Delete dataset_flag Error @@@@@@ "+\
                                     str(delete_dataset_target_flag))
                
        except Exception as delete_dataset_import_function_exception:
            # writing the exception/error to the error log table 
            create_and_insert_error(run_time_stamp)                                
            print ("Delete dataset Error @@@@@@@@@@@@@@@@@@ ",
                         str(delete_dataset_import_function_exception))
            raise
    
    # MAPE calcualtion and database writing
    try:
        
        print ("MAPE OUTPUT AND DATABASE LOADING ##################")

        
        # Reading the forecast output from S3
        s3 = resource("s3")
        s3_bucket = s3.Bucket(con.bucket_name)
#         directory = con.forecast_output_folder_name + '/' + project + '_' \
#                             + alg + '/mape_input/'
        directory = 'GlueScripts4/' + con.forecast_output_folder_name + '/' + project + '_' \
                            + alg + '/mape_input/'
        
        files_in_s3 = [f.key.split(directory)[1] \
                    for f in s3_bucket.objects.filter(Prefix=directory).all()]
        
        csv_file_names = [csv for csv in files_in_s3 if csv.endswith('csv')]
        
        all_csv_data = DataFrame()
        for filename in csv_file_names:
            try:
                #df = read_csv('s3://'+con.bucket_name+'/'+ directory + \
                #              filename, index_col=None, header=0)
                df = wr.s3.read_csv('s3://'+con.bucket_name+'/'+directory + filename, index_col=None, header=0)
                all_csv_data = concat([all_csv_data, df], axis=0, \
                                      ignore_index=True)
            except Exception as mape_output_block_csv_read_exception : 
                # writing the exception/error to the error log table 
                create_and_insert_error(run_time_stamp)                                
                print ("Error at mape_output_block_csv_read_exception\n",
                       str(mape_output_block_csv_read_exception))
                raise
        all_csv_data_filter_cols_p50 = all_csv_data[['item_id', 'date','p50']]
        all_csv_data_filter_cols_p50["model"] =alg.upper()+"_P50"
        all_csv_data_filter_cols_p50.rename(columns={"p50":"Forecast"},inplace=True)
        
        all_csv_data_filter_cols_p40 = all_csv_data[['item_id', 'date','p40']]   #Added - Priyanka
        all_csv_data_filter_cols_p40["model"] =alg.upper()+"_P40"
        all_csv_data_filter_cols_p40.rename(columns={"p40":"Forecast"},inplace=True)
        
        all_csv_data_filter_cols_p30 = all_csv_data[['item_id', 'date','p30']]   #Added - Priyanka
        all_csv_data_filter_cols_p30["model"] =alg.upper()+"_P30"
        all_csv_data_filter_cols_p30.rename(columns={"p30":"Forecast"},inplace=True)
        
        
        all_csv_data_filter_cols=pd.concat([all_csv_data_filter_cols_p50,all_csv_data_filter_cols_p40,all_csv_data_filter_cols_p30])
        

        forecast_output_folder = 's3://'+con.bucket_name+'/'+ 'GlueScripts4/' + \
            con.forecast_output_folder_name + '/' \
            + project + '_' + alg + '/'+ alg + '_fct_expr1/'
            
#         file_key = f"{con.forecast_output_folder_name}/{project}_{alg}/{alg}_fct_expr1/"
        file_key = f"GlueScripts4/{con.forecast_output_folder_name}/{project}_{alg}/{alg}_fct_expr1/"
        
#         target_data_file = 's3://' + con.bucket_name + '/' + \
#             con.processed_folder_name + '/' + \
#                 project + '_' + alg + '_'+ con.target_file_name  
        target_data_file = 's3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
            con.processed_folder_name + '/' + \
                project + '_' + alg + '_'+ con.target_file_name
        
        #df_target = read_csv(target_data_file)
        df_target = wr.s3.read_csv(target_data_file)
        
        df_target_sku = df_target
        

        # Calculating the MAPE on test period
        #df_all_month = calculating_mape(forecast_output_folder,df_target_sku,\
        #                                con.forecast_horizon,alg.upper())
        df_all_month_p50 = calculating_mape(con.bucket_name, file_key, forecast_output_folder, df_target_sku,
                                        con.forecast_horizon, alg.upper())
#         Added - Priyanka
        df_all_month_p40 = calculating_mape_p40(con.bucket_name, file_key, forecast_output_folder, df_target_sku,
                                        con.forecast_horizon, alg.upper())
        df_all_month_p30 = calculating_mape_p30(con.bucket_name, file_key, forecast_output_folder, df_target_sku,
                                        con.forecast_horizon, alg.upper())
#         Added - Priyanka
        
        df_all_month=pd.concat([df_all_month_p50,df_all_month_p40,df_all_month_p30])
        
        df_test_mape = df_all_month[df_all_month.month == 'OVERALL']
        df_test_mape.drop(['month'], axis = 1,inplace = True)
        
        df_test_mape['item_id'] = df_test_mape['item_id'].str.lower()

        merged_df = merge(all_csv_data_filter_cols, df_test_mape, 
                          on=['item_id','model'], how='left')
        
        merged_df['date'] = to_datetime(merged_df['date'])
        merged_df['date'] = \
                to_datetime(merged_df['date'].dt.date).dt.strftime('%b-%Y')

        #  concat run_time_stamp with the forecast op.
        
        merged_df['RUN_TIMESTAMP'] = run_time_stamp
        # Making sure, the forecast op's item id values are just like 
        # original font.
        skus ={}
        for item in aws_skun['item_id'].unique(): skus[item]=item.lower()
        key_list=list(skus.keys())
        val_list=list(skus.values())
        def sku_case_convert(x): return key_list[val_list.index(x)]
        merged_df['item_id'] = merged_df['item_id'].apply(sku_case_convert)
                
        
        # rearranging columns
        merged_df = merged_df[['RUN_TIMESTAMP', 'item_id', 'date', 'model', \
                                           'Forecast','perc_error_MAPE','perc_error_MAD']] 

        merged_df = merged_df.rename(columns = {'Forecast':'FORECAST_VALUE',\
                                                'date':'MONTH_YEAR',\
                                                    'model':'FORECAST_METHOD',\
                                                'perc_error_MAPE':'TEST_MAPE', \
                                        'perc_error_MAD':'TEST_MAD', \
                                            'item_id':'ITEM_ID'})
                                            
 
        
        merged_df['FORECAST_VALUE'] = \
                            merged_df['FORECAST_VALUE'].round().astype(int)
        merged_df['FORECAST_VALUE'] = where(\
                            merged_df['FORECAST_VALUE'].astype(int) < 0, 0, \
                                merged_df['FORECAST_VALUE']).astype(int) 
        
                
#         final_output_directory = con.forecast_output_folder_name + \
#                                 '/' + project + '_' + \
#                                 alg + '/mape_output_or_database_table_load/'
        final_output_directory = 'GlueScripts4/' + con.forecast_output_folder_name + \
                                '/' + project + '_' + \
                                alg + '/mape_output_or_database_table_load/'
        
        # Creating the output directory in S3
        try:
            s3 = client('s3')
            s3.put_object(Bucket=con.bucket_name, Key=(final_output_directory))
            
        except Exception as mape_output_directory_creation_exception: 
            print ("Exception at mape_output_directory_creation_exception\n",
                   str(mape_output_directory_creation_exception))
            create_and_insert_error(run_time_stamp)
            
        # Storing the output as csv in S3
        #merged_df.to_csv('s3://' + con.bucket_name + '/' + \
        #                 final_output_directory + project + '_' + \
        #                     alg +'_output.csv', index = False) 
        wr.s3.to_csv(df=merged_df,
                     path=f"s3://{con.bucket_name}/{final_output_directory}{project}_{alg}_output.csv",
                     index=False)
                     
        #Added on 29th Nov'22 skip 'Nan'
        merged_df=merged_df[~(merged_df["TEST_MAPE"].isna())]
        print(merged_df.shape)
        
        
        merged_df.rename(columns={"RUN_TIMESTAMP":"RUN_TIME_STAMP"},inplace=True)
        print("Shape of merged_df dataset : ", merged_df.shape)


        
        #Static table name entered
        cur, conn = connect_to_db()
        try:
            write_pandas(conn, merged_df, 'KFS_ORDERS_BASELINE_FORECAST')
        except Exception as e:
            print(f'Error while writing data to table KFS_ORDERS_BASELINE_FORECAST : {e}')
        
        
        #Added 3Feb23 - starts
        #This part is for filtering data only for P50 results & storing it in a separate Snowflake Table
        df_p50 = merged_df[merged_df["FORECAST_METHOD"].str.contains('P50')]
        print("Shape of df_p50 dataset : ", df_p50.shape)
        
        cur, conn = connect_to_db()
        try:
            write_pandas(conn, df_p50, 'KFS_ORDERS_BASELINE_FORECAST2')
        except Exception as e:
            print(f'Error while writing data to table KFS_ORDERS_BASELINE_FORECAST2 : {e}')
        #Added 3Feb23 - ends
        

    except Exception as mape_output_and_database_loading_exception:
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)        
        print ("MAPE OUTPUT AND DATABASE LOADING Error @@@@@@@@@@@@@ ",
               str(mape_output_and_database_loading_exception))
        raise