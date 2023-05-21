# create_related_ts.py
#
# Python Version: 3.6.10
#
# Input : Related time series data uploaded to the S3 bucket.
#                1) related dataframe 
#                2) forecast horizon
#                3) iteration number
#                4) run_time_stamp
#
# Output : Dataframe 
#
# Description : Data preparation for each iteration
#
# Coding Steps :
#               1. read the related data and iteration number
#               2. Extract the data to be used for the current iteration
#
# Created By: Buddha Swaroop
#
# Created Date: 14-OCT-2020
#
# Modified Date: 19-FEB-2021
#
# Reviewed By: Sushanth Nalinaksh
#
# Reviewed Date: 22-FEB-2021
#
# List of called programs: None
#
# Approximate time to execute the code:  1 mins
#
# Loading libraries
from pandas import to_datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta
from error_logging import create_and_insert_error

# Create_related_ts_data..

def create_related_ts_data(aws_rel_skus,aws_skun,forecast_horizon,
                           iteration_number,run_time_stamp):
    
    """Create data slicing from input datafrmaes and  
    supply to the current forecasting iteration

    Parameters
    ----------
    aws_rel_skus : dataframe
        related data
    aws_skun : dataframe
        target data
    forecast_horizon : number
        number of future months forecast (each iteration)
    iteration_number : number
        forecast round number 
    run_time_stamp : string
        Timestamp based run id. for the job.

    How it works
    ------------
        1. Read the target data, related data, forecast horizon and 
            iteration number
        2. Filter the data from the target dataframe
        3. return to the forecast iteration

    """    
    
    try:
        # Data preparation from input dataframes for forecast round 1 
        if iteration_number == 1:
            aws_related_skus_filtered = aws_rel_skus 
            aws_related_skus_filtered_date = ((to_datetime( \
                aws_related_skus_filtered["timestamp"]) >= \
                                    min(aws_skun['timestamp'])) & 
                 (to_datetime(aws_related_skus_filtered["timestamp"]) \
                  <= max(aws_skun['timestamp'])))
            related_ts_data = \
                aws_related_skus_filtered[aws_related_skus_filtered_date]

            return related_ts_data
        
        # Data preparation from input dataframes for forecast round 2
        elif iteration_number == 2:
            aws_related_skus_filtered = aws_rel_skus 
            aws_related_skus_filtered_date = ((to_datetime(\
                                aws_related_skus_filtered["timestamp"]) \
                                          >= min(aws_skun['timestamp'])) & 
                     (to_datetime(aws_related_skus_filtered["timestamp"]) \
                      <= (datetime.strptime(max(aws_skun['timestamp']),\
                '%Y-%m-%d').date()+relativedelta(months=+forecast_horizon)\
                         ).strftime('%Y-%m-%d')))
            related_ts_data = \
                aws_related_skus_filtered[aws_related_skus_filtered_date]

            return related_ts_data
        
        # Data preparation from input dataframes for forecast round 3 till end.
        else:
            aws_related_skus_filtered = aws_rel_skus
            aws_related_skus_filtered_date = ((to_datetime(\
                                      aws_related_skus_filtered["timestamp"]) \
                                               >= min(aws_skun['timestamp'])) & 
                            (to_datetime(aws_related_skus_filtered["timestamp"]) \
                                                     <= (datetime.strptime(max(\
                                                           aws_skun['timestamp']),\
                             '%Y-%m-%d').date()+relativedelta(\
               months=+forecast_horizon*iteration_number-2)).strftime('%Y-%m-%d')))

            related_ts_data = aws_related_skus_filtered[aws_related_skus_filtered_date]

            return related_ts_data

    except Exception as create_related_ts_data_function_exception:
        print ("Exception in the create_related_ts_data_function.\n",
                                       str(create_related_ts_data_function_exception))
        create_and_insert_error(run_time_stamp)
        return str(create_related_ts_data_function_exception)
