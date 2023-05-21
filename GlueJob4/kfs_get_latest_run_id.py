# Run id. creation for the job - ASHLAND
#
# Python Version: 3.6.10
#
# Description : Get the latest run id.(RUN_TIME_STAMP) from 
#               the processed shipments table
#
# Coding Steps :
#               1. Get the connection string
#               2. Get the latest run id.(RUN_TIME_STAMP) from 
#                  processed shipments table
#               3. Return the run id
#

# List of called programs: None
#
# Approximate time to execute the code: 10 sec


# 1. Import packages and functions

from pandas import read_sql

import processed_configuration as con
from error_logging import create_and_insert_error


# 2. Get the latest run id or run timestamp

def get_latest_run_id(cur, conn):
    """Get the latest run timestamp from the processed AA shipments table.

    Parameters
    ----------
    cur : object
        Snowflake DB cursor object.
    conn : object
        Snowflake DB connection object.

    Returns
    -------
    string
        run id. (run_time_stamp)

    How it works
    ------------
        1. Get the connection string.
        2. Get the latest run id.(RUN_TIME_STAMP) 
        from processed shipments table.
        3. Return the run timestamp.

    """
    try:
        print('Inside... get_latest_run_id()')
        
        # Initializing run id. - run_time_stamp
        run_time_stamp = ""
        
        # Select all run timestamps from the processed shipments table
        df_all_run_ts = read_sql(con.sel_run_tmp_kfs_order, conn)
        list_all_run_ts = list(df_all_run_ts.RUN_TIME_STAMP.unique())

        # Sort run timestamps in desc order
        list_all_run_ts.sort(reverse=True)
        
        # Select the latest one
        run_time_stamp = list_all_run_ts[0]
        
        print('Exiting... get_latest_run_id()')
        
        #Return the latest id
        return run_time_stamp
    except:
        print("Exception occurred inside get_latest_run_id()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(run_time_stamp)
        raise    
    