# orders data cleaning and updating orders table
#
# Python version used: 3.8.12
#
# Coding Steps:
#               1. Update the orders table with the processed data
#
# Created By: Priyanka Srivastava
#
# Created Date: 07-Dec-2022

# 1. Import built-in packages and user defined functions

from numpy import where
from pandas import DataFrame
from pytz import timezone
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

import processed_configuration as con
from error_logging import create_and_insert_error
from snowflake_db_connection import connect_to_db

      
# 2. Transforming the processed orders data and updating the table

def transform_data_and_write_db( \
                                period,
                                kfs_order,
                                update_ind,
                                run_time_stamp,
                                cur
                               ):
    """Transforming the processed orders data and updating 
    the orders table.

    Parameters
    ----------
    period : string
        Mon-yyyy.
    kfs_order : dataframe
        Pre-processed orders data.
    update_ind : bool
        Indicator to decide whether to overwrite or 
        insert the table for a given Mon-yyyy.
    run_time_stamp : string
        Timestamp based run id. for the job.
    cur : object
        Snowflake DB cursor object.

    Returns
    -------
    temp
        Returns a dataframe copy of the tranformed data.

    How it works
    ------------
        1. Select only the columns that will be inserted to the table.
        2. Rename them accordingly. 
        3. Delete records for a Mon-yyy in the table if 
           new data for the same period has come.
        4. Insert the processed data in to the orders table.

    """    
    try:
        print("Inside... transform_data_and_write_db()")

        # Selecting the required columns
        temp = kfs_order.copy()
        temp['mmm_yyyy'] = period
        temp['run_time_stamp'] = run_time_stamp
        temp = temp[[\
                     'run_time_stamp',
                     'item_id',
                     'mmm_yyyy',
                     'target_value'
                     ]].copy()
        
        # Renaming the columns as in the orders table
        temp.rename(
                    columns={
                    'run_time_stamp':'RUN_TIME_STAMP',
                    'mmm_yyyy':'MONTH_YEAR',
                    'item_id':'ITEM_ID',
                    'target_value':'UNITS'},
                    inplace=True
                    )
        
        # Replacing spaces if any
        temp['MONTH_YEAR'].replace(" ", "")
        mon_yr = temp['MONTH_YEAR'][0] #Added
        print("mon_yr:", mon_yr)
        
        # Rounding off the UNITS
        temp['UNITS'] = round(temp['UNITS'])
                
        # Get the delete and insert queries from config file
        delete_qry = con.del_frm_kfs_order + " where MONTH_YEAR = '" + \
                     mon_yr + "'"
        insert_qry = con.insert_kfs_order

        # Delete the old records for the Mon-yyyy, if it exists
        if update_ind:
            print("update ind is true")
            cur.execute(delete_qry)

#         Writing all SKUs data into the intermediate table 
        
        cur, conn = connect_to_db()
        try:
            write_pandas(conn, temp, 'KFS_PROCESSED_ORDERS_TARGET')
        except Exception as e:
            print(f'Error while writing data to table KFS_PROCESSED_ORDERS_TARGET : {e}')

        print("Exiting... transform_data_and_write_db()")
        return temp
    except:
        print("Exception occurred inside transform_data_and_write_db()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise
    