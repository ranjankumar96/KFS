# Champion_alteration.py
#
# Python Version: 3.6.10
#
# Description : changing champion rank based on the flat and 
#               declining trend in rolling foecast.
#
# Coding Steps :
#               1. Read the latest run id's shipments forecast base line table
#                    data
#               2. Examine the trend of the champion rank 1 for all PF / ITEM
#               3. Implement the Hybrid model based on the criteria match
#
# Created By: Ranjan.Kumar
#
# Created Date: 09-DEC-2022
#
#
# Reviewed By: 
#
# Reviewed Date: 
#
# Approximate time to execute the code: 2 mins

# Loading Libraries..
from pandas import DataFrame, to_datetime
import processed_configuration as con
from error_logging import create_and_insert_error
from snowflake_db_connection import connect_to_db
from kfs_get_latest_run_id import get_latest_run_id
from datetime import datetime
from pytz import timezone
import awswrangler as wr
import sys
from awsglue.utils import getResolvedOptions
import boto3
from datetime import datetime,date,timedelta

#Added on 05Feb23 - Ranjan - Successful
args = getResolvedOptions(sys.argv, ['REGION_NAME', 'SRC_FILE_PATH', 'SNS_TOPIC_ARN'])
region_name = args['REGION_NAME']
SRC_FILE_PATH = args['SRC_FILE_PATH']
sns_Topic_Arn = args['SNS_TOPIC_ARN']

DATE = datetime. now(). strftime("%Y%m%d_%H%M%S") 
#Added on 05Feb23 - Ranjan - Successful

# Champion alteration..

def champion_alteration():
    
    # Database connection validation..
    
    try:        
        # Get cursor and connection to DB
        cur, conn = connect_to_db()
        
        # Get the latest run_id from processed shipments table
        run_time_stamp = get_latest_run_id(cur, conn)
    
    except Exception as db_connection_exception:
        print ("Database connection error at champion_alteration function.\n", 
               str(db_connection_exception))
        import sys
        sys.exit(1)
    
        
    cur.execute(con.latest_run_time_stamp_data)
    result=cur.fetchall()
    
    
    df = DataFrame(result, columns=['RUN_TIME_STAMP', 'ITEM_ID', \
                                'MONTH_YEAR', 'FORECAST_METHOD', \
        'FORECAST_VALUE', 'TEST_MAPE','TEST_MAD', 'UPDATE_TIME_STAMP','CHAMP_RANK', 'LONG_FORECAST'])


    # 2. For all algorithms, identify if there is a flat forecast over last 15 
    #    months, 
    #   indicated by the value of the coefficient of variance 
    #   (standard deviation / mean) 
    #   being less than or equal to a configurable threshold 
    #   (flat_threshold = 0.01). 
    #   If there is a flat forecast, LONG_FORECAST column in baseline forecast 
    #   table is updated with a flag ‘F’.

    # 3. For all algorithms, identify if there is a declining forecast over 18
    #   months, 
    #   indicated by the ratio of the mean of last 6 months of forecasts and
    #   the mean of the first 3 months of the forecast. 
    #   If there is a declining forecast ratio (the ratio is less than a
    #   configurable value = 0.5), 
    #   LONG_FORECAST column in the baseline forecast table is updated with the 
    #   flag ‘D’. 

    items = df.ITEM_ID.unique()
    forecast_methods = df.FORECAST_METHOD.unique()
    
    # Reading input target data csv file to get low volume PF/ITEM 
    try:
        from pandas import read_csv
        target_file = 's3://' + con.bucket_name + '/' + 'GlueScripts4/' + \
                    con.source_folder + '/' + con.target_file_name
        aws_skun = wr.s3.read_csv(target_file)
        aws_skun['item_id'] = aws_skun['item_id'].str.strip()
        aws_skun['item_id'] = aws_skun['item_id'].astype(str)
        
        # Get all PF/Item whose average volume is <= configurable value
        target_data_volume = aws_skun.groupby('item_id')\
            ['target_value'].mean().round().astype(int)
        low_volume_item_ids = target_data_volume[\
                target_data_volume.le(con.filter_volume)].index   #need to look into this
        
    except Exception as target_csv_read_exception:
        # writing the exception/error to the error log table 
        create_and_insert_error(run_time_stamp)                                    
        print("ERROR while reading processed target csv file \
                     @@@@@@@@@@@@@@@@@@@\n",
                str(target_csv_read_exception))
        raise
    
    

    for fm in forecast_methods: 
        for pf in items:
            if pf in low_volume_item_ids: continue
            df_test_flag = df.loc[(df.FORECAST_METHOD == fm) & \
                                  (df.ITEM_ID == pf)]
            try: flat_flag = (round(df_test_flag.tail(9).FORECAST_VALUE.std()) \
                    / round(df_test_flag.tail(9).FORECAST_VALUE.mean())) <= 0.01
            except Exception as flat_flag_check_exception : 

                if 'division by zero' in str(flat_flag_check_exception):
                    flat_flag = True
                    Exception_msg = "WARNING : division by zero exception \
                        occured for "+ str(pf)+ "of forecast method "+ \
                                    str(fm)+ "while checking FLAT FORECAST."
                    print (Exception_msg)

                    # Get the current timestamp
                    tz = timezone('EST')
                    now_est = datetime.now(tz)
                    now_est = str(now_est)

                    # Create a temp dataframe for the warning message
                    temp_dict = {
                                 'RUN_TIME_STAMP':[run_time_stamp],
                                 'GLUE_JOB':[con.tar_glue_job],
                                 'EXCEPTION_RAISED_TIME':[now_est[:26]],
                                 'EXCEPTION_KEY':['NA'],
                                 'EXCEPTION_MESSAGE':['NA'],
                                 'MESSAGE':[Exception_msg]
                                }

                    df_warn = DataFrame(temp_dict)
                    warn_record = df_warn.values.tolist()

                    # Get the insert query
                    insert_qry = con.insert_error_log

                    # Insert the warning inside the error log table
                    if(cur != ""):
                        cur.executemany(insert_qry, warn_record)
                    else:
                        print("Cursor object is empty.")

                        create_and_insert_error(run_time_stamp)

                else:
                    Exception_msg = "Exception occured for "+ str(pf)+ "of \
                        forecast method "+ \
                        str(fm)+ "while checking FLAT FORECAST."
                    print (Exception_msg)

                    create_and_insert_error(run_time_stamp)
                    raise

            try: decline_flag = (round(df_test_flag.tail(6).FORECAST_VALUE.mean()) \
                            / round(df_test_flag.FORECAST_VALUE[3:6].mean())) < 0.5
            except Exception as decline_flag_check_exception : 

                if 'division by zero' in str(decline_flag_check_exception):
                    decline_flag = True
                    Exception_msg = "WARNING : division by zero exception \
                        occured for "+ str(pf)+ "of forecast method "+ \
                            str(fm)+ "while checking DECLINING FORECAST."
                    print (Exception_msg)

                    # Get the current timestamp
                    tz = timezone('EST')
                    now_est = datetime.now(tz)
                    now_est = str(now_est)

                    # Create a temp dataframe for the warning message
                    temp_dict = {
                                 'RUN_TIME_STAMP':[run_time_stamp],
                                 'GLUE_JOB':[con.tar_glue_job],
                                 'EXCEPTION_RAISED_TIME':[now_est[:26]],
                                 'EXCEPTION_KEY':['NA'],
                                 'EXCEPTION_MESSAGE':['NA'],
                                 'MESSAGE':[Exception_msg]
                                }

                    df_warn = DataFrame(temp_dict)
                    warn_record = df_warn.values.tolist()

                    # Get the insert query
                    insert_qry = con.insert_error_log

                    # Insert the warning inside the error log table
                    if(cur != ""):
                        cur.executemany(insert_qry, warn_record)
                    else:
                        print("Cursor object is empty.")

                        create_and_insert_error(run_time_stamp)

                else:
                    Exception_msg = "Exception occured for "+ str(pf)+ \
                        "of forecast method "+ \
                            str(fm)+ "while checking DECLINING FORECAST."
                    print (Exception_msg)

                    create_and_insert_error(run_time_stamp)
                    raise

            if flat_flag : df.loc[(df.FORECAST_METHOD == fm) & \
                        (df.ITEM_ID == pf), 'LONG_FORECAST'] = 'F'
            elif decline_flag : df.loc[(df.FORECAST_METHOD == fm) & \
                        (df.ITEM_ID == pf), 'LONG_FORECAST'] = 'D'
            else : pass


    # 4. If the champion rank 1 model, has either a flat forecast flag or a 
    #    declining forecast flag ( F or D) ,
    #    the best ranked model without a flag will be identified. 

    # 5. A hybrid model will be created with 3 months forecast from the
    #    champion rank 1 model 
    #    and remaining 15 months forecast from the model above and created #12MONTHS
    #    with champion rank 0.
    #    For other columns, rank 1 model will copied verbatim, 


    df_champion_altered = DataFrame(columns=['RUN_TIME_STAMP', \
        'ITEM_ID', 'MONTH_YEAR', 'FORECAST_METHOD',  \
        'FORECAST_VALUE', 'TEST_MAPE', 'TEST_MAD','UPDATE_TIME_STAMP', 'CHAMP_RANK','LONG_FORECAST'])
        
    try:        
        for pf in items:
            df_false_champion = df.loc[((df.ITEM_ID == pf) & \
                                        (df.CHAMP_RANK == 1)) & \
                    ((df.LONG_FORECAST == 'F') | (df.LONG_FORECAST == 'D'))]
            if not df_false_champion.empty: 
                print (pf)
                df_true_champion = df.loc[((df.ITEM_ID == pf) & \
                                         (~df.LONG_FORECAST.isin(['F','D'])))\
                                    ].sort_values(by=['CHAMP_RANK']).head(15)
                if df_true_champion.empty and len(forecast_methods) < 2:
                    exception_msg = "For this PF / TOS :"+str(pf)+"\n"+\
                           "There is no alternate champion as well as the run \
                    is made only one forecast method i.e :"+\
                        str(forecast_methods)
                    print (exception_msg)
                    
                    raise Exception (exception_msg)
                if df_true_champion.empty : 
                    print ("There is no alternate champion for \
                           this PF / TOS :",
                           str(pf))
                    continue
                    
                df_true_champion = df_true_champion.iloc[to_datetime(\
                        df_true_champion.MONTH_YEAR, format='%b-%Y').argsort()]
                df_hybrid_model = df_false_champion
                df_hybrid_model.FORECAST_VALUE[3:6] = \
                    df_false_champion.FORECAST_VALUE[3:6]
                df_hybrid_model.FORECAST_VALUE[6:] = \
                    df_true_champion.FORECAST_VALUE[6:]
                df_hybrid_model.CHAMP_RANK = 0
                df_hybrid_model.FORECAST_METHOD = \
                    df_false_champion.FORECAST_METHOD.iloc[0] + '-' + \
                        df_true_champion.FORECAST_METHOD.iloc[0]
                df_champion_altered = df_champion_altered.append(\
                                                            df_hybrid_model)
                #break
    
        # 6. the forecast values of the rank 1 model for the last 15 months  
        #    will then be altered along with the forecast method updated to a 
        #   concatenated text “Method 1-Method 2” of the 2 models used. 
    
        champion_altered_pfs = df_champion_altered.ITEM_ID.unique()
        df = df.append(df_champion_altered)
        for pf in champion_altered_pfs:
            df.loc[df.ITEM_ID == pf,'CHAMP_RANK'] += 1
    
    
        # 7. writing to database table
    
        #df = df.drop(['UPDATE_TIME_STAMP'], axis = 1)  
        df.FORECAST_VALUE = df.FORECAST_VALUE.astype(int)
        print("Final dataframe shape :", df.shape)
        print("Final dataframe :", df.head(1))
        print("update_Timestamp :",df["UPDATE_TIME_STAMP"].unique())
        
        #Added - 28Feb'23
        tz = timezone('EST')
        now_est = datetime.now(tz)
        now_est = str(now_est)
        
        df["UPDATE_TIME_STAMP"]=now_est[:26]  
        #Added - 28Feb'23        
    
        #cur.execute(con.delete_query)
    
         #Static table name entered, for Append large volume rows
        cur, conn = connect_to_db()
        from snowflake.connector.pandas_tools import write_pandas
        try:
            write_pandas(conn, df, 'KFS_ORDERS_BASELINE_FORECAST3')
        except Exception as e:
            print(f'Error while writing data to table KFS_ORDERS_BASELINE_FORECAST3 : {e}')
            
        #Added on 06Feb23 - Ranjan - Successful
        sns = boto3.client("sns", region_name=region_name)
        sns.publish(TopicArn=sns_Topic_Arn, 
                Message="Hi All,\n\n DFA Fenwal Glue6 run is successful.Altered Champion Forecast is available in Snowflake Table - KFS_ORDERS_BASELINE_FORECAST3 \n\nThanks",
                Subject=" DFA Fenwal Glue6 run is successful")
        #Added on 06Feb23 - Ranjan - Successful            

    except Exception as altered_champions_final_df_exception:
        if 'only one forecast method' in \
            str(altered_champions_final_df_exception):
            print ("@@@",altered_champions_final_df_exception)
            create_and_insert_error(run_time_stamp)
          
        else:    
            print ("Error occured at altered_champions_final_df block\n",\
                   str(altered_champions_final_df_exception))
            create_and_insert_error(run_time_stamp)
            sns = boto3.client("sns", region_name=region_name)
            sns.publish(TopicArn=sns_Topic_Arn, 
                    Message="Hi All,\n\nException occured inside DFA Fenwal Glue6, Kindly check KFS_ERROR_LOG \n\nThanks", 
                    Subject="Alert: Exception occured while running")            
            raise
# To execute this module when called
if __name__ == "__main__":
    champion_alteration()