# Code modified on 3Feb23

from pandas import read_sql, to_datetime

from calendar import month_abbr
import processed_configuration as con
from error_logging import create_and_insert_error
import awswrangler as wr

def concat_for_rel(min_date, max_date, run_time_stamp, cur, conn, sku_list):
    try:
        print("Inside... concat_for_rel()")
        
        # Get the data from the DB
        future_order2 = read_sql(con.prsd_related_query, conn)
        print("Shape of data read from snowflake : ", future_order2.shape)
        
        #Added on 3Feb23 - Priyanka - starts
        future_order2["ITEM_ID"]=future_order2["ITEM_ID"].astype(str) #Added 9Feb23
        future_order=future_order2[future_order2["ITEM_ID"].isin(sku_list)] 
        print("Shape of dataset post Item_id filter : ", future_order.shape)
        print("Unique ITEM_ID's : ", future_order["ITEM_ID"].nunique())
        #Added on 3Feb23 - Priyanka - ends        
        
        future_order = future_order.drop(['RUN_TIME_STAMP'], axis=1)        
        future_order['YEAR'] = future_order['MONTH_YEAR'].str[-4:]
        future_order['YEAR'] = future_order['YEAR'].astype(int)
        future_order['MONTH'] = future_order['MONTH_YEAR'].str[:3]
        future_order['MONTH'] = future_order['MONTH'].apply(lambda x: list(month_abbr).index(x))
        future_order['DAY'] = 1
        future_order['timestamp'] = to_datetime(future_order[[\
                                                              'YEAR', 
                                                              'MONTH', 
                                                              'DAY'
                                                              ]])
        print("Shape of future_order dataset : ", future_order.shape)
        
        # Slice data for the required time range
        future_order = future_order[\
                                    (future_order['timestamp'] >= min_date) & 
                                    (future_order['timestamp'] <= max_date)
                                    ].copy()
        print("Shape of future_order dataset post applying min_date & max_date filter: ", future_order.shape)
        
        future_order = future_order.drop([\
                                          'MONTH_YEAR',
                                          'YEAR',
                                          'MONTH',
                                          'DAY'
                                          ], axis=1)
        print("Shape of future_order dataset post dropping columns: ", future_order.shape)
        
        future_order['timestamp'] = future_order['timestamp'].astype(str)
        future_order1=future_order.rename(columns = {'ITEM_ID':'item_id','FUTURE_ORDERS':'Future_Orders'})
        df_rel_final = future_order1.sort_values(by = ['item_id','timestamp'])
        df_rel_final['timestamp'] = df_rel_final['timestamp'].astype(str)
        df_rel_final1 = df_rel_final[["timestamp", "item_id", "Future_Orders"]].copy()
        
        print("Shape of final future_order dataset: ", df_rel_final1.shape)
        print("Final future_order dataset : ", df_rel_final1.head(2))
        
        analytical_path = con.aws_service1 + "://" + \
                          con.bucket_name + "/" + \
                          con.folder_name + "/" + \
                          con.rel_file_name
        print("analytical_path : ", analytical_path)
        
        wr.s3.to_csv(df_rel_final1, analytical_path, index=False)
        print("Exiting... concat_for_rel()")
        return df_rel_final1
    
    except:
        print("Exception occurred inside concat_for_rel()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise
