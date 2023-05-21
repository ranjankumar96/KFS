# Code modified on 3Feb23


from numpy import quantile, where
from pandas import to_datetime, read_sql
from calendar import month_abbr
from dateutil.relativedelta import relativedelta

import processed_configuration as con
from error_logging import create_and_insert_error
import awswrangler as wr

def outlier_treatment(run_time_stamp, cur, conn, sku_list):
    try:
        print("Inside... outlier_treatment()")
        data2 = read_sql(con.prsd_target_query, conn)
        print("Shape of data read from snowflake : ", data2.shape)
        
        #Added on 3Feb23 - Priyanka - starts
        data2["ITEM_ID"]=data2["ITEM_ID"].astype(str) #Added 9Feb23
        data=data2[data2["ITEM_ID"].isin(sku_list)] 
        print("Shape of dataset post Item_id filter : ", data.shape)
        print("Unique ITEM_ID's : ", data["ITEM_ID"].nunique())
        #Added on 3Feb23 - Priyanka - ends
        
        data = data.drop(['RUN_TIME_STAMP'],axis=1)
        prod_fam_list = data['ITEM_ID'].unique()
        print("original data")
        print(len(prod_fam_list))
        pf_final = []
        
        for pf in prod_fam_list:
            order_pf = data[data['ITEM_ID']==pf].copy()
            if ((order_pf.shape[0]>=3)):
                pf_final.append(pf) 
        print("after 3 months condition")        
        print(len(pf_final))
        
        for pf in pf_final:
            
           # Calculate 1st and 99th percentile
            q1 = quantile(data[data['ITEM_ID'] == pf]['UNITS'], 
                          0.01)
            q3 = quantile(data[data['ITEM_ID'] == pf]['UNITS'],
                          0.99)

            # Define Boundries for outlier as 1st and 99th percentile
            lower_bound = int(max(q1, 0))
            upper_bound = int(q3)
            
            # Capping and flooring the orders within the boundaries
            data['UNITS'] = where(
                            (
                                (data['ITEM_ID']==pf) &
                                (data['UNITS'] > upper_bound)
                            ),
                            upper_bound,
                            data['UNITS']
                            )

            data['UNITS'] = where(
                            (
                                (data['ITEM_ID']==pf) &
                                (data['UNITS'] < lower_bound)
                            ),
                            lower_bound,
                            data['UNITS']
                            )               
        
        order = data.copy()
        print("Length of Order data : ", order.shape)
        print("Target data : ", order.head(2))
        
        order.rename(columns = {
                    'MONTH_YEAR':'timestamp',
                    'UNITS':'target_value',
                    'ITEM_ID':'item_id',
                    },inplace=True)
        order['YEAR'] = order['timestamp'].str[-4:]
        order['YEAR'] = order['YEAR'].astype(int)
        order['MONTH'] = order[\
                                     'timestamp'
                                    ].str[:3].apply(lambda x: list(\
                                                                   month_abbr
                                                                   ).index(x))
        order['DAY'] = 1
        order['timestamp'] = to_datetime(order[['YEAR', 'MONTH', 'DAY']])
        
        min_date = min(order['timestamp'])
        print("min_date :", min_date)
        
        total_months = con.comp_forecast_horizon + con.buffer
        print("total_months :", total_months)
        
        print("maximum order timestamp : ", max(order['timestamp']))
        
        max_date = max(order['timestamp']) + \
        relativedelta(months = total_months)
        print("max_date :", max_date)
        
        target_data1 = order.sort_values(\
                                            by = ['item_id','timestamp']
                                            ).reset_index(drop = True)
        print("Shape of target_data1 : ", target_data1.shape)
        print("Target_data1 :", target_data1.head(2))

        target_data3=target_data1.copy()
        pf_tos_list = list(target_data3.item_id.unique())
        print("Length of pf_tos_list : ", len(pf_tos_list))
        
        dict_pf_tos_str_dt = {}        
        
        for item in pf_tos_list:
            temp = target_data3[target_data3['item_id'] == item]
            dict_pf_tos_str_dt[item] = min(temp['timestamp'])
            
        target_data3['timestamp'] = target_data3['timestamp'].astype(str)
        
        # Rearranging the columns as required
        target_data3 = target_data3[["timestamp", "target_value", "item_id"]]
        print("Shape of final Target dataset : ", target_data3.shape)
        
        analytical_path = con.aws_service1 + "://" + \
                          con.bucket_name + "/" + \
                          con.folder_name + "/" + \
                          con.target_file_name
        print("analytical_path : ", analytical_path)
        
        
        wr.s3.to_csv(target_data3, analytical_path, index=False)
        print("Exiting... outlier_treatment()")        
        return data,min_date,max_date,pf_tos_list

    except:
        print("Exception occurred inside outlier_treatment()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise