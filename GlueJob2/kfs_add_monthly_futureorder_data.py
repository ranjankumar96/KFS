# Preprocessing of monthly orders and updating table
#
# Python Version: 3.8.12
#
# Description : Preprocessing the orders data and updating the orders 
#               table
#
# Coding Steps :
#               1. Read the orders data from S3
#               2. Treat negative orders and calculate the future orders 
#                  for all item_id
#               3. Write the processed orders data to the future orders table
#
# Created By: Priyanka Srivastava
#
# Created Date: 22-Dec-2022


# 1. Import built-in packages and user defined functions

import pandas as pd
import numpy as np
import os
from boto3 import resource
from io import BytesIO
from pytz import timezone
from datetime import datetime, date
from calendar import monthrange, month_abbr
from pandas import read_csv, read_excel, concat, read_sql, DataFrame, to_datetime, merge
from dateutil.relativedelta import relativedelta
import awswrangler as wr
from snowflake.connector.pandas_tools import write_pandas

import processed_configuration as con
from error_logging import create_and_insert_error
from snowflake_db_connection import connect_to_db

# 2. Preprocessing the orders data

def add_monthly_data(cur, conn, run_time_stamp):
    """Preprocessing the orders data and updating the orders table.

    Parameters
    ----------
    cur : object
        Snowflake DB cursor object.
    conn : object
        Snowflake DB connection object.        
    run_time_stamp : string
        Timestamp based run id. for the job.

    How it works
    ------------
        1. Read the incremental current month
        orders data from S3.
        2. Filter data for 590 sku. Exclude Order Types FH, FL, FP, FQ, FS, FV, ZA, ZB, ZO,
        which in turn removes 1 item (Hence, left with 589 skus).
        
        
        4. Write the processed orders data to the orders table.

    """
    try:
        print("Inside... add_monthly_data()")

        # Get the current time, month and year in EST zone
        tz = timezone('EST')
        now_est = datetime.now(tz)
        curr_mon = now_est.strftime("%b")
        curr_yr = now_est.strftime("%Y")

        print("tz: ",tz,"\nnow_est: ",now_est,"\ncurr_mon: ",curr_mon,"\ncurr_yr: ",curr_yr)
        
        # Connect to AWS S3 bucket where the orders file reside
        s3 = resource(con.aws_service1)
        bucket = s3.Bucket(con.bucket_name)
        prefix = con.folder_name
        prefix_objs = bucket.objects.filter(Prefix = prefix)
        
        # Initializing the list of dataframes that will
        # hold incremental monthly data
        prefix_dfs = []

        # Setting the file available indicator to false
        incre_file_ind = False
        
        # Iterating through the objects in S3 bucket folder
        # to find incremental monthly file
        for obj in prefix_objs:
            name, extension = os.path.splitext(obj.key)
            body = obj.get()['Body'].read()
            if 'future_orders' in str(obj).lower():
                if(extension == '.csv'):
                    temp = read_csv(BytesIO(body))
                elif(extension == '.xlsx'):
                    temp = read_excel(BytesIO(body),engine='openpyxl')
                else:
                    raise Exception( \
                            "Future Orders file is not " + 
                            "in the expected format (csv or xlsx) for the " +
                            curr_mon + "-" + curr_yr + " run."
                           )

                if(temp.shape[0] == 0):
                    raise Exception( \
                            "Future Orders file is empty for the " +
                            curr_mon + "-" + curr_yr + " run."
                           )

                prefix_dfs.append(temp)
                incre_file_ind = True
                
        # Raising the resp. exception message if
        # the file is not found
        if (not incre_file_ind):
            raise Exception( \
                            "Future Orders data is missing for the " + 
                            curr_mon + "-" + curr_yr + " run."
                           )
        
        # Concatenating the list of dfs
        if(len(prefix_dfs)>0):
            for i in range(len(prefix_dfs)):
                prefix_dfs[i] = prefix_dfs[i][con.req_columns]
            df_monthly_future_order = concat(prefix_dfs, axis = 0, ignore_index = True)
        else:
            raise Exception( \
                            "Orders data is not available for the "
                            + curr_mon + "-" + curr_yr + " run."
                           )
            
        print("Shape of Incremental data read from S3 : ",df_monthly_future_order.shape)
        print("Incremental df_monthly_future_order : ", df_monthly_future_order.head(1))
        
        # Setting incremental order date to 1st day of the month
        input_dt = df_monthly_future_order["Order Date"].min()
        print('Incremental order date is:', input_dt)
        incre_ordt = input_dt.replace(day=1)
        print('Datetime is :', incre_ordt)
        
        #Dropping last row    
#         df_monthly_future_order.drop(df_monthly_future_order.tail(1).index,inplace=True)
        
        # Added - 2Feb23 For removing blank rows
        df_monthly_future_order=df_monthly_future_order[~((df_monthly_future_order["3rd Item Number"].isna()) | \
                                                          (df_monthly_future_order["3rd Item Number"]==""))]
        print("Shape of Incremental data post removing blank rows : ",df_monthly_future_order.shape)
        
        df_monthly_future_order.columns=df_monthly_future_order.columns.str.strip().str.replace(' ', '_')
        
        # Keeping track of original names of SKUs
        df_monthly_future_order["3rd_Item_Number_orig"]=df_monthly_future_order["3rd_Item_Number"]
        df_monthly_future_order["2nd_Item_Number_orig"]=df_monthly_future_order["2nd_Item_Number"]
        
        # Replacing columns with " " values with ""
        cols_to_strip=['3rd_Item_Number','2nd_Item_Number','Or_Ty']

        for cl in cols_to_strip:
            df_monthly_future_order[cl]=df_monthly_future_order[cl].astype(str).str.strip()
            df_monthly_future_order[cl]=df_monthly_future_order[cl].str.upper()
            
        
        df_monthly_future_order['INCREMENTAL_MONTH'] = to_datetime(df_monthly_future_order['Order_Date'])
        df_monthly_future_order['INCREMENTAL_MONTH'] = \
                to_datetime(df_monthly_future_order['INCREMENTAL_MONTH'].dt.date).dt.strftime('%b-%Y')
        print(df_monthly_future_order.shape)
        df_monthly_future_order.head(2)
        
        df_monthly_future_order["INCREMENTAL_MONTH"].value_counts(dropna=False)
        
        # Creating a list of periods for which the data is available
        periods2 = list((df_monthly_future_order['INCREMENTAL_MONTH']).unique())
        print("periods2 : ",periods2)
        
        periods = [x for x in periods2 if str(x) != 'nan']
        print("periods after removing nan if it exists : ",periods)
        
        print("Number of SKUs before filtering order types : ", df_monthly_future_order["3rd_Item_Number"].nunique())
        
        # 2. Excluded Order Types FH, FL, FP, FQ, FS, FV, ZA, ZB, ZO
        df_monthly_future_order["Flag_step2"]=np.where(df_monthly_future_order["Or_Ty"]\
                                                       .isin(["FH","FL","FP","FQ","FS","FV","ZA","ZB","ZO"]),1,0)
        
        # Dropping excluded order type rows
        df_monthly_future_order2=df_monthly_future_order.query("Flag_step2 == 0")
        print("Data left after excluding 9 order types for ALL SKUs : ",df_monthly_future_order2.shape)
        
        print("Number of SKUs left after filtering order types : ", df_monthly_future_order2["3rd_Item_Number"].nunique())
        
        # New addition - Removing blank item_ids
        df_monthly_future_order2=df_monthly_future_order2[df_monthly_future_order2["3rd_Item_Number"]!=""]
        print("Shape of dataset post removal of blank item_ids : ", df_monthly_future_order2.shape)
        print("Number of SKUs left post removal of blank item_ids : ", df_monthly_future_order2["3rd_Item_Number"].nunique())
        
        
        print("Order date min :", df_monthly_future_order2["Order_Date"].min(),\
              "\nOrder date max :", df_monthly_future_order2["Order_Date"].max())
        
        print("Request date min :", df_monthly_future_order2["Request_Date"].min(),\
              "\nRequest date max :", df_monthly_future_order2["Request_Date"].max())
        
        print("Unique Item_id : " ,df_monthly_future_order2["3rd_Item_Number"].nunique())
        
        
        ### 1. Creating Future order dataset
        
        df_monthly_future_order3=df_monthly_future_order2[["3rd_Item_Number","Request_Date",'Order_Date',"Quantity_Ordered"]]\
        .sort_values(by=["3rd_Item_Number","Request_Date",'Order_Date'])
        
        df_monthly_future_order4=df_monthly_future_order3.groupby\
        (["3rd_Item_Number","Request_Date",'Order_Date'])["Quantity_Ordered"].sum().reset_index()
        
        df_monthly_future_order4["Rq_Date"]=df_monthly_future_order4["Request_Date"].to_numpy().astype('datetime64[M]')
        df_monthly_future_order4["Or_Date"]=df_monthly_future_order4["Order_Date"].to_numpy().astype('datetime64[M]')
        
        print("Shape of df_monthly_future_order4 : ", df_monthly_future_order4.shape)
        print("df_monthly_future_order4 : ", df_monthly_future_order4.head(1))
        
        agg_data=df_monthly_future_order4.groupby(["3rd_Item_Number","Or_Date","Rq_Date",])["Quantity_Ordered"].sum().reset_index()
        print("Shape of dataset on aggregating the data at Order date and Request date : ", agg_data.shape)
        
        print("agg_data['Or_Date'].min() : ", agg_data["Or_Date"].min(),"agg_data['Or_Date'].max() :", agg_data["Or_Date"].max())
        
        agg_data["ind"]=np.where((agg_data["Or_Date"]<agg_data["Rq_Date"]),1,0)
        
        agg_data2=agg_data.copy()
        agg_data2["or_month"]=agg_data2["Or_Date"].dt.month
        agg_data2["or_year"]=agg_data2["Or_Date"].dt.year

        agg_data2["rq_month"]=agg_data2["Rq_Date"].dt.month
        agg_data2["rq_year"]=agg_data2["Rq_Date"].dt.year

              
        agg_data2["same_month_ind"]=np.where(((agg_data2["or_month"]==agg_data2["rq_month"]) \
                                              & (agg_data2["or_year"]==agg_data2["rq_year"])),0,1)
        
        agg_data2["final_ind"]=np.where(((agg_data2["ind"]==1) & (agg_data2["same_month_ind"]==1)),1,0)
        
        print("agg_data2['final_ind'] count: ",agg_data2["final_ind"].value_counts(dropna=False))
        
        agg_data3=agg_data2[agg_data2["final_ind"]==1]
        print("Shape of data post filtering final_ind = 1 : " , agg_data3.shape)
        
        agg_data3.drop(columns={"ind","or_month","or_year","rq_month","rq_year","same_month_ind","final_ind"},inplace=True)
        agg_data3.rename(columns={"Quantity_Ordered":"Future_Orders"},inplace=True)
        print("Shape of data post dropping & renaming columns : ", agg_data3.shape)
        print("Data post dropping & renaming columns : ", agg_data3.head(1))
        
        # 2. Aggregating the data
        data_req=agg_data3.groupby(["3rd_Item_Number","Rq_Date"])["Future_Orders"].sum().reset_index()
        
        # Sorted the data by 3rd_Item_Number & Request_Date
        data_req=data_req[["3rd_Item_Number","Rq_Date","Future_Orders"]].sort_values(by=["3rd_Item_Number","Rq_Date"])
        
        data_req["Future_Orders"].fillna(0,inplace=True)
        print("value_counts of Future_Orders : ", data_req["Future_Orders"].value_counts(dropna=False))
        
        # Assumed that all the orders are placed on 1st of that month and created a Date column
        data_req["Date"]=data_req["Rq_Date"].to_numpy().astype('datetime64[M]')
        
        # Aggregating the Quantity_Ordered at Monthly level
        data_req=data_req.groupby(["3rd_Item_Number","Date"])["Future_Orders"].sum().reset_index()
        
        # 2. Checking if there's any negative orders in Quantity_Ordered per month per sku!
        data_req["Flag"]=np.where(data_req["Future_Orders"]<0,1,0)
        data_req["Future_Orders_n"]=np.where(data_req["Future_Orders"]<0,0,data_req["Future_Orders"])
        data_req.drop(columns={"Future_Orders"},inplace=True)
        data_req.rename(columns={"Future_Orders_n":"Future_Orders"},inplace=True)
        print("Shape of dataset after treating for negative orders : ", data_req.shape)
                
        print("Flag counts : ", data_req["Flag"].value_counts(dropna=False))
        
        
        # Dropping column not required
        data_req.drop(columns={"Flag"},inplace=True)
        print("Data after removing Flag column : ", data_req.head(1))
        
        print(data_req["Date"].min(),data_req["Date"].max())
        
        
        # Reading historical datasets
        print("Reading the Historical orders table ...",con.prsd_KFS_orders_table)
        kfs_proc_order2= "select * from " + con.prsd_KFS_orders_table
        kfs_proc_order=read_sql(kfs_proc_order2, conn)
        
        kfs_proc_order["ITEM_ID"]=kfs_proc_order["ITEM_ID"].astype(str) #Added 9thFeb23
        
        print("Reading the Historical Future orders table ...",con.prsd_inter_FUTURE_ORDER_table)
        kfs_fut_order2= "select * from " + con.prsd_inter_FUTURE_ORDER_table
        kfs_fut_order=read_sql(kfs_fut_order2, conn)
        
        print("periods :", periods[0])
        
        # Removing data of same months' run
        kfs_fut_order_n=kfs_fut_order[kfs_fut_order["INCREMENTAL_MONTH"]!=periods[0]]
        print(kfs_fut_order_n.shape)
        kfs_fut_order_n.head(2)
        
        kfs_fut_order_n["ITEM_ID"]=kfs_fut_order_n["ITEM_ID"].astype(str) #Added 9thFeb23
        
        kfs_fut_order_n["INCREMENTAL_MONTH"].value_counts(dropna=False)
        
        # Deleting records if it already exsits in the current incremental file
        del_frm_inter_order="delete from PROCESSED_INTER_FUTURE_ORDER_RELATED"
        delete_qry_inter = del_frm_inter_order + " where INCREMENTAL_MONTH = '" + \
                     periods[0] + "'"
        delete_qry_inter
        
        cur.execute(delete_qry_inter)

        # New addition
        data1=kfs_proc_order[["ITEM_ID"]].drop_duplicates() #Historical KFS_PROCESSED_ORDERS_TARGET dataset
        data2=kfs_fut_order_n[["ITEM_ID"]].drop_duplicates() #Historical PROCESSED_INTER_FUTURE_ORDER_RELATED dataset
        data3=data_req[["3rd_Item_Number"]].drop_duplicates() #Monthly incremental dataset

        print("Historical KFS_PROCESSED_ORDERS_TARGET: ",data1.shape,\
             "\nHistorical PROCESSED_INTER_FUTURE_ORDER_RELATED: ",data2.shape,\
             "\nMonthly Incremental data: ",data3.shape)
        
        data3.rename(columns={"3rd_Item_Number":"ITEM_ID"},inplace=True)
        data_all=pd.concat([data1,data2,data3])
        print("Shape of concatenated dataset : ", data_all.shape)
        
        data_all=data_all.drop_duplicates(subset=["ITEM_ID"])
        print("Shape of concatenated dataset post removal of duplicated ITEM_IDs : ", data_all.shape)
        
        data_all["min"]=incre_ordt #NEW
        
        print("Minimum date in data_all dataset : ", data_all["min"].min())
        
        # Add 12 months to the date
        data_all['max']=data_all["min"] + pd.DateOffset(months=con.plus_month_period)
        
        data_all2=data_all.set_index("ITEM_ID")
        
        # Create MultiIndex with separate Date rangs per group
        midx=pd.MultiIndex.from_frame(
            data_all2.apply(
                lambda x: pd.date_range(x['min'],x['max'],freq='MS'), axis=1
            ).explode().reset_index(name='Date')[['Date','ITEM_ID']]
        )

        # Changing the above obtained MultiIndex to DataFrame
        data_all2=midx.to_frame(index=False)
        
        # data_req will contain data for further more future months' data, hence outer join
        # Joining the DataFrame with the actual data to get the Quantity_Ordered populated
        temp3=pd.merge(data_all2,data_req,how='outer',left_on=["ITEM_ID",'Date'],right_on=["3rd_Item_Number",'Date'])
        print("Combined dataset shape : ", temp3.shape)
        
#         Dropping and renaming columns
        temp3["ITEM_ID_N"]=np.where(temp3["ITEM_ID"].isna(),temp3["3rd_Item_Number"],temp3["ITEM_ID"])
        temp3.drop(columns={"ITEM_ID","3rd_Item_Number"},inplace=True)
        temp3.rename(columns={"ITEM_ID_N":"ITEM_ID"},inplace=True)
        print("Shape of dataset post dropping and renaming columns : ", temp3.shape)
        
        temp3["Future_Orders"].fillna(0,inplace=True)
        
        kfs_fut_order_n["year"]=kfs_fut_order_n["MONTH_YEAR"].str[-4:]
        kfs_fut_order_n["month"]=kfs_fut_order_n["MONTH_YEAR"].str[:3]
        kfs_fut_order_n["month"]=pd.to_datetime(kfs_fut_order_n.month, format='%b').dt.month
        kfs_fut_order_n["day"]=1

        kfs_fut_order_n["date_time_str"] = kfs_fut_order_n["year"].astype('str') + '-' + kfs_fut_order_n["month"].astype('str') +\
        '-' + kfs_fut_order_n["day"].astype('str')

        kfs_fut_order_n["Date"]=pd.to_datetime(kfs_fut_order_n['date_time_str'], format='%Y-%m-%d')
        print("kfs_fut_order_n : ", kfs_fut_order_n.head(1))

        kfs_fut_order_n.drop(columns={"year","MONTH_YEAR","month","day","date_time_str"},inplace=True)
        print("Data types of kfs_fut_order_n : ", kfs_fut_order_n.dtypes)
        
    
        #Added on 30thJan23 - starts
        #Working on historical dataset to check item_id's with 0 demand in last 2 years
        data_futord=kfs_fut_order_n.rename(columns={"ITEM_ID":"item_id","FUTURE_ORDERS":"Future_Orders"})
        data_futord.drop(columns={"RUN_TIME_STAMP","INCREMENTAL_MONTH"},inplace=True)
        print("Shape of historical Future order dataset :" , data_futord.shape)
            
        # Appending history and incremental datasets
        data_comb=pd.concat([data_futord,temp3])
        print("Shape of combined dataset : ", data_comb.shape)
                
        print("Maximum timestamp : ", data_all2["Date"].max())
        
        # Checking which all SKUs doesnt have any orders in past 2 years - starting 2021 to 2022 for creating history dataset
        three_years = data_all2["Date"].max() + relativedelta(months=-36)
        print("Date for 3 years : ", three_years)
        
        data_comb["Date_Tag"]=np.where((data_comb["Date"]>three_years),"Last 3 year","Other")
        
        data_comb["Date_Tag"].value_counts(dropna=False)
        
        grp=data_comb.groupby(["item_id","Date_Tag"])["Future_Orders"].sum().reset_index()
        print("Shape of grp table : ", grp.shape)
        
        item_emp=grp[(grp["Future_Orders"]==0) & (grp["Date_Tag"]=="Last 3 year")]
        print("No of items in combined dataset which has 0 demand in past 3 years : ", item_emp.shape)
        
        #Converting the above ontained items to a list
        sku_list_3yrs=item_emp["item_id"].tolist()
        print("List of skus which has 0 demand in past 3 years : ", sku_list_3yrs)
                     
        #We would want to exclude items from above list which were provided by Business
        # File path of SKUs to be forecasted
        sku_file = 's3://' + con.bucket_name + '/' + con.input_sku + '/' + con.sku_file_name
        print("sku_file :", sku_file)
                
        target_sku = wr.s3.read_csv(sku_file)
        print("No of SKUs provided by Business : ", target_sku.shape)
        print("target_sku :", target_sku)
                
        # Convert to string
        target_sku["combined PN"]=target_sku["combined PN"].astype(str)
        target_sku["combined PN"]=target_sku["combined PN"].str.upper()
                
        # Removing leading and trailing spaces
        target_sku["combined_PN_new"]=target_sku["combined PN"].astype(str).str.strip()
                
        # Checking if theres any NaN values in item list
        target_sku[target_sku["combined_PN_new"].isna()]
                
        target_sku["Flag"]=np.where((target_sku["combined_PN_new"]==target_sku["combined PN"]),1,0)
                
        # To check how many SKUs has changed post removing leading and trailing spaces
        print("Flag counts : ", target_sku["Flag"].value_counts(dropna=False))
                
        # Dropping duplicates in #SKUs
        target_sku2=target_sku.copy()
        target_sku2=target_sku2[["combined_PN_new"]].drop_duplicates()
        print("target_sku2 :", target_sku2)
                
        # Converting the new SKUs names to a list - Has been added in main code
        sku_list=target_sku2["combined_PN_new"].tolist()
        print("sku_list : ", sku_list)
        
        # Converting the historical orders ITEM_IDs to a list
        sku_list_ord=data1["ITEM_ID"].tolist()
        sku_list_ord
                
        # Removing sku_list provided by Filip from sku_list_2yrs
        # using set() to perform task
        set1 = set(sku_list) #List of Business SKUs
        print(len(set1))

        set2 = set(sku_list_ord) #List of SKUs in historical orders dataset
        print(len(set2))

        set3 = set(sku_list_3yrs) #SKUs with 0 demand in last 2 yrs + future years
        print(len(set3))

        res = list(set3 - set1 - set2) #We need to exclude SKUs provided by business + historical orders skus for forecasting purpose
        print(len(res))
                
        print(res)
        
        # Changing elements in list to a str in order to write sql code
        if len(res)==1:
            fin_lst=str(tuple(res)).replace(",)",")")
            print("Length of res is 1 & element in it is : ",fin_lst)
            print(type(fin_lst))
        else:
            fin_lst=tuple(res)
            fin_lst = str(fin_lst)
            print("Length of res is NE 1 & elements in it are : ",fin_lst)
            print(type(fin_lst))
        
        # Get the delete and insert queries from config file
        delete_qry_item = con.del_frm_inter_order + " where ITEM_ID in " + \
                     fin_lst + ""
        print("delete query : ", delete_qry_item)
        
        print("No. of resultant skus whose data needs to be deleted from historical inter order table :",len(res))
        
        if len(res)>0:
            cur.execute(delete_qry_item)
            
        # Removing these skus from incremental data as well if exists
        temp4=temp3[~temp3["ITEM_ID"].isin(res)]
        print("Shape of temp4 dataset  post removing 0 demand items : ", temp4.shape)

        #Added on 30thJan23 - ends
        
        temp5=temp4[["Date","ITEM_ID","Future_Orders"]]
        temp5.columns=["timestamp","item_id","Future_Orders"]
        print("temp5 shape : ", temp5.shape) 
        
#         Adding run_time_stamp
        temp5['run_time_stamp'] = run_time_stamp
    
#         Changing format of timestamp to month_year
        # Extracting date from Request_Date column in
        # the format Mon-yyyy
        temp5['mm'] = temp5['timestamp'].dt.month
        
        temp5['mon'] = temp5['mm'].apply(lambda x: month_abbr[x])
        
        print("value count of mon : ", temp5['mon'].value_counts(dropna=False))
        
        temp5['mon_yyyy'] = temp5['mon'] + "-" + temp5['timestamp'].dt.year.astype(str)
        
        temp5=temp5.drop(columns={"mm","mon","timestamp"})
        
        temp5.columns=["ITEM_ID","FUTURE_ORDERS","RUN_TIME_STAMP","MONTH_YEAR"]
        temp5=temp5[["RUN_TIME_STAMP","MONTH_YEAR","ITEM_ID","FUTURE_ORDERS"]]
        # Rounding off the UNITS
        temp5['FUTURE_ORDERS'] = round(temp5['FUTURE_ORDERS'])
        
        print("Shape of temp5 dataset : ", temp5.shape)
        
        print("Unique MONTH_YEAR : ", temp5["MONTH_YEAR"].unique())
        
        temp5["INCREMENTAL_MONTH"]=periods[0]
        
        # Appending this monthly orders data to the intermediate table in snowflake
        #Static table name entered

        # Create table - PROCESSED_INTER_FUTURE_ORDER_RELATED in snowflake which will contain history non-aggregated data

        cur, conn = connect_to_db()
        try:
            write_pandas(conn, temp5, 'PROCESSED_INTER_FUTURE_ORDER_RELATED')
        except Exception as e:
            print(f'Error while writing data to table PROCESSED_INTER_FUTURE_ORDER_RELATED : {e}')
            
        
        # Now, reading the intermediate future order file in order to aggregate at month_year
        kfs_inter_order1= "select * from " + con.prsd_inter_FUTURE_ORDER_table
        kfs_inter_order=read_sql(kfs_inter_order1, conn)
        print("Shape of intermediate table : ", kfs_inter_order.shape)
        
        kfs_inter_order["ITEM_ID"]=kfs_inter_order["ITEM_ID"].astype(str) #Added 9thFeb23
        
               
        kfs_inter_order2=kfs_inter_order.groupby(["ITEM_ID","MONTH_YEAR"])["FUTURE_ORDERS"].sum().reset_index()
        
        # Filtering future orders for Items present in historical orders table
        kfs_inter_order3=kfs_inter_order2[kfs_inter_order2["ITEM_ID"].isin(sku_list_ord)]
        print(kfs_inter_order3.shape)
        kfs_inter_order3.head(2)
        
        kfs_inter_order3["year"]=kfs_inter_order3["MONTH_YEAR"].str[-4:]
        kfs_inter_order3["month"]=kfs_inter_order3["MONTH_YEAR"].str[:3]
        kfs_inter_order3["month"]=pd.to_datetime(kfs_inter_order3.month, format='%b').dt.month
        kfs_inter_order3["day"]=1

        kfs_inter_order3["date_time_str"] = kfs_inter_order3["year"].astype('str') + '-' + kfs_inter_order3["month"].astype('str') +\
        '-' + kfs_inter_order3["day"].astype('str')

        kfs_inter_order3["Date"]=pd.to_datetime(kfs_inter_order3['date_time_str'], format='%Y-%m-%d')
        
        print("max Date in data_all2 dataset is : ",  data_all2["Date"].max())
        
        #Sorting the data
        kfs_inter_order3=kfs_inter_order3.sort_values(by=["ITEM_ID","Date"])
        
        #         Filtering for data till next 12 months
        kfs_inter_order4=kfs_inter_order3[kfs_inter_order3["Date"]<=data_all2["Date"].max()]
        print("kfs_inter_order4 : " ,kfs_inter_order4.tail(1))
        
        kfs_inter_order4["RUN_TIME_STAMP"]=run_time_stamp

        kfs_inter_order4.drop(columns={"year","month","day","date_time_str","Date"},inplace=True)        

        kfs_inter_order4=kfs_inter_order4[["RUN_TIME_STAMP","MONTH_YEAR","ITEM_ID","FUTURE_ORDERS"]]
        print("Shape of final dataset to be inserted in PROCESSED_FUTURE_ORDER_RELATED is : ", kfs_inter_order4.shape)
        print("kfs_inter_order4 : " ,kfs_inter_order4.head(1))
        
              
        # Appending this monthly orders data to the intermediate table in snowflake
        #Static table name entered

        # Create table - PROCESSED_FUTURE_ORDER_RELATED in snowflake which will contain history & current month aggregated data
        # For deleting records from snowflake table
        cur.execute('TRUNCATE TABLE PROCESSED_FUTURE_ORDER_RELATED')
        cur, conn = connect_to_db()
        
        try:
            write_pandas(conn, kfs_inter_order4, 'PROCESSED_FUTURE_ORDER_RELATED')
        except Exception as e:
            print(f'Error while writing data to table PROCESSED_FUTURE_ORDER_RELATED : {e}')
                 
        print("Exiting... add_monthly_data()")
        
    except:
        print("Exception occurred inside add_monthly_data()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise
