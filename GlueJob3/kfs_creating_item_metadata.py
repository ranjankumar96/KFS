from pandas import read_sql
import processed_configuration as con
from error_logging import create_and_insert_error
import awswrangler as wr

import pandas as pd
import numpy as np

# 2. Create item meta data and export it to S3

def creating_item_metadata(pf_tos_list, run_time_stamp, cur, conn):
    try:
        print("Inside... creating_item_metadata()")
        
        df_item_meta = read_sql(con.sel_from_itm_mtd, conn)
        print("Shape of data read from snowflake : ", df_item_meta.shape)
        
        df_item_meta["ITEM_ID"]=df_item_meta["ITEM_ID"].astype(str) #Added 9Feb23
        
        df_final = df_item_meta.rename(columns = { \
                                  'ITEM_ID':'item_id',
                                  'DEMAND_PROFILE':'Demand_Profile',
                                  'PRODUCT_LINE':'Product_Line',
                                  'BUSINESS_TEAM':'Business_Team'
                                  })
        df_final2 = df_final[[ \
                             'item_id',
                             'Demand_Profile',
                             'Product_Line',
                             'Business_Team'                           
                            ]].copy()
        
#         Creating dataframe with pf_tos_list
        df = pd.DataFrame({'item_id':pf_tos_list})
        print("Shape of pf_tos_list dataframe : ", df.shape)
        print("pf_tos_list dataframe : ", df.head(1))
        
#         Merging snowflake item_metadata with the pf_tos_list
        df_final3=pd.merge(df,df_final2,how='left',on=["item_id"])
        print("Shape of df_final3 dataframe : ", df_final3.shape)
        print("df_final3 dataframe : ", df_final3.head(1))
        
        print("Demand_Profile : ", df_final3["Demand_Profile"].value_counts(dropna=False))
        print("Business_Team : ", df_final3["Business_Team"].value_counts(dropna=False))
        print("Product_Line : ", df_final3["Product_Line"].value_counts(dropna=False))
        
#         Replacing NaN values for newly introduced item_id
        df_final3["Demand_Profile_n"]=np.where(df_final3["Demand_Profile"].isna(),"Less Than 1 year data",df_final3["Demand_Profile"])
        df_final3["Product_Line_n"]=np.where(df_final3["Product_Line"].isna(),"NotAvailable",df_final3["Product_Line"])
        df_final3["Business_Team_n"]=np.where(df_final3["Business_Team"].isna(),"NotAvailable",df_final3["Business_Team"])

        df_final3.drop(columns={"Demand_Profile","Product_Line","Business_Team"},inplace=True)
        df_final3.rename(columns={"Demand_Profile_n":"Demand_Profile","Product_Line_n":"Product_Line",\
                                  "Business_Team_n":"Business_Team"},inplace=True)
        
        print("Demand_Profile post NaN exclusion : ", df_final3["Demand_Profile"].value_counts(dropna=False))
        print("Business_Team post NaN exclusion : ", df_final3["Business_Team"].value_counts(dropna=False))
        print("Product_Line post NaN exclusion : ", df_final3["Product_Line"].value_counts(dropna=False))
        
#         Changing datatype to string
        df_final3['Demand_Profile'] = df_final3['Demand_Profile'].astype(str)
        df_final3['Product_Line'] = df_final3['Product_Line'].astype(str)
        df_final3['Business_Team'] = df_final3['Business_Team'].astype(str)
        print("Datatype of final table : ", df_final3.dtypes)
               
        analytical_path = con.aws_service1 + "://" + \
                         con.bucket_name + "/" + \
                         con.folder_name + "/" + \
                         con.itm_mtd_file_name  
        print("analytical_path : ", analytical_path)
        
        df_final3.to_csv(analytical_path, index=False)
        print("Exiting... creating_item_metadata()")
    
    except:
        print("Exception occurred inside creating_item_metadata()")
        
        # Creating and logging an error message, in case of an exception
        create_and_insert_error(cur, run_time_stamp)
        raise