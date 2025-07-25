
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import snowflake.connector
import warnings
import json
from datetime import datetime,date,timedelta
import time
import calendar
warnings.filterwarnings('ignore')
from sqlalchemy import create_engine
import logging

class SnowflakeDataUploder:
    def read_large_data(self, file_path: str):
        large_df = pd.read_excel(file_path)
        return large_df

    def connect_to_snowflake(self):
        conn = snowflake.connector.connect(
            user="MICHAEL.JONES@KINESSO.COM",  # Write your user id
            account="kinesso.us-east-1",
            authenticator="externalbrowser",
            role="UMUK_DYSON_GLOBAL",
            warehouse="UMUK_DYSON_GLOBAL",
            database="GR_KINESSO",
            schema="UMUK_DYSON_GLOBAL"
        )
        return conn

    def load_data_to_snowflake(self, conn, df_chunk, sf_table_name):
        cursor = conn.cursor()
        try:
            write_pandas(df_chunk, sf_table_name)
        except Exception as e:
            print('Error: ', e)

    def load_large_dataframe_to_snowflake(self, df, sf_table_name, batch_size=10000):
        conn = self.connect_to_snowflake()

        df_batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]

        for batch in df_batches:
            self.load_data_to_snowflake(conn, batch, sf_table_name)

#------------------------Main file --------------------#
# Read main table from snowflake
sf_table_name = 'MT_MEDIAPLAN_NL_V3'

conn = SnowflakeDataUploder().connect_to_snowflake()
cur = conn.cursor()
query = f"""SELECT * FROM {sf_table_name}"""
cur.execute(query)
records = cur.fetchall()

columns = [desc[0] for desc in cur.description]

main_df = pd.DataFrame(records, columns=columns)

## Renaming the column names
cols = ['Country','Plan','Campaign','Category Code','Sub-Category','Product','NPD Number','Funnel Objective',
        'Global Media Type','Media Group','Media Type','Sub-Type','Flight Status','Net Cost GBP Planned',
        'Net Cost GBP Actual','GRPs','Impressions','Engaged Visits','Sessions','Revenue Net Cost GBP',
        'ROAS Net Cost GBP','Clicks','Year','Quarter','Month','Week','EndDate','timestamp','Flag','last_modified']
main_df.columns = cols

## Create a new column.
main_df['origin'] = 'previous'
cols = main_df.columns.tolist()
cols.insert(26,cols[-1])
cols.pop()
cols.pop()
main_df = main_df[cols]

main_df = main_df.round({'Net Cost GBP Planned': 9,'Net Cost GBP Actual': 9,'GRPs': 9,'Impressions': 9,
                         'Engaged Visits': 9,'Sessions': 9,'Revenue Net Cost GBP': 9,
                         'ROAS Net Cost GBP': 9,'Clicks': 9})


## Converting selected metrics to Integers for accurate comparisons.
main_df['Net Cost GBP Planned Comp'] = main_df['Net Cost GBP Planned'].astype(int)
main_df['Net Cost GBP Actual Comp'] = main_df['Net Cost GBP Actual'].astype(int)
main_df['GRPs Comp'] = main_df['GRPs'].astype(int)
main_df['Impressions Comp'] = main_df['Impressions'].astype(int)
main_df['Engaged Visits Comp'] = main_df['Engaged Visits'].astype(int)
main_df['Sessions Comp'] = main_df['Sessions'].astype(int)
main_df['Revenue Net Cost GBP Comp'] = main_df['Revenue Net Cost GBP'].astype(int)
main_df['ROAS Net Cost GBP Comp'] = main_df['ROAS Net Cost GBP'].astype(int)
main_df['Clicks Comp'] = main_df['Clicks'].astype(int)
main_df = main_df[main_df['Flag'] == 'Active']
main_df = main_df[main_df['NPD Number'].notnull()]


#------------------------Stage file --------------------#
stage_file = 'RawGuidelineReports/2023-2025 MediaTools BOLT NPD.xlsm'
stage_df = pd.read_excel(stage_file)

##
stage_df['Week'] = stage_df['Week'].dt.date
stage_df['EndDate'] = stage_df['EndDate'].dt.date

stage_df = stage_df.round({'Net Cost GBP Planned': 9,'Net Cost GBP Actual': 9,'GRPs': 9,'Impressions': 9,
                           'Engaged Visits': 9,'Sessions': 9,'Revenue Net Cost GBP': 9,
                           'ROAS Net Cost GBP': 9,'Clicks': 9})

stage_df['origin'] = 'current'
stage_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

stage_df['Flag'] = ['New' for date in stage_df['EndDate'] if date is not None]

## Converting selected metrics to Integers for accurate comparisons.
stage_df['Net Cost GBP Planned Comp'] = stage_df['Net Cost GBP Planned'].astype(int)
stage_df['Net Cost GBP Actual Comp'] = stage_df['Net Cost GBP Actual'].astype(int)
stage_df['GRPs Comp'] = stage_df['GRPs'].astype(int)
stage_df['Impressions Comp'] = stage_df['Impressions'].astype(int)
stage_df['Engaged Visits Comp'] = stage_df['Engaged Visits'].astype(int)
stage_df['Sessions Comp'] = stage_df['Sessions'].astype(int)
stage_df['Revenue Net Cost GBP Comp'] = stage_df['Revenue Net Cost GBP'].astype(int)
stage_df['ROAS Net Cost GBP Comp'] = stage_df['ROAS Net Cost GBP'].astype(int)
stage_df['Clicks Comp'] = stage_df['Clicks'].astype(int)

## Combine the two dataframes (Main & Stage)
df_combined = pd.concat([main_df, stage_df])

# All columns compared and boolean value stored in C1
df_combined['dup_c1'] = df_combined.duplicated(subset=['Country','Plan','Campaign','Category Code','Sub-Category',
                                                       'Product','NPD Number','Funnel Objective','Global Media Type',
                                                       'Media Group','Media Type','Sub-Type','Flight Status',
                                                       'Net Cost GBP Planned Comp','Net Cost GBP Actual Comp',
                                                       'GRPs Comp','Impressions Comp','Engaged Visits Comp',
                                                       'Sessions Comp','Revenue Net Cost GBP Comp',
                                                       'ROAS Net Cost GBP Comp','Clicks Comp','Year',
                                                       'Quarter','Month','Week','EndDate'])
#
# # Only dimension columns compared and boolean value stored in C2
df_combined['dup_c2'] = df_combined.duplicated(subset=['Country','Plan','Campaign','Category Code',
                                                       'Sub-Category','Product','NPD Number','Funnel Objective',
                                                       'Global Media Type','Media Group','Media Type','Sub-Type',
                                                       'Flight Status','Year','Quarter','Month','Week','EndDate'])

df_combined['dup_c3'] = df_combined.duplicated(subset=['Country','Plan','Campaign','Category Code','Sub-Category',
                                                       'Product','NPD Number','Funnel Objective','Global Media Type',
                                                       'Media Group','Media Type','Sub-Type','Flight Status',
                                                       'Net Cost GBP Planned','Net Cost GBP Actual','GRPs',
                                                       'Impressions','Engaged Visits','Sessions',
                                                       'Revenue Net Cost GBP','ROAS Net Cost GBP',
                                                       'Clicks','Year','Quarter','Month','Week','EndDate'])

df_combined['dup_c4'] = df_combined.duplicated(subset=['Country','Plan','Campaign','Category Code','Sub-Category',
                                                       'Product','NPD Number','Funnel Objective',
                                                       'Global Media Type','Media Group','Media Type',
                                                       'Sub-Type','Flight Status','Year','Quarter',
                                                       'Month','Week','EndDate','Net Cost GBP Planned Comp',
                                                       'Net Cost GBP Actual Comp'])

final_df = df_combined[df_combined['origin'] == 'current']
final_df = final_df.drop(columns=['Net Cost GBP Planned Comp','Net Cost GBP Actual Comp','GRPs Comp',
                                  'Impressions Comp','Engaged Visits Comp','Sessions Comp','Revenue Net Cost GBP Comp',
                                  'ROAS Net Cost GBP Comp','Clicks Comp','dup_c3','dup_c4'])

# # Replacing all '-' and " " with "_" against all columns.
final_df.columns = map(lambda x: x.replace('-','_').replace(' ', '_'), final_df.columns)
#
# # Handle dates for snowflake
final_df['WEEK'] = pd.to_datetime(final_df['Week'],errors='coerce').dt.date
final_df['ENDDATE'] = pd.to_datetime(final_df['EndDate'],errors='coerce').dt.date

# # fill all the null values
final_df = final_df.fillna('')

## Create a Snowflake connection.
# sf = SnowflakeDataUploder()
# stage_table_name = 'STAGE_MT_MEDIAPLAN_NL_V3'
# conn = sf.connect_to_snowflake()
# cursor = conn.cursor()

# query = f'''
#             CREATE OR REPLACE TABLE {stage_table_name} (
#                 COUNTRY VARCHAR(25),
#                 PLAN VARCHAR(255),
#                 CAMPAIGN VARCHAR(255),
#                 CATEGORY_CODE VARCHAR(10),
#                 SUB_CATEGORY VARCHAR(255),
#                 PRODUCT VARCHAR(255),
#                 NPD_NUMBER VARCHAR(255),
#                 FUNNEL_OBJECTIVE VARCHAR(255),
#                 GLOBAL_MEDIA_TYPE VARCHAR(255),
#                 MEDIA_GROUP VARCHAR(255),
#                 MEDIA_TYPE VARCHAR(255),
#                 SUB_TYPE VARCHAR(100),
#                 FLIGHT_STATUS VARCHAR(25),
#                 Net_Cost_GBP_Planned FLOAT,
#                 Net_Cost_GBP_Actual FLOAT,
#                 GRPS FLOAT,
#                 IMPRESSIONS FLOAT,
#                 ENGAGED_VISITS FLOAT,
#                 SESSIONS FLOAT,
#                 REVENUE_NET_COST_GBP FLOAT,
#                 ROAS_NET_COST_GBP FLOAT,
#                 CLICKS FLOAT,
#                 YEAR NUMBER(38,0),
#                 QUARTER VARCHAR(10),
#                 MONTH VARCHAR(5),
#                 WEEK DATE,
#                 ENDDATE DATE,
#                 origin Varchar(10),
#                 timestamp TIMESTAMP,
#                 Flag VARCHAR(255),
#                 dup_c1 BOOLEAN,
#                 dup_c2 BOOLEAN
#             )'''
#
# cursor.execute(query)

# ------------------Load data into snowflake---------------#
# sf.load_large_dataframe_to_snowflake(final_df, stage_table_name)

# print('STAGE DATA UPLOADED SUCCESSFULLY!!!!!!')

# print(final_df.columns)
print(final_df.head())

folder_name = 'Transformed Guideline Report'
final_df.to_excel(f"{folder_name}/TransformedGuidelineReport_{datetime.now().strftime('%Y%m%d')}.xlsx")