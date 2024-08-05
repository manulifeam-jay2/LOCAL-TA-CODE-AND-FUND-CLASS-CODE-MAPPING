import os
import pandas as pd
from loguru import logger as L
from databricks import sql
from dotenv import load_dotenv
from datetime import datetime
from libs import auto_adjust_column_widths, format_YYYYMMDDHHMMSS

# Load variables from .env file
load_dotenv()

# Access the variables
HOST = os.getenv("HOST")
AUM = os.getenv("AUM")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
Root_Folder_Path = os.getenv("Root_Folder_Path")
EDL_Master_List_Output_Folder = os.getenv("EDL_Master_List_Output_Folder")
now_str = format_YYYYMMDDHHMMSS(datetime.now())
fp = os.path.join(Root_Folder_Path, EDL_Master_List_Output_Folder, f"EDL_Master_{now_str}.xlsx")

L.info(f"Server Host: {HOST}")
L.info(f"Http Path: {AUM}")
L.info(f"Output file path: {fp}")

connection = sql.connect(
    server_hostname=HOST,
    http_path=AUM,
    access_token=ACCESS_TOKEN
)

query_sql = """
    WITH RankedDatesData AS (
        SELECT 
            fund_class_code_1, 
            liability_portf_code, 
            liability_portf_name,
            country_of_domicile_code, 
            owner_type,
            portfolio_group_code,
            portfolio_group_name,
            ROW_NUMBER() OVER (PARTITION BY liability_portf_code ORDER BY effective_date DESC) AS rn
        FROM `hive_metastore`.`inv_dal_eod`.`cube_portfolios`
        WHERE country_of_domicile_code IN ('BB', 'HK', 'PH', 'TW', 'SG', 'MM', 'KR', 'CN', 'MY', 'TH', 'VN', 'IN', 'KH', 'ID', 'JP')
        AND liability_portf_code <> ''
    )
    SELECT 
        CONCAT(country_of_domicile_code, '_', fund_class_code_1) AS edl_unique_key,
        fund_class_code_1 AS edl_fund_class_code_1, 
        liability_portf_code AS edl_liability_portf_code,  
        liability_portf_name AS edl_liability_portf_name,
        country_of_domicile_code AS edl_country_of_domicile_code, 
        owner_type AS edl_owner_type, 
        portfolio_group_code AS edl_portfolio_group_code, 
        portfolio_group_name AS edl_portfolio_group_name
    FROM RankedDatesData
    WHERE rn = 1
"""

with connection.cursor() as cursor:
    cursor.execute(query_sql)
    rslt = cursor.fetchall()
    df = pd.DataFrame(rslt, columns=[desc[0] for desc in cursor.description])

df.to_excel(fp, sheet_name='edl_databricks', index=False)

L.info("Auto Adjust width")
auto_adjust_column_widths(fp)

L.info(f"Done. File path: {fp}")