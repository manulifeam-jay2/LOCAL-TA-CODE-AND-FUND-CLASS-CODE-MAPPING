import os
import pandas as pd
from databricks import sql

from dotenv import load_dotenv
import os
from datetime import datetime

# Load variables from .env file
load_dotenv()

def format_YYYYMMDDHHMMSS(dt: datetime) -> str:
    return dt.strftime('%Y%m%d%H%M%S')


# Access the variables
HOST = os.getenv("HOST")
AUM = os.getenv("AUM")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
EDL_Master_List_Output_Path = os.getenv("EDL_Master_List_Output_Path")
now_str = format_YYYYMMDDHHMMSS(datetime.now())
fp = os.path.join(EDL_Master_List_Output_Path, f"EDL_Master_{now_str}.xlsx")


print("Host:", HOST)
print("AUM:", AUM)
print("Output file path", fp)

connection = sql.connect(
    server_hostname=HOST,
    http_path=AUM,
    access_token=ACCESS_TOKEN
)

cursor = connection.cursor()
# cursor1 = connection.cursor()

cursor.execute(
"""
SELECT DISTINCT fund_class_code_1, liability_portf_code, liability_portf_name,
country_of_domicile_code, owner_type
FROM `hive_metastore`.`inv_dal_eod`.`cube_portfolios`
WHERE country_of_domicile_code IN ('BB', 'HK', 'PH', 'TW', 'SG', 'MM', 'KR', 'CN', 'MY', 'TH', 'VN', 'IN', 'KH', 'ID', 'JP');
"""
)

result = cursor.fetchall()
# result1 = cursor1.fetchall()
# print("Show result", result)

df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
# df1 = pd.DataFrame(result1, columns=[desc[0] for desc in cursor1.description])

df.to_excel(fp, sheet_name='edl_databricks', index=True)


cursor.close()
connection.close()
 