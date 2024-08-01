import os
import pandas as pd
from databricks import sql

HOST = "HOST"
AUM = "AUM"
ACCESS_TOKEN = "ACCESS_TOKEN"

connection = sql.connect(
    server_hostname = HOST,
    http_path = AUM,
    access_token = ACCESS_TOKEN
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

df.to_excel('/Users/your_location/DATA_MAPPING_CODE/LOCAL-TA-CODE-AND-FUND-CLASS-CODE-MAPPING/excel/edl_databrick.xlsx', sheet_name= 'edl_databricks', index=True)


cursor.close()
connection.close()
 