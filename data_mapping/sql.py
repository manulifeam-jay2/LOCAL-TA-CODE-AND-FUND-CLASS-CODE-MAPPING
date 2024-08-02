import os
import pandas as pd
from databricks import sql

HOST = ''
AUM = ''
ACCESS_TOKEN = ''

connection = sql.connect(
    server_hostname = HOST,
    http_path = AUM,
    access_token = ACCESS_TOKEN
)

cursor = connection.cursor()
# cursor1 = connection.cursor()

cursor.execute(
"""
WITH RankedData AS (
    SELECT
        effective_date,
        fund_class_code_1,
        liability_portf_code,
        liability_portf_name,
        country_of_domicile_code,
        ROW_NUMBER() OVER (PARTITION BY liability_portf_name, fund_class_code_1, country_of_domicile_code ORDER BY effective_date DESC) AS rn
    FROM
        `hive_metastore`.`inv_dal_eod`.`cube_portfolios`
    WHERE
        (fund_class_code_1 IS NOT NULL) AND (liability_portf_name IS NOT NULL) AND (country_of_domicile_code NOT IN ('US', 'CA'))
)
SELECT
    fund_class_code_1,
    liability_portf_code,
    liability_portf_name,
    country_of_domicile_code,
    effective_date
FROM
    RankedData
WHERE
    rn = 1
ORDER BY
    liability_portf_name, fund_class_code_1, country_of_domicile_code;
"""
)

result = cursor.fetchall()
# result1 = cursor1.fetchall()
# print("Show result", result)

df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
df['effective_date'] = pd.to_datetime(df['effective_date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
# df1 = pd.DataFrame(result1, columns=[desc[0] for desc in cursor1.description])

df.to_excel('/Users/urakodz/Downloads/DATA_MAPPING_CODE/LOCAL-TA-CODE-AND-FUND-CLASS-CODE-MAPPING/excel/edl_databricks.xlsx', index=True)


cursor.close()
connection.close()
 