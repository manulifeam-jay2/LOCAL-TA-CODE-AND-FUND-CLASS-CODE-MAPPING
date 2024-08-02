import os
import subprocess
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import pytz

excel_file_path = ''
fund_master_list_path = ''
sql_script_path = ''
output_result_path = ''

if not os.path.exists(excel_file_path):
    print(f"{excel_file_path} does not exist. Running {sql_script_path} to generate the file")
    try:
        subprocess.run(['python', sql_script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to run {sql_script_path}: {e}")
        exit(1)

fund_master_list_df = pd.read_excel(fund_master_list_path)
databricks_df = pd.read_excel(excel_file_path)
ta_codes = fund_master_list_df['local_ta_code'].tolist()

databricks_filtered_df = databricks_df[databricks_df['fund_class_code_1'].isin(ta_codes)]

merged_df = pd.merge(fund_master_list_df, databricks_df, left_on ='local_ta_code', right_on='fund_class_code_1', how='left')

mapping_df = merged_df[['local_ta_code', 'fund_class_code_1']]

unique_values = mapping_df.drop_duplicates()

databricks_df.to_excel(output_result_path, sheet_name='EDL_TABLE', index=False)

with pd.ExcelWriter(output_result_path, engine='openpyxl', mode = 'a') as writer:
    fund_master_list_df.to_excel(writer, sheet_name='fund_master_list', index = False)
    mapping_df.to_excel(writer, sheet_name='mapping', index=False)

wb = load_workbook(output_result_path)
ws = wb['mapping']

fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=3, max_col=3):
    for cell in row:
        if cell.value in unique_values['fund_class_code_1'].values:
            cell.fill = fill

wb.save(output_result_path)
ws = wb['fund_master_list']

ws['F1'] = 'Comparison Result'
for i in range(2, len(fund_master_list_df) + 2):
    ws[f'F{i}'] = f'=IFERROR(VLOOKUP(A{i}, mapping!A:B, 2, FALSE), "ZZ")'

wb.save(output_result_path)