import pandas as pd
import os
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from dotenv import load_dotenv

load_dotenv()

def format_YYYYMMDDHHMMSS(dt: datetime) -> str:
    return dt.strftime('%Y%m%d%H%M%S')


Root_Folder_Path = os.getenv("Root_Folder_Path")
print("Root_Folder_Path: ", Root_Folder_Path)
eFinance_Master_List_File_Name = os.getenv("eFinance_Master_List_File_Name")
eFinance_Master_List_Full_Path = os.path.join(Root_Folder_Path, eFinance_Master_List_File_Name)
print("eFinance_Master_List_Full_Path: ", eFinance_Master_List_Full_Path)

EDL_Master_List_FileName = os.getenv("EDL_Master_List_FileName")
EDL_Master_List_Output_Folder = os.getenv("EDL_Master_List_Output_Folder")
EDL_Master_List_Full_Path = os.path.join(Root_Folder_Path, EDL_Master_List_Output_Folder, EDL_Master_List_FileName)
print("EDL_Master_List_Full_Path:", EDL_Master_List_Full_Path)

now_str = format_YYYYMMDDHHMMSS(datetime.now())
Fund_Code_Mapping_FilePath = os.path.join(Root_Folder_Path, EDL_Master_List_Output_Folder, f"Fund_Code_Mapping_Compare_EDL_eFinance_{now_str}.xlsx")
print("Fund_Code_Mapping_FilePath", Fund_Code_Mapping_FilePath)


df_eFinance_Master = pd.read_excel(eFinance_Master_List_Full_Path)

eFinance_exclude_platform_code_list = [
'BB_GA',
'BB_GA_LIFECO',
'CH_GA',
'CH_INST',
'CH_WOFE',
'HK_ADV',
'HK_GA_INTCOLOAN',
'HK_GA_MFIL',
'HK_GA_MLRL',
'HK_GA_RE',
'HK_INST',
'HK_MPF_APIF1',
'HK_MPF_APIF2',
'HK_MPF_MACAU',
'HK_MPF_ORSO',
'HK_MPF_SC',
'ID_DPLK',
'ID_GA_LIFECO',
'ID_ILP',
'ID_INST',
'JP_GA',
'JP_GA_LIFECO',
'JP_GA_RE',
'JP_LOCAL',
'JP_PRIVATE',
'KH_GA_LIFECO',
'MY_3P',
'MY_GA',
'MY_INST',
'MY_PRS',
'OT_TBD',
'PH_GA',
'PH_ILP',
'PH_ILP_MCBL',
'PH_ILP_MP',
'PH_INST',
'SG_GA_RE',
'SG_ILP',
'SG_INST',
'SG_INST_IR_ICAV',
'SG_MAF',
'SG_MGF',
'SG_OTHER',
'SG_REIT',
'TH_GA_LIFECO',
'TH_MAM',
'TW_GA_LIFECO',
'VN_GA_LIFECO',
'VN_ILP',
'VN_MAM',
'VN_OTHER'
]


df_eFinance_Master



databricks_df = pd.read_excel(EDL_Master_List_Full_Path)

ta_codes = df_eFinance_Master['local_ta_code'].tolist()

databricks_filtered_df = databricks_df[databricks_df['fund_class_code_1'].isin(ta_codes)]

merged_df = pd.merge(df_eFinance_Master, databricks_df, left_on='local_ta_code', right_on='fund_class_code_1', how='left')

mapping_df = merged_df[['local_ta_code', 'fund_class_code_1']]

unique_values = mapping_df.drop_duplicates()

databricks_df.to_excel(Fund_Code_Mapping_FilePath, sheet_name='EDL_Master', index=False)

with pd.ExcelWriter(Fund_Code_Mapping_FilePath, engine='openpyxl', mode = 'a') as writer:
    df_eFinance_Master.to_excel(writer, sheet_name='fund_master_list', index = False)
    mapping_df.to_excel(writer, sheet_name='mapping', index=False)

wb = load_workbook(Fund_Code_Mapping_FilePath)
ws = wb['Compare']

# Filter out eFinance TA scopes
# 4500 Funds -> 2700 have prices
# eFinance_exclude_TA_Scope_list = [
# 'CH_INST',
# 'CH_WOFE',
# 'HK_ADV',
# 'HK_EAGL',
# 'HK_INST',
# 'HK_SCB_CASH',
# 'ID_CAS4TA_PRICES',
# 'ID_DPLK',
# 'ID_ILP',
# 'ID_INST',
# 'JP_GA',
# 'JP_GX',
# 'JP_INST',
# 'JP_LIAISON',
# 'JP_NRI',
# 'JP_PRIVATE',
# 'JP_SUBADVISORY',
# 'MY_ILP',
# 'MY_PRS',
# 'OT_OTHERFUNDS',
# 'PH_GA',
# 'PH_ILP',
# 'PH_INST',
# 'SG_GA',
# 'SG_ILP',
# 'SG_INST',
# 'SG_INST_IR_ICAV',
# 'SG_MAF',
# 'SG_MGF',
# 'SG_OTHER',
# 'SG_REIT',
# 'TH_MAM',
# 'VN_GA',
# 'VN_MAM',
# 'VN_OTHER'
# ]
# Unique key

fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=3, max_col=3):
    for cell in row:
        if cell.value in unique_values['fund_class_code_1'].values:
            cell.fill = fill

wb.save(Fund_Code_Mapping_FilePath)

ws = wb['fund_master_list']
ws['F1'] = 'Comparison Result'
for i in range(2, len(df_eFinance_Master) + 2):
    ws[f'F{i}'] = f'=IFERROR(VLOOKUP(A{i}, mapping!A:B, 2, FALSE), "ZZ")'

wb.save(Fund_Code_Mapping_FilePath)