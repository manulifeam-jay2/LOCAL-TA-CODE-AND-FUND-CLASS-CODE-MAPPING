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
print(fp)
print(ACCESS_TOKEN)
print(HOST)
print(AUM)
