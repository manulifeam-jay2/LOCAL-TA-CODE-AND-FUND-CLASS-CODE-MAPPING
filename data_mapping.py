import pandas as pd

def load_fund_master_list(file_path):
    fund_master_list = pd.read_excel(file_path, header = 6)
    return fund_master_list

def load_edl(file_path):
    edl = pd.read_excel(file_path)
    return edl

def combine_local_ta_code_and_platform_country(df):
    df['combined_key'] = df['local_ta_code'].astype(str) + '_' + df['platform_country'].astype(str)
    return df

def get_unique_combined_keys(df):
    return df['combined_key'].unique()

def write_unique_keys_to_excel(unique_keys, file_path):
    book = pd.read_excel(file_path)

    unique_keys_df = pd.DataFrame(unique_keys, columns=['unique_combined_keys'])
    with pd.ExcelWriter(file_path, engine='openpyxl', mode = 'a', if_sheet_exists='overlay') as writer:
        start_col = len(book.columns)
        unique_keys_df.to_excel(writer, sheet_name='Funds', startcol=start_col, index = False, header=True)

def delete_columns_from_excel(file_path, columns_to_delete):
    df = pd.read_excel(file_path, header = 6)

if __name__ == "__main__":

    fund_master_list_path = '/Users/urakodz/Downloads/TASK1_DATA_MAPPING/eDataMart.20240704-1514.Master1.xlsx'
    edl_path = '/Users/urakodz/Downloads/TASK1_DATA_MAPPING/Data_mapping.xlsx'

    fund_master_list = load_fund_master_list(fund_master_list_path)
    edl = load_edl(edl_path)

    fund_master_list = combine_local_ta_code_and_platform_country(fund_master_list)

    unique_keys = get_unique_combined_keys(fund_master_list)

    print(unique_keys)

    write_unique_keys_to_excel(unique_keys, fund_master_list_path)
    print(f"unique_keys column written to {fund_master_list_path}")