import pandas as pd
from sqlalchemy import create_engine, types
import pdfplumber

def find_average_per_char(word):
    sum = 0
    non_number_counter = 0
    if word is None:
        return 10000
    
    for char in word:
        if char != "." or char != ",":
            if char == "-":
                sum += ord(char) + 10
            else:
                sum += ord(char)
        else:
            non_number_counter += 1

    if non_number_counter == len(word):
        return 50

    return sum/(len(word) - non_number_counter)


def find_header_index(df):
    column = 0

    for n_cols, col in enumerate(df.columns):
        for row in reversed(list(range(0, len(df[col])))):
            item = df.iloc[row, n_cols]
            aux = find_average_per_char(item)
            if aux <= 57 and aux >= 48:
                column = col
    
    for row in reversed(list(range(0, len(df[column])))):
        aux = find_average_per_char(df[col][row])
        if aux > 57 or aux < 48:
            return column, row + 1
    
    if find_average_per_char(column) > 57 or find_average_per_char(column) < 48:
        return column, 0
    
    return column, -1

def concatenate_dataframes(df1, df2):
    num_cols_df1 = len(df1.columns)
    num_cols_df2 = len(df2.columns)
    
    if num_cols_df2 < num_cols_df1:
        for i in range(num_cols_df2, num_cols_df1):
            df2[f"temp_column_{i}"] = None
    elif num_cols_df2 > num_cols_df1:
        df2 = df2.iloc[:, :num_cols_df1]
    
    df2.columns = df1.columns
    
    return pd.concat([df1, df2], ignore_index=True)


def extrair_dados_e_inserir_no_banco(pdf_file):
    column_prefix = "column"
    header_index = []

    with pdfplumber.open(pdf_file) as pdf:
        all_tables = []
        for page in pdf.pages:
            page_tables = page.extract_tables()
            for table in page_tables:
                column_names = table[0]
                column_names = [f"{column_prefix}_{i}" if cn is None else cn for i, cn in enumerate(column_names)]
                df_table = pd.DataFrame(table[1:], columns=column_names).reset_index(drop=True)
                all_tables.append(df_table)

    last_valid_df = None
    updated_tables = []

    for df in all_tables:
        col, row = find_header_index(df=df)
        if row != -1:
            last_valid_df = df
            updated_tables.append(df)
        else:
            if last_valid_df is not None:
                concatenated_df = concatenate_dataframes(last_valid_df, df)
                updated_tables[-1] = concatenated_df
                last_valid_df = concatenated_df
            else:
                updated_tables.append(df)

    all_tables = updated_tables

    for i, df_table in enumerate(all_tables, start=1):
        df_table.columns = df_table.columns.str.lower()

        column_types = df_table.dtypes.apply(lambda x: x.name).to_dict()
        pg_types = {
            'object': types.String,  
            'int64': types.Integer,
            'float64': types.Float,
            'bool': types.Boolean,
            'datetime64[ns]': types.DateTime,
        }
        column_types_pg = {col: pg_types.get(dtype, types.String) for col, dtype in column_types.items()}
        engine = create_engine('postgresql+psycopg2://postgres:wladimir@localhost:5432/testpdf')
        df_table.to_sql(f'teste_{i}', engine, index=False, if_exists='replace', method='multi', dtype=column_types_pg)
