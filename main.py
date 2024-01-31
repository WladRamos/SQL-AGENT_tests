import pandas as pd
from sqlalchemy import create_engine, types
import pdfplumber

with pdfplumber.open("2015-08.pdf") as pdf:

    all_tables = []
    for page in pdf.pages:

        page_tables = page.extract_tables()
        
        if page_tables:

            column_names = page_tables[0][0]
            
            all_tables.extend([pd.DataFrame(table[1:], columns=column_names) for table in page_tables])

df = pd.concat(all_tables)

df.columns = df.columns.str.lower()

column_types = df.dtypes.apply(lambda x: x.name).to_dict()

pg_types = {
    'object': types.String,  
    'int64': types.Integer,
    'float64': types.Float,
    'bool': types.Boolean,
    'datetime64[ns]': types.DateTime,
}

column_types_pg = {col: pg_types.get(dtype, types.String) for col, dtype in column_types.items()}

print("Tipos de dados definidos dinamicamente:")
print(column_types_pg)

engine = create_engine('postgresql+psycopg2://postgres:wladimir@localhost:5432/Testes_Python_GPT')

df.to_sql('nanonets_test', engine, index=False, if_exists='replace', method='multi', dtype=column_types_pg)

