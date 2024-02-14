import pandas as pd
from sqlalchemy import create_engine, types
import pdfplumber

with pdfplumber.open("ArquivoTeste2.pdf") as pdf:

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

############################################################################################################
import os
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

db_uri = "postgresql+psycopg2://postgres:wladimir@localhost:5432/Testes_Python_GPT"
db = SQLDatabase.from_uri(db_uri)

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

# Defina um sistema personalizado que inclui informações sobre as tabelas
system_template = """Você é responsável por buscar no banco de dados as informações necessárias para que outra IA gere gráficos que satisfaçam a sequinte requisição do usuário: {input}.

Você tem acesso às seguintes do banco de dados: {tabelas}

Retorne apenas o conteúdo do banco de dados que está relacionado com a requisição do usuário, não crie novas informações. Garanta que todas as linhas e colunas relacionadas estarão na sua resposta.

Se a pergunta não parecer relacionada ao banco de dados, apenas retorne "Dados não disponíveis" como resposta.
"""
# Crie um template de prompt usando o sistema personalizado
prompt_template = ChatPromptTemplate.from_messages([
    ("system", system_template),
    ("human", "{input} {tabelas}"),
    MessagesPlaceholder("agent_scratchpad"),
])

# Crie o agente com o novo template de prompt
agent = create_sql_agent(
    llm=llm,
    db=db,
    prompt=prompt_template,
    agent_type="openai-tools",
    verbose=True,
)

requisica_usuario = "Quero um gráfico com o maior valor de cada moeda."
tabelas = db.get_usable_table_names()

agent_data = agent.invoke({"input": requisica_usuario, "tabelas": tabelas})

context = agent_data['output']

print(context)
print("----------Fim da extração das tabelas------------")