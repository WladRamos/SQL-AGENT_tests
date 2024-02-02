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
system_template = """Você é responsável por buscar no banco de dados as informações necessárias para que outra IA gerar gráficos que satisfaçam a sequinte requisição do usuário: {input}.

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

requisica_usuario = "Quero um gráfico sobre a variação do dólar."
tabelas = db.get_usable_table_names()

agent_data = agent.invoke({"input": requisica_usuario, "tabelas": tabelas})

context = agent_data['output']

print(context)
print("----------Fim da extração das tabelas------------")
#########################################################################################################
from langchain_core.output_parsers import StrOutputParser
def _sanitize_output(text: str):
    _, after = text.split("```python")
    return after.split("```")[0]
prompt = ChatPromptTemplate.from_template("""Você é um cientista de dados experiente e com amplo conhecimento as bibliotecas pandas, matplotlib e plotly, e terá como tarefa gerar códigos python usando o plotly para gerar gráficos a partir do conteúdo a seguir: 
    <context>
    {context}
    </context>

    Requisição do usuário: {input}

    OBSERVAÇÃO: Para todo gráfico gerado, você deve armazenar a conversão para json na lista graficos, assim como no exemplo a seguir
    
    Return only python code in Markdown format, e.g.:
    ```python
    tabela1 = [['categorias', 'vendas'], ['eletronicos', '120'], ['roupas', '85'], ['livros', '50'], ['brinquedos', '40'], ['esportes', '90']]
        categorias = [item[0] for item in tabela1[1:]]  # Extrai as categorias
        vendas = [int(item[1]) for item in tabela1[1:]]  # Extrai os valores de vendas como inteiros
        trace = go.Bar(
            x=categorias,
            y=vendas,
            marker=dict(color='rgb(26, 118, 255)')
        )
        layout = go.Layout(title='Vendas por Categoria', xaxis=dict(title='Categorias'), yaxis=dict(title='Vendas'))
        fig1 = go.Figure(data=[trace], layout=layout)
        graficos.append(fig1.to_json())
    ```""")

chain = prompt | llm | StrOutputParser() | _sanitize_output
code = chain.invoke({"input": requisica_usuario, "context": context})
print(code)