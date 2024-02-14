from extracao import extrair_dados_e_inserir_no_banco
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import os
from langchain_core.output_parsers import StrOutputParser

def _sanitize_output(text: str):
    _, after = text.split("```python")
    return after.split("```")[0]

def create_openai_agent(db_uri, db):
    os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
    db = SQLDatabase.from_uri(db_uri)
    llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
    
    system_template = """Você é responsável por buscar no banco de dados as informações necessárias para que outra IA gerar gráficos que satisfaçam a sequinte requisição do usuário: {input}.

    Você tem acesso às seguintes do banco de dados: {tabelas}

    Retorne apenas o conteúdo do banco de dados que está relacionado com a requisição do usuário, não crie novas informações. Garanta que todas as linhas e colunas relacionadas estarão na sua resposta.

    Se a pergunta não parecer relacionada ao banco de dados, apenas retorne "Dados não disponíveis" como resposta.
    """
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", system_template),
        ("human", "{input} {tabelas}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])
    
    agent = create_sql_agent(
        llm=llm,
        db=db,
        prompt=prompt_template,
        agent_type="openai-tools",
        verbose=True,
    )
    return agent

def generate_plotly_code(requisicao_usuario, context):
    llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)
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
    code = chain.invoke({"input": requisicao_usuario, "context": context})
    return code

def gera_graficos_e_executa(requisicao_usuario):
    db_uri = 'postgresql+psycopg2://postgres:wladimir@localhost:5432/Testes_Python_GPT'
    extrair_dados_e_inserir_no_banco("ArquivoTeste2.pdf")
    agent = create_openai_agent(db_uri)
    requisicao_usuario = "Quero um gráfico sobre a variação do dólar."
    db = SQLDatabase.from_uri(db_uri)
    tabelas = db.get_usable_table_names()
    agent_data = agent.invoke({"input": requisicao_usuario, "tabelas": tabelas})
    context = agent_data['output']
    code = generate_plotly_code(requisicao_usuario, context)
    print(code)


