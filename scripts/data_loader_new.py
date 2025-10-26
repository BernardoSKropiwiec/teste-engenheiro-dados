import os
import pandas as pd
from google.cloud import bigquery
import xmltodict
import json
from bs4 import BeautifulSoup
from io import StringIO

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'/home/bernardog/Documents/repos/teste-engenheiro-dados/psa-data-test-476002-902991aa98ac.json'

client = bigquery.Client()


def load_table_data_from_dataframe(df, table_id):
    """Wrapper to keep load semantics in a single place."""
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

def load_raw_csv(path, table_id):
    df = pd.read_csv(path)
    load_table_data_from_dataframe(df, table_id)

def load_raw_txt(path, table_id):
    df = pd.read_csv(path, delimiter="|")
    load_table_data_from_dataframe(df, table_id)

def load_raw_json(path, table_id):
    # Lê o arquivo inteiro como string, sem normalizar
    with open(path, "r", encoding="utf-8") as f:
        raw_json = f.read()
    df = pd.DataFrame({"conteudo": [raw_json]})
    load_table_data_from_dataframe(df, table_id)

def load_raw_xml(path, table_id):
    with open(path, "r", encoding="utf-8") as f:
        raw_xml = f.read()
    df = pd.DataFrame({"conteudo": [raw_xml]})
    load_table_data_from_dataframe(df, table_id)

def load_raw_html(path, table_id):
    with open(path, "r", encoding="utf-8") as f:
        raw_html = f.read()
    df = pd.DataFrame({"conteudo": [raw_html]})
    load_table_data_from_dataframe(df, table_id)

def load_formated_xml(path, table_id):
    with open(path, 'r', encoding='utf-8') as f:
        data_dict = xmltodict.parse(f.read())

    json_str = json.dumps(data_dict, indent=2, ensure_ascii=False)
    df = pd.DataFrame({"conteudo": [json_str]})
    load_table_data_from_dataframe(df, table_id)


def load_formated_html(path, table_id):
    """Extrai a tabela HTML e carrega o DataFrame resultante no BigQuery."""
    with open(path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    table = soup.find("table")
    if table is None:
        raise ValueError(f"Nenhuma tabela encontrada em {path}.")

    #Transformação dos dados da primeira tabela do arquivo para um DataFrame.
    df = pd.read_html(StringIO(str(table)))[0]

    #Mapeamento de nomes, para não dar conflito durante a carga
    column_map = {
        "ID Log": "id_log",
        "Timestamp": "log_time",
        "Usuário": "usuario",
        "Ação": "acao",
        "Entidade Afetada": "entidade_afetada",
        "Resultado": "resultado",
        "IP": "ip",
    }
    df = df.rename(columns=column_map)

    missing_cols = set(column_map.values()) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Colunas ausentes no HTML: {', '.join(sorted(missing_cols))}")

    df["log_time"] = pd.to_datetime(df["log_time"], errors="coerce")
    if df["log_time"].isna().any():
        raise ValueError("Valores inválidos na coluna log_time")

    load_table_data_from_dataframe(df, table_id)
    
  

if __name__ == "__main__":
    load_raw_csv("dados/dados_clientes.csv", "psa_raw.clientes")
    load_raw_txt("dados/analises_tributarias.txt", "psa_raw.analises_tributaria")
    load_raw_json("dados/tarefas_projetos.json", "psa_raw.tarefas_projetos")
    load_raw_xml("dados/notas_fiscais.xml", "psa_raw.notas_fiscais")
    load_raw_html("dados/logs_sistema.html", "psa_raw.logs_sistema")
    load_formated_xml("dados/notas_fiscais.xml", "psa_raw.notas_fiscais_json")
    load_formated_html("dados/logs_sistema.html", "psa_raw.logs_sistema_table")
