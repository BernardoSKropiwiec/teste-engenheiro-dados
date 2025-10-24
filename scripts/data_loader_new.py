import os
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\BERNARDOKROPIWIEC\Documents\REPOS\Testes\teste-engenheiro-dados\psa-data-test-476002-902991aa98ac.json'

client = bigquery.Client()

def load_raw_csv(path, table_id):
    df = pd.read_csv(path)
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

def load_raw_txt(path, table_id):
    df = pd.read_csv(path, delimiter="|")
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

def load_raw_json(path, table_id):
    # Lê o arquivo inteiro como string, sem normalizar
    with open(path, "r", encoding="utf-8") as f:
        raw_json = f.read()
    df = pd.DataFrame({"conteudo": [raw_json]})
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

def load_raw_xml(path, table_id):
    with open(path, "r", encoding="utf-8") as f:
        raw_xml = f.read()
    df = pd.DataFrame({"conteudo": [raw_xml]})
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

def load_raw_html(path, table_id):
    with open(path, "r", encoding="utf-8") as f:
        raw_html = f.read()
    df = pd.DataFrame({"conteudo": [raw_html]})
    client.load_table_from_dataframe(df, table_id).result()
    print(f"{table_id} carregada.")

if __name__ == "__main__":
    load_raw_csv("dados/dados_clientes.csv", "psa_raw_test.cliente")
    load_raw_txt("dados/analises_tributarias.txt", "psa_raw_test.analises_tributarias")
    load_raw_json("dados/tarefas_projetos.json", "psa_raw_test.tarefas_projetos")
    load_raw_xml("dados/notas_fiscais.xml", "psa_raw_test.notas_fiscais")
    load_raw_html("dados/logs_sistema.html", "psa_raw_test.logs_sistema")
