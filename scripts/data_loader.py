import os
import json
from google.cloud import bigquery
import pandas as pd 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\BERNARDOKROPIWIEC\Documents\REPOS\Testes\teste-engenheiro-dados\psa-data-test-476002-902991aa98ac.json'


class CSVExtractor:
    def read(self, path: str) -> pd.DataFrame:
        #Lê o CSV e retorna um DataFrame bruto.
        return pd.read_csv(path)
    
class TXTExtractor:
    def read(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, delimiter="|")
    
class JSONExtractor:
    def read(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


class AnaliseTransformer:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida e limpa o DataFrame."""
        # Remove linhas com id_cliente nulo
        df = df.dropna(subset=["id_analise","cliente_id"])
        return df
    
class ClienteTransformer:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida e limpa o DataFrame."""
        # Remove linhas com id_cliente nulo
        df = df.dropna(subset=["id_cliente"])
        # Converte data para formato ISO
        df["data_cadastro"] = pd.to_datetime(df["data_cadastro"], errors="coerce").dt.date
        return df
    
class BigQueryLoader:
    def __init__(self):
        self.client = bigquery.Client()

    def load(self, df, table_id: str):
        job = self.client.load_table_from_dataframe(df, table_id)
        job.result()  # Espera o job finalizar
        print(f"Tabela {table_id} carregada com sucesso.")


class ProjetoTransformer:
    def transform(self, data: dict) -> pd.DataFrame:
        projetos = data.get("projetos", [])
        if not projetos:
            return pd.DataFrame()
        df = pd.json_normalize(
            projetos,
            sep="_",
        )
        if "tarefas" in df.columns:
            df = df.drop(columns=["tarefas"])
        df["data_inicio"] = pd.to_datetime(df["data_inicio"], errors="coerce").dt.date
        df["data_prevista_fim"] = pd.to_datetime(df["data_prevista_fim"], errors="coerce").dt.date
        for col in ["horas_totais_estimadas", "horas_totais_realizadas"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        if "valor_projeto" in df.columns:
            df["valor_projeto"] = pd.to_numeric(df["valor_projeto"], errors="coerce")
        df = df.dropna(subset=["id_projeto"])
        return df


class TarefaTransformer:
    def transform(self, data: dict) -> pd.DataFrame:
        registros = []
        for projeto in data.get("projetos", []):
            pid = projeto.get("id_projeto")
            for tarefa in projeto.get("tarefas", []):
                registro = {**tarefa, "id_projeto": pid}
                registros.append(registro)
        if not registros:
            return pd.DataFrame()
        df = pd.DataFrame(registros)
        for col in ["data_inicio", "data_prevista_fim"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in ["horas_estimadas", "horas_realizadas"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        df = df.dropna(subset=["id_tarefa", "id_projeto"])
        return df



def run_pipeline():
    loader = BigQueryLoader()
    
    csv_extractor = CSVExtractor()
    txt_extractor = TXTExtractor()
    json_extractor = JSONExtractor()

    csv_transformer = ClienteTransformer()
    txt_transformer = AnaliseTransformer()
    projeto_transformer = ProjetoTransformer()
    tarefa_transformer = TarefaTransformer()

    csv_df_raw = csv_extractor.read("dados/dados_clientes.csv")
    txt_df_raw = txt_extractor.read("dados/analises_tributarias.txt")
    json_raw = json_extractor.read("dados/tarefas_projetos.json")

    csv_df_clean = csv_transformer.transform(csv_df_raw)
    txt_df_clean = txt_transformer.transform(txt_df_raw)
    df_projetos = projeto_transformer.transform(json_raw)
    df_tarefas = tarefa_transformer.transform(json_raw)

    loader.load(csv_df_clean, "psa_raw.cliente")
    loader.load(txt_df_clean, "psa_raw.analise")
    if not df_projetos.empty:
        loader.load(df_projetos, "psa_raw.projeto")
    if not df_tarefas.empty:
        loader.load(df_tarefas, "psa_raw.tarefa")
if __name__ == "__main__":
    run_pipeline()

