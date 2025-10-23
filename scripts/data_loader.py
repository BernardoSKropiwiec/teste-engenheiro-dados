import os
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



def run_pipeline():
    loader = BigQueryLoader()
    
    csv_extractor = CSVExtractor()
    txt_extractor = TXTExtractor()

    csv_transformer = ClienteTransformer()
    txt_transformer = AnaliseTransformer()

    csv_df_raw = csv_extractor.read("dados/dados_clientes.csv")
    txt_df_raw = txt_extractor.read("dados/analises_tributarias.txt")

    csv_df_clean = csv_transformer.transform(csv_df_raw)
    txt_df_clean = txt_transformer.transform(txt_df_raw)

    loader.load(csv_df_clean, "psa_raw.cliente")
    loader.load(txt_df_clean, "psa_raw.analise")
if __name__ == "__main__":
    run_pipeline()

