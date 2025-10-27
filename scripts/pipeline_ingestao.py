import csv
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import xmltodict
from bs4 import BeautifulSoup
from google.cloud import bigquery


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "dados/psa-data-test-476002-902991aa98ac.json"
)


client = bigquery.Client()
LOG_FILE = Path(__file__).resolve().parent.parent / "logs" / "pipeline_ingestao.csv"
DDL_SCRIPT = Path(__file__).resolve().parent.parent / "sql" / "ddl_bronze_scripts.sql"
BQ_TIMEOUT = 300  # seconds


class BigQueryLoader:
    """Wrapper da função, para facilitar o logging"""

    def __init__(self, client_obj: bigquery.Client, timeout: int = BQ_TIMEOUT):
        self.client = client_obj
        self.timeout = timeout

    def load_table_data_from_dataframe(
        self, df: pd.DataFrame, table_id: str
    ) -> None:
        self.client.load_table_from_dataframe(
            df, table_id, timeout=self.timeout
        ).result()
        print(f"{table_id} carregada.")


class PipelineLogger:
    """Função para gerar um arquivo de logs da carga dos dados."""

    def __init__(self, log_file: Path):
        self.log_file = log_file

    def append_log(
        self, operation: str, table_id: str, status: str, message: str
    ) -> None:
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self.log_file.exists()
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": operation,
            "table_id": table_id,
            "status": status,
            "message": message,
        }
        with self.log_file.open("a", newline="", encoding="utf-8") as fp:
            writer = csv.DictWriter(fp, fieldnames=record.keys())
            if write_header:
                writer.writeheader()
            writer.writerow(record)


class BaseFileLoader(ABC):
    operation_name = "base_loader"

    def __init__(self, source_path: str, table_id: str):
        self.source_path = Path(source_path)
        self.table_id = table_id

    @abstractmethod
    def build_dataframe(self) -> pd.DataFrame:
        raise NotImplementedError

    def load(self, uploader: BigQueryLoader) -> None:
        df = self.build_dataframe()
        uploader.load_table_data_from_dataframe(df, self.table_id)


class RawCSVLoader(BaseFileLoader):
    operation_name = "load_raw_csv"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo inteiro, sem transformações
        return pd.read_csv(self.source_path)


class RawTXTLoader(BaseFileLoader):
    operation_name = "load_raw_txt"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo inteiro, sem transformações
        return pd.read_csv(self.source_path, delimiter="|")


class RawJSONLoader(BaseFileLoader):
    operation_name = "load_raw_json"

    def build_dataframe(self) -> pd.DataFrame:
        # Lê o arquivo inteiro como string, sem normalizar
        with self.source_path.open("r", encoding="utf-8") as file_handler:
            raw_json = file_handler.read()
        return pd.DataFrame({"conteudo": [raw_json]})


class RawXMLLoader(BaseFileLoader):
    operation_name = "load_raw_xml"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo inteiro, sem transformações
        with self.source_path.open("r", encoding="utf-8") as file_handler:
            raw_xml = file_handler.read()
        return pd.DataFrame({"conteudo": [raw_xml]})


class RawHTMLLoader(BaseFileLoader):
    operation_name = "load_raw_html"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo inteiro, sem transformações
        with self.source_path.open("r", encoding="utf-8") as file_handler:
            raw_html = file_handler.read()
        return pd.DataFrame({"conteudo": [raw_html]})


class FormattedXMLLoader(BaseFileLoader):
    operation_name = "load_formated_xml"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo xml, transformando ele em JSON antes de carregar.
        with self.source_path.open("r", encoding="utf-8") as file_handler:
            data_dict = xmltodict.parse(file_handler.read())
        json_str = json.dumps(data_dict, indent=2, ensure_ascii=False)
        return pd.DataFrame({"conteudo": [json_str]})


class FormattedHTMLLoader(BaseFileLoader):
    operation_name = "load_formated_html"

    def build_dataframe(self) -> pd.DataFrame:
        #Carregamento do arquivo xml, transformando ele em JSON antes de carregar.
        with self.source_path.open("r", encoding="utf-8") as file_handler:
            soup = BeautifulSoup(file_handler, "html.parser")

        table = soup.find("table")
        if table is None:
            raise ValueError(f"Nenhuma tabela encontrada em {self.source_path}.")

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
            raise ValueError(
                f"Colunas ausentes no HTML: {', '.join(sorted(missing_cols))}"
            )

        df["log_time"] = pd.to_datetime(df["log_time"], errors="coerce")
        if df["log_time"].isna().any():
            raise ValueError("Valores inválidos na coluna log_time")

        return df


class PipelineRunner:
    """Função para rodar o loader, usando try except para tratar e logar os erros."""

    def __init__(
        self,
        loaders: list[BaseFileLoader],
        uploader: BigQueryLoader,
        logger: PipelineLogger,
    ):
        self.loaders = loaders
        self.uploader = uploader
        self.logger = logger

    def run(self) -> None:
        for loader in self.loaders:
            self._run_loader(loader)

    def _run_loader(self, loader: BaseFileLoader) -> None:
        operation = loader.operation_name
        source_path = str(loader.source_path)
        table_id = loader.table_id
        self.logger.append_log(operation, table_id, "START", f"Lendo {source_path}")
        try:
            loader.load(self.uploader)
        except Exception as exc:  # keep pipeline running
            msg = f"Falha ao processar {source_path}: {exc}"
            self.logger.append_log(operation, table_id, "ERROR", msg)
            print(msg)
        else:
            self.logger.append_log(
                operation,
                table_id,
                "SUCCESS",
                f"{table_id} carregada com sucesso",
            )


def run_ddl_script(
    client_obj: bigquery.Client,
    script_path: Path,
    logger: PipelineLogger,
) -> None:
    """Executa o script de DDL antes das cargas para garantir as tabelas."""

    operation = "run_ddl_script"
    table_id = script_path.name
    if not script_path.exists():
        msg = f"Arquivo de DDL não encontrado: {script_path}"
        logger.append_log(operation, table_id, "ERROR", msg)
        raise FileNotFoundError(msg)

    logger.append_log(
        operation,
        table_id,
        "START",
        f"Executando script {script_path}",
    )

    try:
        ddl_sql = script_path.read_text(encoding="utf-8")
        client_obj.query(ddl_sql).result(timeout=BQ_TIMEOUT)
    except Exception as exc:
        msg = f"Falha ao executar DDL {script_path}: {exc}"
        logger.append_log(operation, table_id, "ERROR", msg)
        raise
    else:
        logger.append_log(
            operation,
            table_id,
            "SUCCESS",
            f"Script {script_path} executado com sucesso",
        )


def main():
    uploader = BigQueryLoader(client)
    logger = PipelineLogger(LOG_FILE)
    run_ddl_script(client, DDL_SCRIPT, logger)
    loaders: list[BaseFileLoader] = [
        RawCSVLoader("dados/dados_clientes.csv", "psa_raw.clientes"),
        RawTXTLoader("dados/analises_tributarias.txt", "psa_raw.analises_tributarias"),
        RawJSONLoader("dados/tarefas_projetos.json", "psa_raw.tarefas_projetos"),
        RawXMLLoader("dados/notas_fiscais.xml", "psa_raw.notas_fiscais"),
        RawHTMLLoader("dados/logs_sistema.html", "psa_raw.logs_sistema"),
        FormattedXMLLoader("dados/notas_fiscais.xml", "psa_raw.notas_fiscais_json"),
        FormattedHTMLLoader("dados/logs_sistema.html", "psa_raw.logs_sistema_table"),
    ]
    pipeline = PipelineRunner(loaders, uploader, logger)
    pipeline.run()


if __name__ == "__main__":
    main()
