"""Aplicação FastAPI para expor as views analytics da PSA.

A API se conecta ao BigQuery utilizando as credenciais configuradas no ambiente
(`GOOGLE_APPLICATION_CREDENTIALS`) e oferece endpoints somente leitura para
consultar dados da camada Gold (`psa_analytics`).
"""

from __future__ import annotations

import os
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from typing import Any, Iterable

from fastapi import FastAPI, HTTPException, Query
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import bigquery
from google.cloud.bigquery.table import Row


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "dados/psa-data-test-476002-902991aa98ac.json"
)

DEFAULT_PROJECT_ID = "psa-data-test-476002"
DEFAULT_ANALYTICS_DATASET = "psa_analytics"
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
DEFAULT_QUERY_TIMEOUT = 120  # seconds

ANALYTICS_VIEWS: tuple[str, ...] = (
    "vw_anl_analise_tributaria",
    "vw_anl_nota_fiscal",
    "vw_anl_projetos",
    "vw_snt_projetos_performance",
    "vw_snt_analises_tributarias",
    "vw_snt_notas_fiscais",
)


app = FastAPI(
    title="PSA Data API",
    version="1.0.0",
    description=(
        "API para consulta das views analíticas (`psa_analytics`) tratadas."
    ),
)


@lru_cache
# Cliente BigQuery reutilizável (cacheado entre as requisições da API).
def get_bigquery_client() -> bigquery.Client:
    """Cria uma instância singleton do cliente BigQuery."""
    return bigquery.Client()


def get_project_id() -> str:
    """Retorna o ID do projeto configurado para os datasets da PSA."""
    return os.getenv("PSA_PROJECT_ID", DEFAULT_PROJECT_ID)


def get_analytics_dataset() -> str:
    """Retorna o nome do dataset referente à camada analytics."""
    return os.getenv("PSA_ANALYTICS_DATASET", DEFAULT_ANALYTICS_DATASET)


def _qualify_name(dataset: str, resource: str) -> str:
    """Monta o identificador totalmente qualificado no BigQuery."""
    project = get_project_id()
    return f"{project}.{dataset}.{resource}"


# Normaliza valores retornados pelo BigQuery para formatos compatíveis com JSON.
def _coerce_value(value: Any) -> Any:
    """Converte valores do BigQuery para tipos compatíveis com JSON."""
    if isinstance(value, Decimal):
        # Keep integers without decimal places as int, otherwise cast to float.
        if value == value.to_integral():
            return int(value)
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Row):
        return {key: _coerce_value(value[key]) for key in value.keys()}
    if isinstance(value, dict):
        return {key: _coerce_value(val) for key, val in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [_coerce_value(item) for item in value]
    return value


def _execute_query(query: str) -> list[dict[str, Any]]:
    """Executa uma consulta SQL no BigQuery e retorna linhas como dicionários."""
    client = get_bigquery_client()
    job = client.query(query)
    rows = job.result(timeout=DEFAULT_QUERY_TIMEOUT)
    return [{key: _coerce_value(row[key]) for key in row.keys()} for row in rows]


@app.get("/health", summary="Health check")
def health_check() -> dict[str, str]:
    """Endpoint de saúde utilizado para monitoramento."""
    return {"status": "ok"}


@app.get(
    "/analytics",
    summary="Listar views analytics",
    response_description="Lista de views disponíveis na camada analytics.",
)
def list_analytics_views() -> dict[str, Any]:
    """Lista as views analytics disponibilizadas pela API."""
    return {
        "dataset": get_analytics_dataset(),
        "views": sorted(ANALYTICS_VIEWS),
    }


# Endpoints para listar e consultar as views analíticas da camada gold.
@app.get(
    "/analytics/{view_name}",
    summary="Consultar view analytics",
    response_description="Dados retornados da view solicitada.",
)
def get_analytics_view(
    view_name: str,
    limit: int = Query(
        default=DEFAULT_LIMIT,
        ge=1,
        le=MAX_LIMIT,
        description="Quantidade máxima de registros retornados.",
    ),
) -> dict[str, Any]:
    """Retorna registros de uma view analytics com limite configurável."""
    view_name = view_name.lower()
    if view_name not in ANALYTICS_VIEWS:
        raise HTTPException(
            status_code=404,
            detail=f"View '{view_name}' não está disponível via API.",
        )

    dataset = get_analytics_dataset()
    qualified_name = _qualify_name(dataset, view_name)
    query = f"SELECT * FROM `{qualified_name}` LIMIT {limit}"

    print(query)

    try:
        rows = _execute_query(query)
    except NotFound as exc:
        raise HTTPException(
            status_code=404,
            detail=f"View '{qualified_name}' não encontrada no BigQuery.",
        ) from exc
    except GoogleAPICallError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Erro ao consultar BigQuery: {exc.message or str(exc)}",
        ) from exc
    except Exception as exc:  # pragma: no cover - fallback para erros inesperados
        raise HTTPException(
            status_code=500,
            detail="Erro inesperado ao consultar BigQuery.",
        ) from exc

    return {
        "dataset": dataset,
        "view": view_name,
        "limit": limit,
        "rows": rows,
    }
