#!/usr/bin/env python3
"""Data quality checks for the psa_curated (Silver) layer."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional

from google.cloud import bigquery

DEFAULT_PROJECT_ID = "psa-data-test-476002"
DEFAULT_DATASET = "psa_curated"


@dataclass
class DataQualityCheck:
    name: str
    description: str
    severity: str
    query: str


@dataclass
class CheckResult:
    name: str
    description: str
    severity: str
    failures: Optional[int]
    sample: Optional[str]
    status: str
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "severity": self.severity,
            "failures": self.failures,
            "sample": self.sample,
            "status": self.status,
            "error": self.error,
        }


def table_ref(project_id: str, dataset: str, table_name: str) -> str:
    return f"`{project_id}.{dataset}.{table_name}`"


def build_checks(project_id: str, dataset: str) -> List[DataQualityCheck]:
    cliente = table_ref(project_id, dataset, "cliente")
    analise = table_ref(project_id, dataset, "analise_tributaria")
    projeto = table_ref(project_id, dataset, "projeto")
    tarefa = table_ref(project_id, dataset, "tarefa")
    nota = table_ref(project_id, dataset, "nota_fiscal")
    nota_imposto = table_ref(project_id, dataset, "nota_fiscal_imposto")
    nota_item = table_ref(project_id, dataset, "nota_fiscal_item")

    return [
        DataQualityCheck(
            name="cliente_required_fields",
            description="Campos obrigatórios e formato de CNPJ em cliente",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_cliente AS STRING), ', ' LIMIT 5) AS sample
                FROM {cliente}
                WHERE id_cliente IS NULL
                  OR razao_social IS NULL
                  OR cnpj IS NULL
                  OR REGEXP_CONTAINS(cnpj, r'[^0-9]')
                  OR LENGTH(cnpj) != 14
                  OR estado IS NULL
                  OR data_cadastro IS NULL
            """,
        ),
        DataQualityCheck(
            name="cliente_duplicate_id",
            description="Chaves primárias duplicadas na tabela cliente",
            severity="CRITICAL",
            query=f"""
                WITH duplicated AS (
                  SELECT id_cliente
                  FROM {cliente}
                  GROUP BY id_cliente
                  HAVING COUNT(*) > 1
                )
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_cliente AS STRING), ', ' LIMIT 5) AS sample
                FROM duplicated
            """,
        ),
        DataQualityCheck(
            name="analise_tributaria_required_fields",
            description="Analises tributárias com campos obrigatórios, ranges e tipos válidos",
            severity="HIGH",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_analise AS STRING), ', ' LIMIT 5) AS sample
                FROM {analise}
                WHERE id_analise IS NULL
                  OR id_cliente IS NULL
                  OR valor_identificado IS NULL
                  OR valor_identificado < 0
                  OR periodo_tipo IS NULL
                  OR periodo_tipo NOT IN ('TRIMESTRE', 'SEMESTRE', 'MES', 'DESCONHECIDO')
                  OR (periodo_tipo = 'MES' AND (periodo_valor IS NULL OR periodo_valor NOT BETWEEN 1 AND 12))
                  OR (periodo_tipo = 'TRIMESTRE' AND (periodo_valor IS NULL OR periodo_valor NOT BETWEEN 1 AND 4))
                  OR (periodo_tipo = 'SEMESTRE' AND (periodo_valor IS NULL OR periodo_valor NOT IN (1, 2)))
            """,
        ),
        DataQualityCheck(
            name="analise_tributaria_orphan_clientes",
            description="Analises tributárias referenciam clientes inexistentes",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(DISTINCT CAST(a.id_cliente AS STRING), ', ' LIMIT 5) AS sample
                FROM {analise} AS a
                LEFT JOIN {cliente} AS c
                  ON a.id_cliente = c.id_cliente
                WHERE c.id_cliente IS NULL
            """,
        ),
        DataQualityCheck(
            name="projeto_temporal_values",
            description="Datas coerentes e valores não negativos em projetos",
            severity="HIGH",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_projeto AS STRING), ', ' LIMIT 5) AS sample
                FROM {projeto}
                WHERE id_projeto IS NULL
                  OR id_cliente IS NULL
                  OR data_inicio IS NULL
                  OR data_prevista_fim IS NULL
                  OR data_inicio > data_prevista_fim
                  OR valor_projeto IS NULL
                  OR valor_projeto < 0
                  OR horas_totais_estimadas IS NULL
                  OR horas_totais_estimadas < 0
                  OR horas_totais_realizadas IS NULL
                  OR horas_totais_realizadas < 0
            """,
        ),
        DataQualityCheck(
            name="projeto_orphan_clientes",
            description="Projetos referenciam clientes inexistentes",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(DISTINCT CAST(p.id_cliente AS STRING), ', ' LIMIT 5) AS sample
                FROM {projeto} AS p
                LEFT JOIN {cliente} AS c
                  ON p.id_cliente = c.id_cliente
                WHERE c.id_cliente IS NULL
            """,
        ),
        DataQualityCheck(
            name="tarefa_temporal_values",
            description="Tarefas com campos obrigatórios e horas positivas",
            severity="HIGH",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_tarefa AS STRING), ', ' LIMIT 5) AS sample
                FROM {tarefa}
                WHERE id_tarefa IS NULL
                  OR id_projeto IS NULL
                  OR data_inicio IS NULL
                  OR data_prevista_fim IS NULL
                  OR data_inicio > data_prevista_fim
                  OR horas_estimadas IS NULL
                  OR horas_estimadas < 0
                  OR horas_realizadas IS NULL
                  OR horas_realizadas < 0
            """,
        ),
        DataQualityCheck(
            name="tarefa_orphan_projetos",
            description="Tarefas referenciam projetos inexistentes",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(DISTINCT CAST(t.id_projeto AS STRING), ', ' LIMIT 5) AS sample
                FROM {tarefa} AS t
                LEFT JOIN {projeto} AS p
                  ON t.id_projeto = p.id_projeto
                WHERE p.id_projeto IS NULL
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_required_fields",
            description="Notas fiscais com datas válidas e valores não negativos",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_nota AS STRING), ', ' LIMIT 5) AS sample
                FROM {nota}
                WHERE id_nota IS NULL
                  OR numero_nota IS NULL
                  OR id_cliente IS NULL
                  OR data_emissao IS NULL
                  OR data_emissao > CURRENT_DATE()
                  OR valor_servico IS NULL OR valor_servico < 0
                  OR valor_total IS NULL OR valor_total < 0
                  OR valor_total < valor_servico
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_orphan_clientes",
            description="Notas fiscais referenciam clientes inexistentes",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(DISTINCT CAST(n.id_cliente AS STRING), ', ' LIMIT 5) AS sample
                FROM {nota} AS n
                LEFT JOIN {cliente} AS c
                  ON n.id_cliente = c.id_cliente
                WHERE c.id_cliente IS NULL
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_duplicate_numero",
            description="Duplicidade de numero_nota por cliente",
            severity="HIGH",
            query=f"""
                WITH duplicated AS (
                  SELECT numero_nota, id_cliente
                  FROM {nota}
                  WHERE numero_nota IS NOT NULL AND id_cliente IS NOT NULL
                  GROUP BY numero_nota, id_cliente
                  HAVING COUNT(*) > 1
                )
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(
                    FORMAT('%s|%s', CAST(numero_nota AS STRING), CAST(id_cliente AS STRING)),
                    ', ' LIMIT 5
                  ) AS sample
                FROM duplicated
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_imposto_values",
            description="Tipos de impostos válidos com valores não negativos",
            severity="MEDIUM",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_imposto AS STRING), ', ' LIMIT 5) AS sample
                FROM {nota_imposto}
                WHERE id_imposto IS NULL
                  OR id_nota IS NULL
                  OR tipo_imposto IS NULL
                  OR UPPER(tipo_imposto) NOT IN ('ISS', 'PIS', 'COFINS')
                  OR valor IS NULL
                  OR valor < 0
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_imposto_orphan_nota",
            description="Registros de impostos sem nota fiscal associada",
            severity="CRITICAL",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(DISTINCT CAST(i.id_nota AS STRING), ', ' LIMIT 5) AS sample
                FROM {nota_imposto} AS i
                LEFT JOIN {nota} AS n
                  ON i.id_nota = n.id_nota
                WHERE n.id_nota IS NULL
            """,
        ),
        DataQualityCheck(
            name="nota_fiscal_item_values",
            description="Itens da nota com quantidades positivas e valores válidos",
            severity="MEDIUM",
            query=f"""
                SELECT
                  COUNT(*) AS failures,
                  STRING_AGG(CAST(id_item AS STRING), ', ' LIMIT 5) AS sample
                FROM {nota_item}
                WHERE id_item IS NULL
                  OR id_nota IS NULL
                  OR quantidade IS NULL OR quantidade <= 0
                  OR valor_unitario IS NULL OR valor_unitario < 0
            """,
        ),
    ]


def run_checks(client: bigquery.Client, checks: Iterable[DataQualityCheck]) -> List[CheckResult]:
    results: List[CheckResult] = []
    for check in checks:
        try:
            job = client.query(check.query)
            rows = list(job.result())
            if not rows:
                raise ValueError("Query did not return any rows")
            failures = rows[0].get("failures")
            sample = rows[0].get("sample")
            failures = int(failures) if failures is not None else None
            status = "PASS" if failures == 0 else "FAIL"
            results.append(
                CheckResult(
                    name=check.name,
                    description=check.description,
                    severity=check.severity,
                    failures=failures,
                    sample=sample,
                    status=status,
                )
            )
        except Exception as exc:  # pylint: disable=broad-except
            results.append(
                CheckResult(
                    name=check.name,
                    description=check.description,
                    severity=check.severity,
                    failures=None,
                    sample=None,
                    status="ERROR",
                    error=str(exc),
                )
            )
    return results


def print_report(results: Iterable[CheckResult]) -> None:
    header = f"{'Check':35} {'Severity':8} {'Status':7} {'Failures':8} Sample/Erro"
    print(header)
    print("-" * len(header))
    for result in results:
        sample = result.error if result.status == "ERROR" else (result.sample or "-")
        print(
            f"{result.name:35} {result.severity:8} {result.status:7} "
            f"{str(result.failures or 0):8} {sample}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data Quality para camada psa_curated")
    parser.add_argument(
        "--project-id",
        default=os.environ.get("GCP_PROJECT") or DEFAULT_PROJECT_ID,
        help="ID do projeto GCP (default: %(default)s)",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="Nome do dataset no BigQuery (default: %(default)s)",
    )
    parser.add_argument(
        "--output-json",
        help="Opcional: caminho para salvar o resultado em JSON",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = bigquery.Client(project=args.project_id)
    checks = build_checks(args.project_id, args.dataset)
    results = run_checks(client, checks)
    print_report(results)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as fp:
            json.dump([r.to_dict() for r in results], fp, ensure_ascii=False, indent=2)
        print(f"\nRelatório salvo em {args.output_json}")

    has_failure = any(r.status in {"FAIL", "ERROR"} for r in results)
    if has_failure:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
