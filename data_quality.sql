-- Data Quality checks for psa_curated (Silver layer)
-- Como usar:
--   1. Abra o BigQuery Console e selecione o projeto desejado.
--   2. Cole todo o script em uma query multi-statement e execute.
--   3. O resultado consolidado aparecerá ao final selecionando a temp table dq_results.
-- Ajuste o prefixo `psa-data-test-476002.psa_curated` caso esteja trabalhando em outro dataset.

CREATE TEMP TABLE dq_results (
  check_name STRING,
  severity STRING,
  description STRING,
  failures INT64,
  sample STRING
);

-- ---------------------------------------------------------------------------
-- Cliente
INSERT INTO dq_results
SELECT
  'cliente_required_fields',
  'CRITICAL',
  'Campos obrigatórios e formato de CNPJ em cliente',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_cliente AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.cliente`
WHERE id_cliente IS NULL
  OR razao_social IS NULL
  OR cnpj IS NULL
  OR REGEXP_CONTAINS(cnpj, r'[^0-9]')
  OR LENGTH(cnpj) != 14
  OR estado IS NULL
  OR data_cadastro IS NULL;

INSERT INTO dq_results
WITH duplicated AS (
  SELECT id_cliente
  FROM `psa-data-test-476002.psa_curated.cliente`
  GROUP BY id_cliente
  HAVING COUNT(*) > 1
)
SELECT
  'cliente_duplicate_id',
  'CRITICAL',
  'Chaves primárias duplicadas na tabela cliente',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_cliente AS STRING), ', ' LIMIT 5) AS sample
FROM duplicated;

-- ---------------------------------------------------------------------------
-- Analise Tributaria
INSERT INTO dq_results
SELECT
  'analise_tributaria_required_fields',
  'HIGH',
  'Analises tributárias com campos obrigatórios, ranges e tipos válidos',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_analise AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.analise_tributaria`
WHERE id_analise IS NULL
  OR id_cliente IS NULL
  OR valor_identificado IS NULL
  OR valor_identificado < 0
  OR periodo_tipo IS NULL
  OR periodo_tipo NOT IN ('TRIMESTRE', 'SEMESTRE', 'MES', 'DESCONHECIDO')
  OR (periodo_tipo = 'MES' AND (periodo_valor IS NULL OR periodo_valor NOT BETWEEN 1 AND 12))
  OR (periodo_tipo = 'TRIMESTRE' AND (periodo_valor IS NULL OR periodo_valor NOT BETWEEN 1 AND 4))
  OR (periodo_tipo = 'SEMESTRE' AND (periodo_valor IS NULL OR periodo_valor NOT IN (1, 2)));

INSERT INTO dq_results
SELECT
  'analise_tributaria_orphan_clientes',
  'CRITICAL',
  'Analises tributárias referenciam clientes inexistentes',
  COUNT(*) AS failures,
  STRING_AGG(DISTINCT CAST(a.id_cliente AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.analise_tributaria` AS a
LEFT JOIN `psa-data-test-476002.psa_curated.cliente` AS c
  ON a.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL;

-- ---------------------------------------------------------------------------
-- Projeto
INSERT INTO dq_results
SELECT
  'projeto_temporal_values',
  'HIGH',
  'Datas coerentes e valores não negativos em projetos',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_projeto AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.projeto`
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
  OR horas_totais_realizadas < 0;

INSERT INTO dq_results
SELECT
  'projeto_orphan_clientes',
  'CRITICAL',
  'Projetos referenciam clientes inexistentes',
  COUNT(*) AS failures,
  STRING_AGG(DISTINCT CAST(p.id_cliente AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.projeto` AS p
LEFT JOIN `psa-data-test-476002.psa_curated.cliente` AS c
  ON p.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL;

-- ---------------------------------------------------------------------------
-- Tarefa
INSERT INTO dq_results
SELECT
  'tarefa_temporal_values',
  'HIGH',
  'Tarefas com campos obrigatórios e horas positivas',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_tarefa AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.tarefa`
WHERE id_tarefa IS NULL
  OR id_projeto IS NULL
  OR data_inicio IS NULL
  OR data_prevista_fim IS NULL
  OR data_inicio > data_prevista_fim
  OR horas_estimadas IS NULL
  OR horas_estimadas < 0
  OR horas_realizadas IS NULL
  OR horas_realizadas < 0;

INSERT INTO dq_results
SELECT
  'tarefa_orphan_projetos',
  'CRITICAL',
  'Tarefas referenciam projetos inexistentes',
  COUNT(*) AS failures,
  STRING_AGG(DISTINCT CAST(t.id_projeto AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.tarefa` AS t
LEFT JOIN `psa-data-test-476002.psa_curated.projeto` AS p
  ON t.id_projeto = p.id_projeto
WHERE p.id_projeto IS NULL;

-- ---------------------------------------------------------------------------
-- Nota Fiscal
INSERT INTO dq_results
SELECT
  'nota_fiscal_required_fields',
  'CRITICAL',
  'Notas fiscais com datas válidas e valores não negativos',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_nota AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.nota_fiscal`
WHERE id_nota IS NULL
  OR numero_nota IS NULL
  OR id_cliente IS NULL
  OR data_emissao IS NULL
  OR data_emissao > CURRENT_DATE()
  OR valor_servico IS NULL OR valor_servico < 0
  OR valor_total IS NULL OR valor_total < 0
  OR valor_total < valor_servico;

INSERT INTO dq_results
WITH duplicated AS (
  SELECT numero_nota, id_cliente
  FROM `psa-data-test-476002.psa_curated.nota_fiscal`
  WHERE numero_nota IS NOT NULL AND id_cliente IS NOT NULL
  GROUP BY numero_nota, id_cliente
  HAVING COUNT(*) > 1
)
SELECT
  'nota_fiscal_duplicate_numero',
  'HIGH',
  'Duplicidade de numero_nota por cliente',
  COUNT(*) AS failures,
  STRING_AGG(FORMAT('%s|%s', CAST(numero_nota AS STRING), CAST(id_cliente AS STRING)), ', ' LIMIT 5) AS sample
FROM duplicated;

INSERT INTO dq_results
SELECT
  'nota_fiscal_orphan_clientes',
  'CRITICAL',
  'Notas fiscais referenciam clientes inexistentes',
  COUNT(*) AS failures,
  STRING_AGG(DISTINCT CAST(n.id_cliente AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.nota_fiscal` AS n
LEFT JOIN `psa-data-test-476002.psa_curated.cliente` AS c
  ON n.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL;

-- ---------------------------------------------------------------------------
-- Nota Fiscal Imposto
INSERT INTO dq_results
SELECT
  'nota_fiscal_imposto_values',
  'MEDIUM',
  'Tipos de impostos válidos com valores não negativos',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_imposto AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.nota_fiscal_imposto`
WHERE id_imposto IS NULL
  OR id_nota IS NULL
  OR tipo_imposto IS NULL
  OR UPPER(tipo_imposto) NOT IN ('ISS', 'PIS', 'COFINS')
  OR valor IS NULL
  OR valor < 0;

INSERT INTO dq_results
SELECT
  'nota_fiscal_imposto_orphan_nota',
  'CRITICAL',
  'Registros de impostos sem nota fiscal associada',
  COUNT(*) AS failures,
  STRING_AGG(DISTINCT CAST(i.id_nota AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.nota_fiscal_imposto` AS i
LEFT JOIN `psa-data-test-476002.psa_curated.nota_fiscal` AS n
  ON i.id_nota = n.id_nota
WHERE n.id_nota IS NULL;

-- ---------------------------------------------------------------------------
-- Nota Fiscal Item
INSERT INTO dq_results
SELECT
  'nota_fiscal_item_values',
  'MEDIUM',
  'Itens da nota com quantidades positivas e valores válidos',
  COUNT(*) AS failures,
  STRING_AGG(CAST(id_item AS STRING), ', ' LIMIT 5) AS sample
FROM `psa-data-test-476002.psa_curated.nota_fiscal_item`
WHERE id_item IS NULL
  OR id_nota IS NULL
  OR quantidade IS NULL OR quantidade <= 0
  OR valor_unitario IS NULL OR valor_unitario < 0;

-- ---------------------------------------------------------------------------
-- Resultado consolidado
SELECT
  check_name,
  severity,
  description,
  failures,
  COALESCE(sample, '-') AS sample
FROM dq_results
ORDER BY
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    ELSE 4
  END,
  failures DESC,
  check_name;
