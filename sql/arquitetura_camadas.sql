
--Tabelas nivel Silver------


--Clientes
CREATE OR REPLACE TABLE psa_curated.cliente AS
SELECT DISTINCT
  id_cliente AS id_cliente,
  UPPER(TRIM(razao_social)) AS razao_social,
  REGEXP_REPLACE(cnpj, r'\D', '') AS cnpj,
  LOWER(TRIM(setor)) AS setor,
  LOWER(TRIM(cidade)) AS cidade,
  estado,
  PARSE_DATE('%Y-%m-%d', data_cadastro) AS data_cadastro,
  CURRENT_TIMESTAMP() AS data_tratamento
FROM psa_raw.clientes
WHERE id_cliente IS NOT NULL;

ALTER TABLE psa_curated.cliente ADD PRIMARY KEY (id_cliente) NOT ENFORCED;
----------------------------------------


--Analises Tributárias
CREATE OR REPLACE TABLE psa_curated.analise_tributaria AS
SELECT DISTINCT
  SAFE_CAST(id_analise AS STRING) AS id_analise,
  SAFE_CAST(cliente_id AS STRING) AS id_cliente,
  UPPER(TRIM(tipo_tributo)) AS tipo_tributo,
  -- Extrai apenas o ano em uma coluna própria
  SAFE_CAST(SUBSTR(periodo_analise, 1, 4) AS INT64) AS ano,

  -- Coluna nova para o tipo de periodo.
  CASE
    WHEN REGEXP_CONTAINS(periodo_analise, r'-Q[1-4]') THEN 'TRIMESTRE'
    WHEN REGEXP_CONTAINS(periodo_analise, r'-S[1-2]') THEN 'SEMESTRE'
    WHEN REGEXP_CONTAINS(periodo_analise, r'-\d{2}$') THEN 'MES'
    ELSE 'DESCONHECIDO'
  END AS periodo_tipo,

  -- Extrai valor numérico (mês, trimestre ou semestre)
  CASE
    WHEN REGEXP_CONTAINS(periodo_analise, r'-Q[1-4]') 
      THEN SAFE_CAST(REGEXP_EXTRACT(periodo_analise, r'Q([1-4])') AS INT64)
    WHEN REGEXP_CONTAINS(periodo_analise, r'-S[1-2]') 
      THEN SAFE_CAST(REGEXP_EXTRACT(periodo_analise, r'S([1-2])') AS INT64)
    WHEN REGEXP_CONTAINS(periodo_analise, r'-\d{2}$') 
      THEN SAFE_CAST(REGEXP_EXTRACT(periodo_analise, r'-(\d{2})$') AS INT64)
    ELSE NULL
  END AS periodo_valor,

  SAFE_CAST(valor_identificado AS FLOAT64) AS valor_identificado,
  INITCAP(TRIM(observacoes)) AS observacoes,
  CURRENT_TIMESTAMP() AS data_tratamento

FROM psa_raw.analises_tributarias
WHERE id_analise IS NOT NULL
  AND cliente_id IS NOT NULL
  AND valor_identificado IS NOT NULL;

ALTER TABLE psa_curated.analise_tributaria
ADD PRIMARY KEY (id_analise) NOT ENFORCED;

ALTER TABLE psa_curated.analise_tributaria
ADD FOREIGN KEY (id_cliente) 
REFERENCES `psa-data-test-476002.psa_curated.cliente`(id_cliente) NOT ENFORCED;

---------------------------------------------------



--Projetos
CREATE OR REPLACE TABLE psa_curated.projeto AS
SELECT DISTINCT
  proj.id_projeto,
  proj.nome_projeto,
  proj.id_cliente,
  proj.responsavel,
  SAFE_CAST(proj.data_inicio AS DATE) AS data_inicio,
  SAFE_CAST(proj.data_prevista_fim AS DATE) AS data_prevista_fim,
  proj.status,
  SAFE_CAST(proj.valor_projeto AS FLOAT64) AS valor_projeto,
  SAFE_CAST(proj.horas_totais_estimadas AS INT64) AS horas_totais_estimadas,
  SAFE_CAST(proj.horas_totais_realizadas AS INT64) AS horas_totais_realizadas
FROM
  `psa-data-test-476002.psa_raw.tarefas_projetos`,
  UNNEST(JSON_EXTRACT_ARRAY(conteudo, "$.projetos")) AS projeto_json,
  UNNEST([STRUCT(
    JSON_VALUE(projeto_json, "$.id_projeto") AS id_projeto,
    JSON_VALUE(projeto_json, "$.nome_projeto") AS nome_projeto,
    JSON_VALUE(projeto_json, "$.cliente_id") AS id_cliente,
    JSON_VALUE(projeto_json, "$.responsavel") AS responsavel,
    JSON_VALUE(projeto_json, "$.data_inicio") AS data_inicio,
    JSON_VALUE(projeto_json, "$.data_prevista_fim") AS data_prevista_fim,
    JSON_VALUE(projeto_json, "$.status") AS status,
    JSON_VALUE(projeto_json, "$.valor_projeto") AS valor_projeto,
    JSON_VALUE(projeto_json, "$.horas_totais_estimadas") AS horas_totais_estimadas,
    JSON_VALUE(projeto_json, "$.horas_totais_realizadas") AS horas_totais_realizadas
  )]) AS proj
WHERE proj.id_projeto IS NOT NULL;

ALTER TABLE `psa-data-test-476002.psa_curated.projeto`
ADD PRIMARY KEY (id_projeto) NOT ENFORCED;

ALTER TABLE `psa-data-test-476002.psa_curated.projeto`
ADD FOREIGN KEY (id_cliente) 
REFERENCES `psa-data-test-476002.psa_curated.cliente`(id_cliente) NOT ENFORCED;
---------------------------------------------------------


--Tarefas do Projeto
CREATE OR REPLACE TABLE `psa-data-test-476002.psa_curated.tarefa`
CLUSTER BY id_projeto AS
SELECT DISTINCT
  JSON_VALUE(projeto_json, "$.id_projeto") AS id_projeto,
  JSON_VALUE(tarefa_json, "$.id_tarefa") AS id_tarefa,
  JSON_VALUE(tarefa_json, "$.descricao") AS descricao,
  SAFE_CAST(JSON_VALUE(tarefa_json, "$.data_inicio") AS DATE) AS data_inicio,
  SAFE_CAST(JSON_VALUE(tarefa_json, "$.data_prevista_fim") AS DATE) AS data_prevista_fim,
  JSON_VALUE(tarefa_json, "$.responsavel_tarefa") AS responsavel_tarefa,
  JSON_VALUE(tarefa_json, "$.status_tarefa") AS status_tarefa,
  SAFE_CAST(JSON_VALUE(tarefa_json, "$.horas_estimadas") AS INT64) AS horas_estimadas,
  SAFE_CAST(JSON_VALUE(tarefa_json, "$.horas_realizadas") AS INT64) AS horas_realizadas
FROM
  `psa-data-test-476002.psa_raw.tarefas_projetos`,
  UNNEST(JSON_EXTRACT_ARRAY(conteudo, "$.projetos")) AS projeto_json,
  UNNEST(JSON_EXTRACT_ARRAY(projeto_json, "$.tarefas")) AS tarefa_json
WHERE
  JSON_VALUE(tarefa_json, "$.id_tarefa") IS NOT NULL;

ALTER TABLE `psa-data-test-476002.psa_curated.tarefa`
ADD PRIMARY KEY (id_tarefa) NOT ENFORCED;

ALTER TABLE `psa-data-test-476002.psa_curated.tarefa`
ADD FOREIGN KEY (id_projeto) 
REFERENCES `psa-data-test-476002.psa_curated.projeto`(id_projeto) NOT ENFORCED;
------------------------------------------



--Nota Fiscal
CREATE OR REPLACE TABLE psa_curated.nota_fiscal
PARTITION BY data_emissao AS   --Partição da tabela pelo dia da emissão. 
WITH parsed AS (
  SELECT DISTINCT
    GENERATE_UUID() AS id_nota,
    JSON_VALUE(nota, '$.NumeroNota') AS numero_nota,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(nota, '$.DataEmissao')) AS data_emissao,
    JSON_VALUE(nota, '$.ClienteID') AS id_cliente,
    CAST(JSON_VALUE(nota, '$.ValorServico') AS FLOAT64) AS valor_servico,
    CAST(JSON_VALUE(nota, '$.ValorTotal') AS FLOAT64) AS valor_total
  FROM `psa_raw.notas_fiscais_json`,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(conteudo, '$.NotasFiscais.NotaFiscal'))) AS nota
)
SELECT * FROM parsed;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal`
ADD PRIMARY KEY (id_nota) NOT ENFORCED;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal`
ADD FOREIGN KEY (id_cliente) 
REFERENCES `psa-data-test-476002.psa_curated.cliente`(id_cliente) NOT ENFORCED;
------------------------------------------

--Imposto da nota Fiscal
CREATE OR REPLACE TABLE psa_curated.nota_fiscal_imposto
CLUSTER BY id_nota, tipo_imposto AS  --Clusterização por tipo_imposto para melhorar performance em consultas de agregação de valores
SELECT
  GENERATE_UUID() AS id_imposto,
  n.id_nota AS id_nota,
  imp.tipo_imposto,
  imp.valor
FROM
  `psa_raw.notas_fiscais_json`,
  psa_curated.nota_fiscal AS n,
  UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(conteudo, '$.NotasFiscais.NotaFiscal'))) AS nota,
  UNNEST([
    STRUCT('ISS' AS tipo_imposto, CAST(JSON_VALUE(nota, '$.Impostos.ISS') AS FLOAT64) AS valor),
    STRUCT('PIS' AS tipo_imposto, CAST(JSON_VALUE(nota, '$.Impostos.PIS') AS FLOAT64) AS valor),
    STRUCT('COFINS' AS tipo_imposto, CAST(JSON_VALUE(nota, '$.Impostos.COFINS') AS FLOAT64) AS valor)
  ]) AS imp
WHERE
  JSON_VALUE(nota, '$.NumeroNota') = n.numero_nota
  AND imp.valor IS NOT NULL;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal_imposto`
ADD PRIMARY KEY (id_imposto) NOT ENFORCED;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal_imposto`
ADD FOREIGN KEY (id_nota) 
REFERENCES `psa-data-test-476002.psa_curated.nota_fiscal`(id_nota) NOT ENFORCED;
---------------------------


--Item da Nota Fiscal
CREATE OR REPLACE TABLE psa_curated.nota_fiscal_item
CLUSTER BY id_nota AS
SELECT
  GENERATE_UUID() AS id_item,
  n.id_nota AS id_nota,
  JSON_VALUE(item, '$.Descricao') AS descricao,
  CAST(JSON_VALUE(item, '$.Quantidade') AS INT64) AS quantidade,
  CAST(JSON_VALUE(item, '$.ValorUnitario') AS FLOAT64) AS valor_unitario
FROM 
  `psa_raw.notas_fiscais_json`,
  psa_curated.nota_fiscal AS n
JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(conteudo, '$.NotasFiscais.NotaFiscal'))) AS nota
  ON JSON_VALUE(nota, '$.NumeroNota') = n.numero_nota,
UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(nota, '$.Itens.Item'))) AS item;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal_item`
ADD PRIMARY KEY (id_item) NOT ENFORCED;

ALTER TABLE `psa-data-test-476002.psa_curated.nota_fiscal_item`
ADD FOREIGN KEY (id_nota) 
REFERENCES `psa-data-test-476002.psa_curated.nota_fiscal`(id_nota) NOT ENFORCED;


--Logs do Sistema
CREATE OR REPLACE TABLE psa_curated.log_sistema
PARTITION BY data_evento
CLUSTER BY usuario, acao AS
SELECT
  DISTINCT
  SAFE_CAST(id_log AS STRING) AS id_log,
  DATE(log_time) AS data_evento,
  TIMESTAMP_TRUNC(log_time, SECOND) AS log_time,
  LOWER(TRIM(usuario)) AS usuario,
  INITCAP(TRIM(acao)) AS acao,
  INITCAP(TRIM(entidade_afetada)) AS entidade_afetada,
  UPPER(TRIM(resultado)) AS resultado,
  REGEXP_REPLACE(IFNULL(ip, ''), r'[^0-9\\.]+', '') AS ip,
  CURRENT_TIMESTAMP() AS data_tratamento
FROM `psa_raw.logs_sistema_table`
WHERE id_log IS NOT NULL;
ALTER TABLE `psa-data-test-476002.psa_curated.log_sistema`
ADD PRIMARY KEY (id_log) NOT ENFORCED;

