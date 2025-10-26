-------------------Bronze---------------------
CREATE TABLE `psa_raw.clientes` (
    id_cliente STRING,
    razao_social STRING,
    cnpj STRING,
    porte_empresa STRING,
    setor STRING,
    cidade STRING,
    estado STRING,
    data_cadastro STRING,
)
OPTIONS(description="Carga bruta dos dados de clientes - CSV original");


CREATE OR REPLACE TABLE `psa_raw.analises_tributarias` (
  id_analise STRING,
  cliente_id STRING,
  tipo_tributo STRING,
  periodo_analise STRING,
  valor_identificado FLOAT64,
  observacoes STRING
)
OPTIONS(description="Carga bruta das análises tributárias - TXT delimitado por pipe '|'");


CREATE OR REPLACE TABLE `psa_raw.notas_fiscais` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos arquivos XML de notas fiscais");


CREATE OR REPLACE TABLE `psa_raw.tarefas_projetos` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos dados de projetos e tarefas - JSON completo");

CREATE OR REPLACE TABLE `psa_raw.logs_sistema` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos logs de sistema - HTML completo");


CREATE OR REPLACE TABLE `psa_raw.notas_fiscais_json` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos arquivos dos arquivos XML, formatados como JSON");

CREATE OR REPLACE TABLE `psa_raw.logs_sistema_table`(
  id_log STRING,
  log_time TIMESTAMP,
  usuario STRING,
  acao STRING,
  entidade_afetada STRING,
  resultado STRING
  ip STRING
)
OPTIONS(description="Carga dos dados sem tratamento da tabela do arquivo de logs em formato HTML");
-------------------silver---------------------


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



--Views----------------

CREATE OR REPLACE VIEW `psa_analytics.resumo_clientes_tributos` AS
SELECT
  c.id_cliente,
  c.razao_social,
  c.cnpj,
  c.setor,
  c.cidade,
  c.estado,
  COUNT(DISTINCT a.id_analise) AS total_analises,
  ROUND(SUM(a.valor_identificado), 2) AS valor_identificado_total,
  ROUND(SUM(i.valor), 2) AS valor_impostos_total
FROM
  `psa_curated.cliente` AS c
LEFT JOIN `psa_curated.analise_tributaria` AS a
  ON c.id_cliente = a.id_cliente
LEFT JOIN `psa_curated.nota_fiscal` AS n
  ON c.id_cliente = n.id_cliente
LEFT JOIN `psa_curated.nota_fiscal_imposto` AS i
  ON n.id_nota = i.id_nota
GROUP BY
  c.id_cliente, c.razao_social, c.cnpj, c.setor, c.cidade, c.estado;


CREATE OR REPLACE VIEW `psa_analytics.performance_projetos` AS
SELECT
  p.id_projeto,
  p.nome_projeto,
  p.responsavel,
  p.status,
  p.valor_projeto,
  p.horas_totais_estimadas,
  p.horas_totais_realizadas,
  COUNT(DISTINCT t.id_tarefa) AS total_tarefas,
  ROUND(SUM(t.horas_realizadas), 2) AS horas_realizadas_total,
  ROUND(SUM(t.horas_estimadas), 2) AS horas_estimadas_total,
  SAFE_DIVIDE(SUM(t.horas_realizadas), NULLIF(SUM(t.horas_estimadas), 0)) AS eficiencia
FROM
  `psa_curated.projeto` AS p
LEFT JOIN `psa_curated.tarefa` AS t
  ON p.id_projeto = t.id_projeto
GROUP BY
  p.id_projeto, p.nome_projeto, p.responsavel, p.status,
  p.valor_projeto, p.horas_totais_estimadas, p.horas_totais_realizadas;
