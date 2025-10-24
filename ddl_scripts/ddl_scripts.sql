CREATE TABLE `psa-data-test-476002.psa_raw.cliente` (
    id_cliente STRING NOT NULL,
    razao_social STRING,
    cnpj STRING,
    porte_empresa STRING,
    setor STRING,
    cidade STRING,
    estado STRING,
    data_cadastro DATE,
    PRIMARY KEY (id_cliente) NOT ENFORCED
);

CREATE TABLE `psa-data-test-476002.psa_raw.analise` (
  id_analise STRING NOT NULL,
  cliente_id STRING NOT NULL,
  tipo_tributo STRING,
  periodo_analise STRING,
  valor_identificado FLOAT64,
  observacoes STRING,
  PRIMARY KEY (id_analise) NOT ENFORCED,
  FOREIGN KEY (cliente_id) REFERENCES `psa-data-test-476002.psa_raw.cliente` (id_cliente) NOT ENFORCED
);

CREATE TABLE `psa-data-test-476002.psa_raw.projeto` (
  id_projeto STRING NOT NULL,
  nome_projeto STRING,
  cliente_id STRING,
  responsavel STRING,
  data_inicio DATE,
  data_prevista_fim DATE,
  status STRING,
  valor_projeto FLOAT64,
  horas_totais_estimadas INT64,
  horas_totais_realizadas INT64,
  PRIMARY KEY (id_projeto) NOT ENFORCED
);

CREATE TABLE `psa-data-test-476002.psa_raw.tarefa` (
  id_tarefa STRING NOT NULL,
  id_projeto STRING NOT NULL,
  descricao STRING,
  data_inicio DATE,
  data_prevista_fim DATE,
  responsavel_tarefa STRING,
  status_tarefa STRING,
  horas_estimadas INT64,
  horas_realizadas INT64,
  PRIMARY KEY (id_tarefa) NOT ENFORCED,
  FOREIGN KEY (id_projeto) REFERENCES `psa-data-test-476002.psa_raw.projeto` (id_projeto) NOT ENFORCED
);

  CREATE TABLE `psa_raw.nota_fiscal` (
    numero_nota STRING NOT NULL,
    data_emissao DATE,
    cliente_id STRING,
    valor_servico FLOAT64,
    valor_total FLOAT64
  );

  CREATE TABLE `psa_raw.nota_fiscal_imposto` (
    numero_nota STRING NOT NULL,
    iss FLOAT64,
    pis FLOAT64,
    cofins FLOAT64
    FOREIGN KEY (numero_nota) REFERENCES `psa_raw.nota_fiscal` (numero_nota) NOT ENFORCED
  );

  CREATE TABLE `psa_raw.nota_fiscal_item` (
    numero_nota STRING NOT NULL,
    sequencia_item INT64,
    descricao STRING,
    quantidade INT64,
    valor_unitario FLOAT64
    FOREIGN KEY (numero_nota) REFERENCES `psa_raw.nota_fiscal` (numero_nota) NOT ENFORCED
  );







-------------------Bronze---------------------
CREATE TABLE `psa_raw_test.cliente` (
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


CREATE OR REPLACE TABLE `psa_raw_test.analises_tributarias` (
  id_analise STRING,
  cliente_id STRING,
  tipo_tributo STRING,
  periodo_analise STRING,
  valor_identificado FLOAT64,
  observacoes STRING
)
OPTIONS(description="Carga bruta das análises tributárias - TXT delimitado por pipe '|'");


CREATE OR REPLACE TABLE `psa_raw_test.notas_fiscais` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos arquivos XML de notas fiscais");


CREATE OR REPLACE TABLE `psa_raw_test.tarefas_projetos` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos dados de projetos e tarefas - JSON completo");

CREATE OR REPLACE TABLE `psa_raw_test.logs_sistema` (
  conteudo STRING
)
OPTIONS(description="Carga bruta dos logs de sistema - HTML completo");

-------------------silver---------------------
CREATE OR REPLACE TABLE psa_curated.clientes AS
SELECT DISTINCT
  id_cliente AS id_cliente,
  UPPER(TRIM(razao_social)) AS razao_social,
  REGEXP_REPLACE(cnpj, r'\D', '') AS cnpj,
  LOWER(TRIM(setor)) AS setor,
  LOWER(TRIM(cidade)) AS cidade,
  estado,
  PARSE_DATE('%Y-%m-%d', data_cadastro) AS data_cadastro
FROM psa_raw.clientes
WHERE id_cliente IS NOT NULL;


CREATE OR REPLACE TABLE psa_curated.analises_tributarias AS
SELECT DISTINCT
  SAFE_CAST(id_analise AS STRING) AS id_analise,
  SAFE_CAST(cliente_id AS STRING) AS cliente_id,
  UPPER(TRIM(tipo_tributo)) AS tipo_tributo,

  -- Extrai o ano (primeiros 4 dígitos)
  SAFE_CAST(SUBSTR(periodo_analise, 1, 4) AS INT64) AS ano,

  -- Define tipo do período
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

FROM psa_raw_test.analises_tributarias
WHERE id_analise IS NOT NULL
  AND cliente_id IS NOT NULL
  AND valor_identificado IS NOT NULL;



CREATE OR REPLACE TABLE `psa-data-test-476002.psa_curated.projetos` AS
SELECT DISTINCT
  proj.id_projeto,
  proj.nome_projeto,
  proj.cliente_id,
  proj.responsavel,
  SAFE_CAST(proj.data_inicio AS DATE) AS data_inicio,
  SAFE_CAST(proj.data_prevista_fim AS DATE) AS data_prevista_fim,
  proj.status,
  SAFE_CAST(proj.valor_projeto AS FLOAT64) AS valor_projeto,
  SAFE_CAST(proj.horas_totais_estimadas AS INT64) AS horas_totais_estimadas,
  SAFE_CAST(proj.horas_totais_realizadas AS INT64) AS horas_totais_realizadas
FROM
  `psa-data-test-476002.psa_raw_test.tarefas_projetos`,
  UNNEST(JSON_EXTRACT_ARRAY(conteudo, "$.projetos")) AS projeto_json,
  UNNEST([STRUCT(
    JSON_VALUE(projeto_json, "$.id_projeto") AS id_projeto,
    JSON_VALUE(projeto_json, "$.nome_projeto") AS nome_projeto,
    JSON_VALUE(projeto_json, "$.cliente_id") AS cliente_id,
    JSON_VALUE(projeto_json, "$.responsavel") AS responsavel,
    JSON_VALUE(projeto_json, "$.data_inicio") AS data_inicio,
    JSON_VALUE(projeto_json, "$.data_prevista_fim") AS data_prevista_fim,
    JSON_VALUE(projeto_json, "$.status") AS status,
    JSON_VALUE(projeto_json, "$.valor_projeto") AS valor_projeto,
    JSON_VALUE(projeto_json, "$.horas_totais_estimadas") AS horas_totais_estimadas,
    JSON_VALUE(projeto_json, "$.horas_totais_realizadas") AS horas_totais_realizadas
  )]) AS proj
WHERE proj.id_projeto IS NOT NULL;





CREATE OR REPLACE TABLE `psa-data-test-476002.psa_curated.tarefas` AS
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
  `psa-data-test-476002.psa_raw_test.tarefas_projetos`,
  UNNEST(JSON_EXTRACT_ARRAY(conteudo, "$.projetos")) AS projeto_json,
  UNNEST(JSON_EXTRACT_ARRAY(projeto_json, "$.tarefas")) AS tarefa_json
WHERE
  JSON_VALUE(tarefa_json, "$.id_tarefa") IS NOT NULL;



ALTER TABLE `psa-data-test-476002.psa_curated.projetos`
ADD PRIMARY KEY (id_projeto) NOT ENFORCED;


ALTER TABLE `psa-data-test-476002.psa_curated.tarefas`
ADD FOREIGN KEY (id_projeto) 
REFERENCES `psa-data-test-476002.psa_curated.projetos`(id_projeto) NOT ENFORCED;