CREATE OR REPLACE VIEW psa_quality.vw_dq_cliente AS
SELECT
  'cliente' AS tabela,
  COUNTIF(id_cliente IS NULL) AS nulos_id_cliente,
  COUNTIF(cnpj IS NULL OR NOT REGEXP_CONTAINS(cnpj, r'^\d{14}$')) AS cnpj_invalido,
  COUNTIF(data_cadastro IS NULL OR data_cadastro > CURRENT_DATE()) AS data_cadastro_invalida,
  COUNT(*) - COUNT(DISTINCT id_cliente) AS duplicatas_id_cliente,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.cliente;


CREATE OR REPLACE VIEW psa_quality.vw_dq_analise_tributaria AS
SELECT
  'analise_tributaria' AS tabela,
  COUNTIF(id_analise IS NULL) AS nulos_id_analise,
  COUNTIF(id_cliente IS NULL) AS nulos_id_cliente,
  COUNTIF(valor_identificado < 0) AS valores_negativos,
  COUNTIF(periodo_tipo NOT IN ('MES','TRIMESTRE','SEMESTRE')) AS periodo_tipo_invalido,
  COUNT(*) - COUNT(DISTINCT id_analise) AS duplicatas_id_analise,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.analise_tributaria;


CREATE OR REPLACE VIEW psa_quality.vw_dq_projeto AS
SELECT
  'projeto' AS tabela,
  COUNTIF(id_projeto IS NULL) AS nulos_id_projeto,
  COUNTIF(id_cliente IS NULL) AS nulos_id_cliente,
  COUNTIF(data_inicio > data_prevista_fim) AS datas_invertidas,
  COUNTIF(valor_projeto < 0) AS valores_negativos,
  COUNT(*) - COUNT(DISTINCT id_projeto) AS duplicatas_id_projeto,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.projeto;


CREATE OR REPLACE VIEW psa_quality.vw_dq_tarefa AS
SELECT
  'tarefa' AS tabela,
  COUNTIF(id_tarefa IS NULL) AS nulos_id_tarefa,
  COUNTIF(id_projeto IS NULL) AS nulos_id_projeto,
  COUNTIF(data_inicio > data_prevista_fim) AS datas_invertidas,
  COUNTIF(horas_estimadas < 0 OR horas_realizadas < 0) AS horas_negativas,
  COUNT(*) - COUNT(DISTINCT id_tarefa) AS duplicatas_id_tarefa,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.tarefa;


CREATE OR REPLACE VIEW psa_quality.vw_dq_nota_fiscal AS
SELECT
  'nota_fiscal' AS tabela,
  COUNTIF(id_nota IS NULL) AS nulos_id_nota,
  COUNTIF(id_cliente IS NULL) AS nulos_id_cliente,
  COUNTIF(valor_total < 0 OR valor_servico < 0) AS valores_negativos,
  COUNTIF(data_emissao IS NULL OR data_emissao > CURRENT_DATE()) AS data_emissao_invalida,
  COUNT(*) - COUNT(DISTINCT numero_nota) AS duplicatas_numero_nota,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.nota_fiscal;


CREATE OR REPLACE VIEW psa_quality.vw_dq_nota_fiscal_imposto AS
SELECT
  'nota_fiscal_imposto' AS tabela,
  COUNTIF(id_imposto IS NULL) AS nulos_id_imposto,
  COUNTIF(id_nota IS NULL) AS nulos_id_nota,
  COUNTIF(valor < 0) AS valores_negativos,
  COUNTIF(tipo_imposto NOT IN ('ISS','PIS','COFINS')) AS tipo_invalido,
  COUNT(*) - COUNT(DISTINCT id_imposto) AS duplicatas_id_imposto,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.nota_fiscal_imposto;


CREATE OR REPLACE VIEW psa_quality.vw_dq_nota_fiscal_item AS
SELECT
  'nota_fiscal_item' AS tabela,
  COUNTIF(id_item IS NULL) AS nulos_id_item,
  COUNTIF(id_nota IS NULL) AS nulos_id_nota,
  COUNTIF(quantidade <= 0 OR valor_unitario < 0) AS valores_invalidos,
  COUNT(*) - COUNT(DISTINCT id_item) AS duplicatas_id_item,
  CURRENT_TIMESTAMP() AS data_checagem
FROM psa_curated.nota_fiscal_item;


