CREATE OR REPLACE TABLE `psa_raw.clientes` (
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
  resultado STRING,
  ip STRING
)
OPTIONS(description="Carga dos dados sem tratamento da tabela do arquivo de logs em formato HTML");