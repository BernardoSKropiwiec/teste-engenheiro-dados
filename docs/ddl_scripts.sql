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
  valor_identificado NUMERIC,
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
