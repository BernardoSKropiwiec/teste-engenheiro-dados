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
