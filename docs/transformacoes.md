# Documentação de Transformações — Camada Curated (`psa_curated`)

Este documento descreve os principais tratamentos, transformações e padronizações aplicados aos dados de cada tabela da camada **Curated** no projeto BigQuery `psa-data-test-476002`.

---

## **1. Tabela `cliente`**

**Fonte:** `psa_raw.clientes`  
**Tratamentos aplicados:**
- Remoção de duplicidades via `SELECT DISTINCT`.  
- Padronização de nomes e strings:
  - `razao_social`: convertido para **maiúsculas** e `TRIM`.  
  - `setor`, `cidade`: convertidos para **minúsculas** e `TRIM`.  
- Limpeza de CNPJ com `REGEXP_REPLACE(cnpj, r'\D', '')`.  
- Conversão da data de cadastro (`data_cadastro`) para tipo `DATE` via `PARSE_DATE`.  
- Inclusão de `data_tratamento` com `CURRENT_TIMESTAMP()`.  
- Definição de `id_cliente` como **Primary Key**.

---

## **2. Tabela `analise_tributaria`**

**Fonte:** `psa_raw.analises_tributarias`  
**Tratamentos aplicados:**
- Conversão de tipos (`SAFE_CAST`) para IDs e valores numéricos.  
- Padronização textual:
  - `tipo_tributo`: convertido para **maiúsculas** e `TRIM`.  
  - `observacoes`: capitalização via `INITCAP` e remoção de espaços.  
- Extração de componentes de período:
  - `ano`: primeiros 4 caracteres de `periodo_analise`.  
  - `periodo_tipo`: categorização entre **MÊS**, **TRIMESTRE**, **SEMESTRE** ou **DESCONHECIDO**.  
  - `periodo_valor`: número do mês, trimestre ou semestre extraído por regex.  
- Filtro de registros válidos (`id_analise`, `cliente_id`, `valor_identificado` não nulos).  
- Inclusão de `data_tratamento`.  
- Criação de **Primary Key** (`id_analise`) e **Foreign Key** (`id_cliente → cliente.id_cliente`).

---

## **3. Tabela `projeto`**

**Fonte:** `psa_raw.tarefas_projetos` (campo JSON `projetos`)  
**Tratamentos aplicados:**
- Extração de campos JSON via `JSON_VALUE`.  
- Conversão de datas (`data_inicio`, `data_prevista_fim`) e valores numéricos (`valor_projeto`, `horas_totais_*`).  
- Garantia de unicidade via `SELECT DISTINCT`.  
- Definição de **Primary Key** (`id_projeto`) e **Foreign Key** (`id_cliente → cliente.id_cliente`).

---

## **4. Tabela `tarefa`**

**Fonte:** `psa_raw.tarefas_projetos` (subcampo JSON `tarefas`)  
**Tratamentos aplicados:**
- Extração de tarefas aninhadas dentro de projetos.  
- Conversão de datas e tipos numéricos (`horas_estimadas`, `horas_realizadas`).  
- Clusterização por `id_projeto` para otimizar consultas por projeto.  
- Criação de **Primary Key** (`id_tarefa`) e **Foreign Key** (`id_projeto → projeto.id_projeto`).

---

## **5. Tabela `nota_fiscal`**

**Fonte:** `psa_raw.notas_fiscais_json`  
**Tratamentos aplicados:**
- Extração de notas fiscais via `UNNEST(JSON_EXTRACT_ARRAY(...))`.  
- Geração de identificador único (`GENERATE_UUID()` para `id_nota`).  
- Conversão de `DataEmissao` para `DATE` e valores monetários para `FLOAT64`.  
- Particionamento por `data_emissao` para otimizar leitura temporal.  
- Definição de **Primary Key** (`id_nota`) e **Foreign Key** (`id_cliente → cliente.id_cliente`).

---

## **6. Tabela `nota_fiscal_imposto`**

**Fonte:** `psa_raw.notas_fiscais_json`  
**Tratamentos aplicados:**
- Desnormalização dos campos `ISS`, `PIS`, `COFINS` em múltiplas linhas via `UNNEST([...])`.  
- Associação com `nota_fiscal` por `numero_nota`.  
- Filtragem de valores nulos.  
- Clusterização por `id_nota, tipo_imposto`.  
- Criação de **Primary Key** (`id_imposto`) e **Foreign Key** (`id_nota → nota_fiscal.id_nota`).

---

## **7. Tabela `nota_fiscal_item`**

**Fonte:** `psa_raw.notas_fiscais_json`  
**Tratamentos aplicados:**
- Extração dos itens de cada nota via `UNNEST(JSON_EXTRACT_ARRAY(...))`.  
- Associação com `nota_fiscal` por `numero_nota`.  
- Conversão de tipos (`INT64`, `FLOAT64`).  
- Clusterização por `id_nota`.  
- Criação de **Primary Key** (`id_item`) e **Foreign Key** (`id_nota → nota_fiscal.id_nota`).

---

## **8. Tabela `log_sistema`**

**Fonte:** `psa_raw.logs_sistema_table`  
**Tratamentos aplicados:**
- Conversão de `log_time` para `DATE` (`data_evento`) e `TIMESTAMP_TRUNC`.  
- Padronização de strings:
  - `usuario`: **minúsculas**, `TRIM`.  
  - `acao` e `entidade_afetada`: `INITCAP`.  
  - `resultado`: **maiúsculas**.  
- Limpeza de IP via `REGEXP_REPLACE` para manter apenas caracteres válidos.  
- Particionamento por `data_evento` e clusterização por `usuario, acao`.  
- Criação de **Primary Key** (`id_log`).  
- Inclusão de `data_tratamento`.
