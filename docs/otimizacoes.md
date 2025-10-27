# Otimizações de Particionamento e Clusterização – Projeto PSA

## Objetivo
Melhorar o desempenho das consultas e reduzir custos no BigQuery, aplicando **particionamento temporal** e **clusterização lógica** nas tabelas da camada *Silver*.

---

## 1. Particionamento

### 1.1. `psa_curated.nota_fiscal`
- **Tipo:** Particionamento por coluna de data (`data_emissao`)  
- **Motivo:** A maior parte das consultas analíticas filtra notas fiscais por período (ex.: mês ou ano).  
- **Benefício:** O BigQuery lê apenas as partições relevantes, reduzindo custo de varredura e tempo de execução.  
- **Implementação:**
  ```sql
  PARTITION BY data_emissao
  ```

### 1.2. `psa_curated.log_sistema`
- **Tipo:** Particionamento por data de evento (`data_evento`)  
- **Motivo:** Consultas de auditoria e relatórios operacionais frequentemente segmentam logs por dia.  
- **Benefício:** Leitura restrita às partições necessárias, permitindo análises rápidas de logs recentes.  
- **Implementação:**
  ```sql
  PARTITION BY data_evento
  ```

---

## 2. Clusterização

### 2.1. `psa_curated.nota_fiscal_imposto`
- **Campos de clusterização:** `id_nota`, `tipo_imposto`  
- **Motivo:** Consultas comuns agregam valores de impostos por nota e tipo de tributo.  
- **Benefício:**  
  - Melhora a eficiência de filtros como `WHERE tipo_imposto = 'ISS'`  
  - Reduz o custo de agrupamentos (`GROUP BY tipo_imposto`)  
- **Implementação:**
  ```sql
  CLUSTER BY id_nota, tipo_imposto
  ```

### 2.2. `psa_curated.nota_fiscal_item`
- **Campo de clusterização:** `id_nota`  
- **Motivo:** Cada item pertence a uma nota fiscal. Agrupar por `id_nota` facilita *joins* e somas por nota.  
- **Benefício:** Consultas analíticas que unem notas e itens se tornam mais rápidas.  
- **Implementação:**
  ```sql
  CLUSTER BY id_nota
  ```

### 2.3. `psa_curated.tarefa`
- **Campo de clusterização:** `id_projeto`  
- **Motivo:** Tarefas são sempre consultadas no contexto de um projeto.  
- **Benefício:** Otimiza *joins* e agregações entre `tarefa` e `projeto`.  
- **Implementação:**
  ```sql
  CLUSTER BY id_projeto
  ```

### 2.4. `psa_curated.log_sistema`
- **Campos de clusterização:** `usuario`, `acao`  
- **Motivo:** Filtros frequentes por usuário e tipo de ação (ex.: "login", "erro", "inserção").  
- **Benefício:** Acelera auditorias e relatórios por perfil de operação.  
- **Implementação:**
  ```sql
  CLUSTER BY usuario, acao
  ```

---

## 3. Resumo das Estratégias

| Tabela | Partição | Clusterização | Benefício principal |
|:--|:--|:--|:--|
| `nota_fiscal` | data_emissao | — | Consultas temporais mais rápidas |
| `nota_fiscal_imposto` | — | id_nota, tipo_imposto | Agregações tributárias otimizadas |
| `nota_fiscal_item` | — | id_nota | Joins rápidos entre notas e itens |
| `tarefa` | — | id_projeto | Consultas por projeto otimizadas |
| `log_sistema` | data_evento | usuario, acao | Auditorias e relatórios rápidos |
