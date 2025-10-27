# Teste Engenheiro de Dados

## Visão geral da solução

Este projeto implementa um **pipeline de ingestão e modelagem de dados no BigQuery**, estruturado em três camadas: **Bronze (psa_raw)**, **Silver (psa_curated)** e **Gold (psa_analytics)**.

### Fluxo de funcionamento

1. **Ingestão Raw (Bronze)**  
   O script `scripts/pipeline_ingestao.py` lê os arquivos brutos localizados em `dados/` (`.csv`, `.json`, `.xml`, `.txt`) e realiza o carregamento para tabelas `psa_raw.*` no BigQuery, mantendo logs em `logs/pipeline_ingestao.csv`.

2. **Modelagem Curated (Silver)**  
   Após a ingestão, os arquivos SQL em `sql/` devem ser executados no BigQuery para criar e popular as tabelas tratadas da camada **Silver (`psa_curated`)**.  
   Essas tabelas aplicam limpeza, normalização, padronização de tipos e criação de chaves primárias e estrangeiras.

3. **Modelagem Analytics (Gold)**  
   As views analíticas são criadas diretamente no BigQuery, na camada **Gold (`psa_analytics`)**, consolidando informações das tabelas tratadas e fornecendo bases para relatórios e dashboards analíticos.

4. **Views de Qualidade de Dados (Quality)**  
   As views responsáveis por monitorar e validar a integridade das tabelas estão na camada **Quality (`psa_quality`)**, que centraliza métricas e verificações de consistência entre as camadas Bronze e Silver.

5. **Execução do pipeline**  
   ```bash
   python scripts/pipeline_ingestao.py
   ```
   Após o carregamento:
   - Execute os scripts SQL de criação das tabelas Silver e views Gold (em um projeto novo).
   - No projeto atual, as tabelas e views já estão criadas.

---

## Preparar ambiente Python

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Dependências e pré-requisitos

- Python 3.10+ (o projeto usa 3.13 na venv local).  
- Conta de serviço com permissões de BigQuery Data Editor e BigQuery Job User.  
- Bibliotecas listadas em `requirements.txt` (pandas, google-cloud-bigquery, pandas-gbq, BeautifulSoup4, xmltodict etc.).  
- Acesso aos datasets `psa_raw` e `psa_curated` dentro do projeto `psa-data-test-476002`.  
- Dados de exemplo em `dados/` (CSV, TXT, JSON, XML e HTML) para realizar testes locais da pipeline.  

---

## Estrutura de pastas

```
.
├── dados/                  # Fontes brutas usadas pelos loaders
├── docs/                   # Espaço reservado para documentação adicional
├── logs/                   # Arquivos gerados em runtime (ex.: pipeline_ingestao.csv)
├── scripts/
│   ├── pipeline_ingestao.py   # Pipeline orientada a objetos para cargas raw/formatted
│   ├── data_loader_new.py     # Versão alternativa/experimental do loader
│   └── data_quality.sql       # Queries auxiliares de validação
├── sql/                    
│   ├── arquitetura_camadas.py   # Arquivo com os scripts de criação das tabelas nivel silver
│   ├── ddl_bronze_scripts.py    # Arquivo com as tabelas nivel bronze
│   └── queries_otimizadas.sql   # Views analiticas e sintéticas da camada gold
└── requirements.txt        # Dependências Python alinhadas ao GCP
```

---

## Linhagem de Dados (Bronze → Gold)

```mermaid
---
config:
  layout: fixed
---
flowchart LR
 subgraph Bronze["psa_raw (Bronze)"]
        clientes_raw["clientes"]
        analises_raw["analises_tributarias"]
        tarefas_raw["tarefas_projetos"]
        notas_raw["notas_fiscais_json"]
        logs_raw["logs_sistema_table"]
  end
 subgraph Silver["psa_curated (Silver)"]
        log_cur["log_sistema"]
        cliente_cur["cliente"]
        analise_cur["analise_tributaria"]
        projeto_cur["projeto"]
        tarefa_cur["tarefa"]
        nota_cur["nota_fiscal"]
        item_cur["nota_fiscal_item"]
        imposto_cur["nota_fiscal_imposto"]
  end
 subgraph Gold["psa_analytics (Gold)"]
        vw_anl_analise["vw_anl_analise_tributaria"]
        vw_anl_nf["vw_anl_nota_fiscal"]
        vw_anl_proj["vw_anl_projetos"]
        vw_snt_nf["vw_snt_notas_fiscais"]
        vw_snt_analises["vw_snt_analises_tributarias"]
        vw_snt_perf["vw_snt_projetos_performance"]
  end
    clientes_raw --> cliente_cur
    analises_raw --> analise_cur
    tarefas_raw --> projeto_cur & tarefa_cur
    notas_raw --> nota_cur & item_cur & imposto_cur
    logs_raw --> log_cur
    cliente_cur --> vw_anl_analise & vw_anl_proj & vw_snt_analises & vw_snt_perf
    analise_cur --> vw_anl_analise & vw_snt_analises
    nota_cur --> vw_anl_nf
    item_cur --> vw_anl_nf
    imposto_cur --> vw_anl_nf
    projeto_cur --> vw_anl_proj & vw_snt_perf
    tarefa_cur --> vw_anl_proj
    vw_anl_nf --> vw_snt_nf
```

---

## Diagrama ER – Camada Silver (`psa_curated`)

```mermaid
---
config:
  theme: dark
  look: classic
---
erDiagram
    CLIENTE {
        STRING id_cliente PK
        STRING razao_social
        STRING cnpj
        STRING setor
        STRING cidade
        STRING estado
        DATE data_cadastro
        TIMESTAMP data_tratamento
    }
    ANALISE_TRIBUTARIA {
        STRING id_analise PK
        STRING id_cliente FK
        STRING tipo_tributo
        INT64 ano
        STRING periodo_tipo
        INT64 periodo_valor
        FLOAT64 valor_identificado
        STRING observacoes
        TIMESTAMP data_tratamento
    }
    PROJETO {
        STRING id_projeto PK
        STRING nome_projeto
        STRING id_cliente FK
        STRING responsavel
        DATE data_inicio
        DATE data_prevista_fim
        STRING status
        FLOAT64 valor_projeto
        INT64 horas_totais_estimadas
        INT64 horas_totais_realizadas
    }
    TAREFA {
        STRING id_tarefa PK
        STRING id_projeto FK
        STRING descricao
        DATE data_inicio
        DATE data_prevista_fim
        STRING responsavel_tarefa
        STRING status_tarefa
        INT64 horas_estimadas
        INT64 horas_realizadas
    }
    NOTA_FISCAL {
        STRING id_nota PK
        STRING numero_nota
        DATE data_emissao
        STRING id_cliente FK
        FLOAT64 valor_servico
        FLOAT64 valor_total
    }
    NOTA_FISCAL_IMPOSTO {
        STRING id_imposto PK
        STRING id_nota FK
        STRING tipo_imposto
        FLOAT64 valor
    }
    NOTA_FISCAL_ITEM {
        STRING id_item PK
        STRING id_nota FK
        STRING descricao
        INT64 quantidade
        FLOAT64 valor_unitario
    }
    CLIENTE ||--o{ ANALISE_TRIBUTARIA : "possui"
    CLIENTE ||--o{ PROJETO : "realiza"
    CLIENTE ||--o{ NOTA_FISCAL : "emite"
    PROJETO ||--o{ TAREFA : "contém"
    NOTA_FISCAL ||--o{ NOTA_FISCAL_IMPOSTO : "inclui"
    NOTA_FISCAL ||--o{ NOTA_FISCAL_ITEM : "contém"
```

---

## Próximos passos e melhorias sugeridas

- **Orquestração/Agendamento:** integrar a pipeline no Apache Airflow ou pelo Google Cloud.  
- **Testes automatizados:** adicionar testes unitários para os loaders e mocks do BigQuery, além de smoke tests SQL (`bq dry-run`) em CI.  
- **Observabilidade expandida:** integrar o log CSV com Cloud Logging/Monitoring e alertas para falhas críticas nos loaders ou checagens de qualidade.  
