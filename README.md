# Teste Engenheiro de Dados

## Visão geral da solução
- **Ingestão Raw**: `scripts/pipeline_ingestao.py` realiza cargas batch dos arquivos disponíveis em `dados/` diretamente para tabelas `psa_raw.*` no BigQuery, mantendo cópias em formato bruto e alternativas estruturadas via BeautifulSoup e xmltodict.
- **Modelagem Curated**: os arquivos em `sql/` descrevem a arquitetura em camadas (ex.: `arquitetura_camadas.sql`, `ddl_scripts.sql`, `queries_otimizadas.sql`), criando tabelas particionadas/clusterizadas e chaves referenciais na camada `psa_curated`.
- **Qualidade e Monitoramento**: `data_quality.py` e `scripts/data_quality.sql` encapsulam regras críticas e altas para checar consistência referencial, obrigatoriedade de campos e coerência temporal. Logs operacionais ficam em `logs/pipeline_ingestao.csv`.
- **Credenciais e Segurança**: a autenticação ocorre via variáveis de ambiente do Google Cloud (`GOOGLE_APPLICATION_CREDENTIALS`) apontando para a conta de serviço `psa-data-test-476002-902991aa98ac.json`.

## Como executar os scripts
1. **Preparar ambiente Python**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
2. **Configurar credenciais do GCP**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/psa-data-test-476002-902991aa98ac.json
   export GOOGLE_CLOUD_PROJECT=psa-data-test-476002
   ```
3. **Executar a ingestão raw**
   ```bash
   python scripts/pipeline_ingestao.py
   ```
   - O script percorre a lista de loaders definida em `main()`, grava logs e envia DataFrames ao BigQuery com timeout de 300s.
4. **Rodar checagens de qualidade**
   ```bash
   python data_quality.py \
     --project-id psa-data-test-476002 \
     --dataset psa_curated \
     --output logs/data_quality.json
   ```
   - Os parâmetros são opcionais (`psa_curated` é o default). O arquivo de saída conterá o resumo em JSON das violações.
5. **Aplicar DDLs/consultas**
   - Utilize o `bq query --use_legacy_sql=false < sql/arquitetura_camadas.sql` ou execute cada bloco pelo BigQuery UI para criar as camadas curadas e constraints não-enforced.

## Dependências e pré-requisitos
- Python 3.10+ (o projeto usa 3.13 na venv local).
- Conta de serviço com permissões de `BigQuery Data Editor` e `BigQuery Job User`.
- Bibliotecas listadas em `requirements.txt` (pandas, google-cloud-bigquery, pandas-gbq, BeautifulSoup4, xmltodict etc.).
- Acesso aos datasets `psa_raw` e `psa_curated` dentro do projeto `psa-data-test-476002`.
- Dados de exemplo em `dados/` (CSV, TXT, JSON, XML e HTML) para realizar testes locais da pipeline.

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
├── sql/                    # DDLs, arquitetura em camadas e consultas otimizadas
├── data_quality.py         # CLI para checagens automatizadas no BigQuery
├── data_quality.sql        # Relatórios de qualidade consolidados
├── requirements.txt        # Dependências Python alinhadas ao GCP
├── psa-data-test-476002-902991aa98ac.json  # Credenciais de serviço (não versionar)
└── venv/                   # Ambiente virtual (ignorar em VCS)
```

## Próximos passos e melhorias sugeridas
1. **Orquestração/Agendamento**: empacotar o pipeline em Cloud Composer ou Cloud Run Jobs com Pub/Sub para garantir execuções recorrentes e observabilidade.
2. **Parametrização de loaders**: mover a lista de tabelas/fontes para um YAML ou tabela de controle no BigQuery, permitindo adicionar novas fontes sem alterar código.
3. **Testes automatizados**: adicionar testes unitários para os loaders e mocks do BigQuery, além de smoke tests de SQL (ex.: `bq dry-run`) em CI.
4. **Observabilidade expandida**: integrar o log em CSV com Cloud Logging/Monitoring e alertas para falhas críticas nos loaders ou checagens de qualidade.
5. **Camada de transformação incremental**: implementar jobs dbt ou Dataform sobre `psa_curated` para gerar data marts (psa_mart) e métricas derivadas.

