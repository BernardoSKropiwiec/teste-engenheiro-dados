# PSA Data API

Este documento descreve a API construída com FastAPI para exposição das views analíticas do projeto PSA.

## Visão Geral

- **Tecnologia:** FastAPI  
- **Fonte de dados:** BigQuery (dataset `psa_analytics`)  
- **Autenticação:** Credenciais de serviço Google Cloud definidas em `GOOGLE_APPLICATION_CREDENTIALS`  
- **Execução local:** `uvicorn scripts.api:app --reload`

### Configuração

| Variável | Descrição | Valor padrão |
| --- | --- | --- |
| `PSA_PROJECT_ID` | ID do projeto GCP | `psa-data-test-476002` |
| `PSA_ANALYTICS_DATASET` | Dataset com as views analíticas | `psa_analytics` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Caminho para o JSON de credenciais | — |

> **Observação:** ajuste as variáveis conforme o ambiente (dev, homologação, produção).

## Endpoints

### `GET /health`

Verifica se a API está disponível.

**Resposta**

```json
{"status": "ok"}
```

### `GET /analytics`



**Resposta**

```json
{
  "dataset": "psa_analytics",
  "views": [
    "vw_anl_analise_tributaria",
    "vw_anl_nota_fiscal",
    "vw_anl_projetos",
    "vw_snt_projetos_performance",
    "vw_snt_analises_tributarias",
    "vw_snt_notas_fiscais"
  ]
}
```

### `GET /analytics/{view_name}`

Consulta registros de uma view específica da camada analytics.

| Parâmetro | Tipo | Obrigatório | Descrição |
| --- | --- | --- | --- |
| `view_name` | string | Sim | Nome da view (ver lista em `/analytics`). |
| `limit` | inteiro | Não | Quantidade máxima de registros (1 a 1000, default 100). |

**Exemplo**

```
GET /analytics/vw_anl_nota_fiscal?limit=50
```

**Resposta**

```json
{
  "dataset": "psa_analytics",
  "view": "vw_anl_nota_fiscal",
  "limit": 50,
  "rows": [
    {
      "id_nota": "NF-001",
      "id_cliente": "CLI-100",
      "valor_total": 1234.56,
      "data_emissao": "2024-01-15T00:00:00",
      ...
    }
  ]
}
```

> **Observação:** campos e tipos variam conforme a definição da view no BigQuery.

## Dicas de Uso

1. Garanta que o arquivo de credenciais (`.json`) está acessível e a variável `GOOGLE_APPLICATION_CREDENTIALS` aponta para ele.  
2. Use a interface automática do FastAPI em `http://localhost:8000/docs` para explorar e testar os endpoints.  
3. Ajuste o parâmetro `limit` para controlar o volume de dados retornado.  
4. Para ambientes diferentes, configure `PSA_PROJECT_ID` e `PSA_ANALYTICS_DATASET` antes de iniciar o servidor.

## Como Executar com Uvicorn

1. Ative o ambiente virtual:  
   ```bash
   source venv/bin/activate
   ```
2. Inicie o servidor FastAPI com recarregamento automático:  
   ```bash
   uvicorn scripts.api:app --reload
   ```
3. Acesse `http://localhost:8000/docs` para testar e inspecionar os endpoints.
