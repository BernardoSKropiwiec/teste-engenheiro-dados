-- Query: Resumo de tributos por cliente e estado
-- Objetivo: gerar visão analítica da soma de impostos por cliente e estado
-- Camadas envolvidas: Gold (psa_analytics)

CREATE OR REPLACE TABLE psa_analytics.resumo_clientes_tributos AS
SELECT
  c.id_cliente,
  c.razao_social,
  c.estado,
  COUNT(DISTINCT nf.id_nota) AS qtd_notas,
  SUM(nf.valor_total) AS valor_total_vendas,
  SUM(imp.valor) AS valor_total_impostos,
  AVG(at.valor_identificado) AS media_valor_identificado
FROM
  `psa_curated.clientes` AS c
JOIN
  `psa_curated.notas_fiscais` AS nf
  ON c.id_cliente = nf.cliente_id
JOIN
  `psa_curated.nota_fiscal_imposto` AS imp
  ON nf.id_nota = imp.id_nota
LEFT JOIN
  `psa_curated.analises_tributarias` AS at
  ON c.id_cliente = at.cliente_id
WHERE
  nf.data_emissao BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY
  c.id_cliente, c.razao_social, c.estado
ORDER BY
  valor_total_vendas DESC;


-- 1. Filtro temporal (BETWEEN): restringe o escopo de dados para reduzir volume processado.
-- 2. JOINs diretos: usa chaves de relacionamento já padronizadas na camada curated.
-- 3. SUM e COUNT agregam dados, reduzindo o tamanho final do resultado.
-- 5. PARTITION BY DATE(data_emissao) na tabela de notas_fiscais
--    → acelera consultas temporais e reduz custo de leitura.
-- 6. Uso de LEFT JOIN em analises_tributarias:
--    → garante clientes sem análises, evitando perda de registros relevantes.
-- 7. Ordenação final por valor_total_vendas DESC facilita análise de top clientes.


CREATE OR REPLACE VIEW `psa_analytics.performance_projetos` AS
SELECT 
  pro.id_projeto,
  cli.id_cliente,
  initcap(cli.razao_social) as nome_cliente,
  pro.responsavel,
  pro.status,
  pro.horas_totais_estimadas,
  pro.horas_totais_realizadas,
  tar.descricao as tarefa_longa,
  max(tar.tot_hr_tarefa) hrs_tarefa
  FROM 
    `psa_curated.projeto`      pro
    join (select 
            t.descricao,
            sum(t.horas_realizadas) as tot_hr_tarefa,
            t.id_projeto
          from psa_curated.tarefa t
        group by 
          t.descricao,
          t.id_projeto) as   tar on pro.id_projeto = tar.id_projeto
    join `psa_curated.cliente` cli on pro.id_cliente = cli.id_cliente
where 
  pro.status <> 'Cancelado'
group by 
    pro.id_projeto,
    cli.id_cliente,
    cli.razao_social,
    pro.responsavel,
    pro.horas_totais_estimadas,
    pro.horas_totais_realizadas,
    tar.descricao,
    pro.status