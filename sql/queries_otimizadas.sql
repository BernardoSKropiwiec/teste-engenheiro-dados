--Views da camada Gold--

--=========View Analítica de análises tributárias========================
--= Criada por: Bernardo S. Kropiwiec                         25/10/2025=
--=Descrição: consolida informações de análises tributárias por cliente.=
--=======================================================================
CREATE OR REPLACE view  psa_analytics.vw_anl_analise_tributaria as 
SELECT 
  ant.id_analise,
  INITCAP(cli.razao_social) as cliente,
  ant.tipo_tributo,
  ant.ano as ano_analise,
  ant.periodo_tipo as tipo_periodo_analise,
  ant.periodo_valor,
  ant.valor_identificado,
  ant.observacoes
FROM 
  `psa_curated.analise_tributaria` ant
  join `psa_curated.cliente`       cli on ant.id_cliente = cli.id_cliente


--== View Analítica de notas fiscais ==================================
--=Criada por: Bernardo S. Kropiwiec                        25/10/2025=
--=Descrição: consolida valores de notas fiscais com itens e impostos.= 
--=====================================================================
CREATE OR REPLACE view  psa_analytics.vw_anl_nota_fiscal as 
WITH base AS (
  SELECT
    nf.id_nota,
    nf.id_cliente,
    nf.numero_nota,
    nf.data_emissao,
    nf.valor_servico,
    nf.valor_total,
    SUM(it.quantidade * it.valor_unitario) AS valor_itens,
    SUM(CASE WHEN imp.tipo_imposto = 'ISS' THEN imp.valor ELSE 0 END) AS valor_iss,
    SUM(CASE WHEN imp.tipo_imposto = 'PIS' THEN imp.valor ELSE 0 END) AS valor_pis,
    SUM(CASE WHEN imp.tipo_imposto = 'COFINS' THEN imp.valor ELSE 0 END) AS valor_cofins
  FROM psa_curated.nota_fiscal AS nf
  LEFT JOIN psa_curated.nota_fiscal_item AS it
    ON nf.id_nota = it.id_nota
  LEFT JOIN psa_curated.nota_fiscal_imposto AS imp
    ON nf.id_nota = imp.id_nota
  GROUP BY nf.id_nota, nf.id_cliente, nf.numero_nota, nf.data_emissao, nf.valor_servico, nf.valor_total
)
SELECT
  id_nota,
  id_cliente,
  numero_nota,
  data_emissao,
  valor_total,
  valor_servico,
  valor_itens,
  valor_iss,
  valor_pis,
  valor_cofins
FROM base


--============== View Analítica de projetos ===========================
-- Criada por: Bernardo S. Kropiwiec                        25/10/2025=
-- Descrição: detalha projetos e tarefas associadas a cada cliente.   =
--=====================================================================
create or replace view psa_analytics.vw_anl_projetos as 
select 
  pro.id_projeto,
  pro.nome_projeto,
  pro.responsavel,
  initcap(cli.razao_social) as nome_cliente,
  pro.status,
  pro.data_inicio,
  pro.data_prevista_fim,
  pro.horas_totais_estimadas,
  pro.horas_totais_realizadas,
  trf.id_tarefa,
  trf.descricao,
  trf.responsavel_tarefa,
  trf.status_tarefa,
  trf.data_inicio       as data_inicio_tarefa,
  trf.data_prevista_fim as data_prevista_fim_tarefa,
  trf.horas_estimadas   as horas_estimadas_tarefa,
  trf.horas_realizadas  as horas_realizadas_tarefa
from 
  `psa_curated.projeto`      pro
  join `psa_curated.tarefa`  trf on pro.id_projeto = trf.id_projeto
  join `psa_curated.cliente` cli on pro.id_cliente = cli.id_cliente


--== View Sintética de performance de projetos ========================
--=Criada por: Bernardo S. Kropiwiec                        25/10/2025=
--=Descrição: resume desempenho dos projetos pela tarefa mais longa.  =
--=====================================================================
CREATE OR REPLACE VIEW`psa_analytics.vw_snt_projetos_performance` as 
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


--========View Sintética de análises tributárias ============================
--=Criada por: Bernardo S. Kropiwiec                              25/10/2025=  
--=Descrição: consolida quantidade, tipos e valores de análises por cliente.= 
--===========================================================================  
CREATE OR REPLACE VIEW `psa_analytics.vw_snt_analises_tributarias` AS
SELECT
  cli.id_cliente,
  INITCAP(cli.razao_social) AS nome_cliente,
  COUNT(DISTINCT ant.id_analise) AS qtd_analises,
  COUNT(DISTINCT ant.tipo_tributo) AS qtd_tipos_tributos,
  SUM(ant.valor_identificado) AS valor_total_identificado,
  MAX(ant.valor_identificado) AS maior_valor_identificado,
  MIN(ant.valor_identificado) AS menor_valor_identificado,
  MAX(ant.ano) AS ultimo_ano_analisado
FROM `psa_curated.analise_tributaria` AS ant
JOIN `psa_curated.cliente` AS cli
  ON ant.id_cliente = cli.id_cliente
GROUP BY cli.id_cliente, cli.razao_social;


--===========View Sintética de notas fiscais ==========================
--=Criada por: Bernardo S. Kropiwiec                        25/10/2025=
--=Descrição: resume volume e valores de notas fiscais por cliente.   =
--=====================================================================
CREATE OR REPLACE VIEW `psa_analytics.vw_snt_notas_fiscais` AS
SELECT
  cli.id_cliente,
  INITCAP(cli.razao_social) AS nome_cliente,
  COUNT(DISTINCT nf.id_nota) AS qtd_notas,
  SUM(nf.valor_total) AS valor_total_notas,
  SUM(nf.valor_servico) AS valor_total_servicos,
  SUM(nf.valor_iss) AS total_iss,
  SUM(nf.valor_pis) AS total_pis,
  SUM(nf.valor_cofins) AS total_cofins,
  MAX(nf.data_emissao) AS ultima_emissao
FROM `psa_analytics.vw_anl_nota_fiscal` AS nf
JOIN `psa_curated.cliente` AS cli
  ON nf.id_cliente = cli.id_cliente
GROUP BY cli.id_cliente, cli.razao_social;
