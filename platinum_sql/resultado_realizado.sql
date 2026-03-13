-- DESPESAS REALIZADAS
SELECT 
    D.`codigo_empresa`,
    D.`num_fatura`,
    D.`data_emissao`,
    D.`data_vencimento`,
    D.`data_baixa`,
    D.`valor_pago` * -1 AS `Valor`,
    D.`doc_excluido`,
    D.`doc_cancelado`,
    D.`cod_historico`,
    D.`mes_referencia`,
    D.`cod_grupo_historico`,
    D.`nome_grupo_historico`,
    D.`nome_historico`
FROM despesas_realizadas AS D

UNION ALL

-- RECEITAS REALIZADAS
SELECT 
    R.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    R.`Venda Realizada`,
    '',
    '',
    0,
    R.`Mes de Referencia`,
    R.`cod_grupo_historico`,
    '',
    ''
FROM receitas_realizadas AS R

UNION ALL

-- PERDAS (2% SOBRE A VENDA DE LOJA, SEM SALÃO)
SELECT 
    R.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    R.`Venda Realizada` * -2.0 / 100.0,
    '',
    '',
    0,
    R.`Mes de Referencia`,
    19,
    '',
    ''
FROM receitas_realizadas AS R
WHERE R.`cod_grupo_historico` = 14

UNION ALL

-- TAXA ADM CARTÕES (1% SOBRE A VENDA DE LOJA+SALÃO)
SELECT 
    R.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    R.`Venda Realizada` * -1.0 / 100.0,
    '',
    '',
    0,
    R.`Mes de Referencia`,
    20,
    '',
    ''
FROM receitas_realizadas AS R

UNION ALL

-- RATEIO DESPESAS CD
SELECT 
    C.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    C.`Contrib_Loja_Despesa_CD` * -1 AS `Valor`,
    '',
    '',
    0,
    C.`Mes`,
    21,
    '',
    ''
FROM contribuicao_despesas_cd AS C

UNION ALL

-- RATEIO DESPESAS CD SEM COMPRAS
SELECT 
    C.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    C.`Contrib_Loja_Despesa_CD_Sem_Compras` * -1 AS `Valor`,
    '',
    '',
    0,
    C.`Mes`,
    24,
    '',
    ''
FROM contribuicao_despesas_cd AS C

UNION ALL

-- CUSTO SERVIÇOS (FIXO EM 66.3%)
SELECT 
    R.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    R.`Venda Realizada` * -66.30 / 100.0,
    '',
    '',
    0,
    R.`Mes de Referencia`,
    22,
    '',
    ''
FROM receitas_realizadas AS R
WHERE R.`cod_grupo_historico` = 15

UNION ALL

-- CUSTO MERCADORIAS VENDIDAS
SELECT 
    R.`Codigo da Empresa`,
    0,
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS TIMESTAMP),
    R.`CMV` * -1,
    '',
    '',
    0,
    R.`Mes de Referencia`,
    23,
    '',
    ''
FROM receitas_realizadas AS R
WHERE R.`cod_grupo_historico` = 14