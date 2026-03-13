-- RECEITAS REALIZADAS POR LOJA
SELECT 
    R.`Codigo da Empresa`,
    R.`Mes de Referencia` AS Mes,
    SUM(R.`Venda Realizada`) AS Valor_Receita_Loja,
    T.Valor AS Valor_Receita_Total,
    SUM(R.`Venda Realizada`) / T.Valor AS Perc_Rec_Total,
    D2.Valor AS Valor_Despesa_CD,
    D2.Valor * SUM(R.`Venda Realizada`) / T.Valor AS Contrib_Loja_Despesa_CD,
    D3.Valor AS Valor_Despesa_CD_Sem_Compras,
    D3.Valor * SUM(R.`Venda Realizada`) / T.Valor AS Contrib_Loja_Despesa_CD_Sem_Compras
FROM receitas_realizadas AS R

LEFT JOIN (
    -- RECEITAS REALIZADAS TOTAL
    SELECT 
        R2.`Mes de Referencia` AS Mes,
        SUM(R2.`Venda Realizada`) AS Valor
    FROM receitas_realizadas AS R2
    GROUP BY R2.`Mes de Referencia`
) AS T ON T.Mes = R.`Mes de Referencia`

LEFT JOIN (
    SELECT 
        d.mes_referencia,
        SUM(D.valor_pago) AS Valor
    FROM despesas_realizadas AS D
    WHERE D.codigo_empresa = '2'
    GROUP BY d.mes_referencia
) AS D2 ON D2.mes_referencia = R.`Mes de Referencia`

LEFT JOIN (
    -- DESPESAS DO CD SEM COMPRAS 
    SELECT 
        d.mes_referencia,
        SUM(D.valor_pago) AS Valor
    FROM despesas_realizadas AS D
    WHERE D.codigo_empresa = '2'
        AND D.cod_historico <> 4 -- Ajustado para coluna existente
    GROUP BY d.mes_referencia
) AS D3 ON D3.mes_referencia = R.`Mes de Referencia`

GROUP BY 
    R.`Codigo da Empresa`,
    R.`Mes de Referencia`,
    T.Valor,
    D2.Valor,
    D3.Valor