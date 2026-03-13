SELECT 
    P.`Mes de Referencia`,
    P.`Codigo da Empresa`,
    CAST(14 AS DECIMAL(18,2)) AS `cod_grupo_historico`,
    SUM(P.`Valor da Venda Liquida Total`) AS `Venda Realizada`,
    SUM(P.`Custo da Venda Total`) AS `CMV`
FROM vendas AS P
WHERE P.`Sistema Origem` = 'MICROVIX'
GROUP BY 
    P.`Mes de Referencia`,
    P.`Codigo da Empresa`

UNION ALL

SELECT 
    S.`Mes de Referencia`,
    S.`Codigo da Empresa`,
    CAST(15 AS DECIMAL(18,2)) AS `cod_grupo_historico`,
    SUM(S.`Valor da Venda Liquida Total`) AS `Venda Realizada`,
    CAST(0 AS DECIMAL(18,2)) AS `CMV`
FROM vendas AS S
WHERE S.`Sistema Origem` = 'TRINKS'
GROUP BY 
    S.`Mes de Referencia`,
    S.`Codigo da Empresa`