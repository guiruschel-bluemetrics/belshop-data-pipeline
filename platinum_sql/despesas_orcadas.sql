SELECT 
    V.`Mes de Referencia`,
    V.`Codigo da Empresa`,
    O.nome_do_grupo,
    O.codigo_do_grupo,
    SUM(V.`Valor da Venda Liquida Total`) AS `Venda Realizada`,
    O.percentual_sobre_fat AS `Percentual Orcamento`,
    SUM(V.`Valor da Venda Liquida Total`) * O.percentual_sobre_fat / 100.0 AS `Valor Orcamento`
FROM (
    SELECT 
        P.`Mes de Referencia`,
        P.`Codigo da Empresa`,
        SUM(P.`Valor da Venda Liquida Total`) AS `Valor da Venda Liquida Total`
    FROM vendas AS P
    WHERE P.`Sistema Origem` = 'MICROVIX'
    GROUP BY 
        P.`Mes de Referencia`,
        P.`Codigo da Empresa`
    
    UNION ALL
    
    SELECT 
        S.`Mes de Referencia`,
        S.`Codigo da Empresa`,
        SUM(S.`Valor da Venda Liquida Total`) - SUM(S.`Custo da Venda Total`) AS `Valor da Venda Liquida Total`
    FROM vendas AS S
    WHERE S.`Sistema Origem` = 'TRINKS'
    GROUP BY 
        S.`Mes de Referencia`,
        S.`Codigo da Empresa`
) AS V
CROSS JOIN fin_orcamento_grupo_sheets AS O
GROUP BY 
    V.`Mes de Referencia`,
    V.`Codigo da Empresa`,
    O.nome_do_grupo,
    O.percentual_sobre_fat,
    O.codigo_do_grupo