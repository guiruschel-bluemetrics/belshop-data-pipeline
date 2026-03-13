SELECT 
    UPPER(RTRIM(V.CODIGO_EMPRESA))     AS `Codigo da Empresa`,
    V.DATA_VENDA                       AS `Data da Venda`,
    SUM(V.VALOR_VENDA_BRUTA_TOTAL)     AS `Valor da Venda Bruta Total`,
    SUM(V.VALOR_TROCA_TOTAL)           AS `Valor de Troca Total`,
    SUM(V.DESCONTO_TOTAL)              AS `Desconto Total`,
    SUM(V.VALOR_VENDA_LIQUIDA_TOTAL)   AS `Valor da Venda Liquida Total`,
    SUM(V.QTDE_VENDA_BRUTA_TOTAL)      AS `Quantidade Venda Bruta Total`,
    SUM(V.QTDE_TROCA_TOTAL)            AS `Quantidade Troca Total`,
    SUM(V.QTDE_VENDA_LIQUIDA_TOTAL)    AS `Quantidade Venda Liquida Total`
FROM VENDAS V
WHERE V.DATA_VENDA >= CURRENT_DATE - INTERVAL '5 years'
GROUP BY 
    UPPER(RTRIM(V.CODIGO_EMPRESA)),
    V.DATA_VENDA;
