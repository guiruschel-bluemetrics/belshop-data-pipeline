SELECT 
    UPPER(LTRIM(RTRIM(E.CODIGO_PRODUTO)))     AS `Codigo do Produto`,
    UPPER(LTRIM(RTRIM(E.CODIGO_EMPRESA)))     AS `Codigo da Empresa`,
    E.DATA_ULTIMA_SAIDA                       AS `Data Ultima Saida`,
    E.DATA_ULTIMA_ENTRADA                     AS `Data Ultima Entrada`,
    E.DATA_PRIMEIRA_ENTRADA                   AS `Data Primeira Entrada`,
    COALESCE(E.QUANTIDADE, 0)                 AS `Quantidade`,
    E.ULTIMO_CUSTO                            AS `Ultimo Custo`,
    COALESCE(E.CODIGO_DEPOSITO, '0')          AS `Codigo do Deposito`
FROM ESTOQUES E
WHERE COALESCE(E.QUANTIDADE, 0) <> 0;
