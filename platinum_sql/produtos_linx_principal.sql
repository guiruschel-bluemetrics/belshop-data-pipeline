SELECT 
    UPPER(RTRIM(P.CODIGO)) AS `Codigo`, 
    UPPER(P.NOME) AS `Nome`, 
    UPPER(P.GRUPO) AS `Grupo`,
    UPPER(P.SUBGRUPO) AS `Subgrupo`,
    UPPER(P.COLECAO) AS `Colecao`,
    UPPER(P.FORNECEDOR) AS `Fabricante`,
    UPPER(P.MARCA) AS `Marca`,
    UPPER(P.LINHA) AS `Linha`,
    P.codigo_comprador,
    P.ULTIMO_CUSTO AS `Ultimo Custo`,
    P.Codigo_barras,
    -- 1. Data Primeira Venda (Lookup)
    AUX.DATA_PRIMEIRA_VENDA AS `Data Primeira Venda`,
    -- 2. Dias Desde Primeira Venda (DayDiff)
    DATEDIFF(current_date(), CAST(AUX.DATA_PRIMEIRA_VENDA AS DATE)) AS `Dias Desde Primeira Venda`,
    -- 3. Codigo e Nome concatenados
    CONCAT(UPPER(RTRIM(P.CODIGO)), ' - ', UPPER(P.NOME)) AS `Produto Completo`
FROM PRODUTOS AS P
LEFT JOIN usuarios AS U ON U.codigo = P.codigo_comprador
LEFT JOIN aux_produtos_venda_linx_principal AS AUX ON P.CODIGO = AUX.id_produto