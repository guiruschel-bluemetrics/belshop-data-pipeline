SELECT
    A.id_cliente,
    A.data_ultima_compra,
    A.id_vendedor
FROM
    (
        SELECT
            V.`Codigo do Cliente` AS id_cliente,
            V.`Data da Venda` AS data_ultima_compra,
            V.`Codigo do Vendedor` AS id_vendedor,
            ROW_NUMBER() OVER (PARTITION BY V.`Codigo do Cliente` ORDER BY V.`Data da Venda` DESC) AS rnk
        FROM
            vendas_linx_principal V
    ) A
WHERE
    A.rnk = 1
ORDER BY
    A.id_cliente DESC