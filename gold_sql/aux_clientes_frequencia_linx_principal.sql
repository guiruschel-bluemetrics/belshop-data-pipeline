SELECT
    V.`Codigo do Cliente` AS id_cliente,
    COUNT(DISTINCT V.`Codigo da Venda`) AS frequencia
FROM
    vendas_linx_principal V
WHERE
    CAST(V.`Data da Venda` AS DATE) > ADD_MONTHS(CURRENT_DATE(), -12)
GROUP BY
    V.`Codigo do Cliente`