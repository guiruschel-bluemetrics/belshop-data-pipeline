SELECT
    V.`Codigo do Cliente` AS id_cliente,
    SUM(V.`Valor da Venda Bruta Total`) / COUNT(DISTINCT V.`Codigo da Venda`) AS valor_medio
FROM
    vendas_linx_principal V
WHERE
    CAST(V.`Data da Venda` AS DATE) > ADD_MONTHS(CURRENT_DATE(), -12)
GROUP BY
    V.`Codigo do Cliente`