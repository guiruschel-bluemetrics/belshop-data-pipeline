SELECT
    V.`Codigo do Produto` AS id_produto,
    MIN(V.`Data da Venda`) AS data_primeira_venda,
    MAX(V.`Data da Venda`) AS data_ultima_venda
FROM
    vendas_linx_principal V
GROUP BY
    V.`Codigo do Produto`