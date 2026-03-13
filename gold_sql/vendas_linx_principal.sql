SELECT
    V.*,
    HOUR(V.`Data da Venda`) AS HORA,
    V.`Quantidade Venda Itens` - V.`Quantidade Troca Itens` AS `Quantidade Venda Liquida Itens`,
    V.`Valor da Venda Itens` - V.`Valor de Troca Itens` AS `Valor da Venda Liquida Itens`,
    V.`Custo da Venda Itens` - V.`Custo de Troca Itens` AS `Custo da Venda Liquida Itens`,
    CONCAT(
        V.`Codigo da Venda`,
        '-',
        V.`Codigo da Empresa`,
        '-',
        CAST(V.`Data da Venda` AS STRING)
    ) AS `UK Venda`,
    CASE
        WHEN V.`Valor da Venda Liquida Total` >= 500 THEN 'FAIXA 7 - ACIMA DE R$ 500'
        WHEN V.`Valor da Venda Liquida Total` >= 250 AND V.`Valor da Venda Liquida Total` < 500 THEN 'FAIXA 6 - ENTRE R$ 250 E R$ 500'
        WHEN V.`Valor da Venda Liquida Total` >= 100 AND V.`Valor da Venda Liquida Total` < 250 THEN 'FAIXA 5 - ENTRE R$ 100 E R$ 250'
        WHEN V.`Valor da Venda Liquida Total` >= 75 AND V.`Valor da Venda Liquida Total` < 100 THEN 'FAIXA 4 - ENTRE R$ 75 E R$ 100'
        WHEN V.`Valor da Venda Liquida Total` >= 50 AND V.`Valor da Venda Liquida Total` < 75 THEN 'FAIXA 3 - ENTRE R$ 50 E R$ 75'
        WHEN V.`Valor da Venda Liquida Total` >= 25 AND V.`Valor da Venda Liquida Total` < 50 THEN 'FAIXA 2 - ENTRE R$ 25 E R$ 50'
        WHEN V.`Valor da Venda Liquida Total` >= 0 AND V.`Valor da Venda Liquida Total` < 25 THEN 'FAIXA 1 - ATÉ R$ 25'
    END AS `Faixa de Valor`,
    TRUNC(CAST(V.`Data da Venda` AS DATE), 'MM') AS `Mes de Referencia`,
    CONCAT('MICROVIX', '||', V.`Codigo do Cliente`) AS `FK Cliente`,
    CONCAT('MICROVIX', '||', V.`Codigo do Vendedor`) AS `FK Vendedores`,
    V.`Codigo da Empresa` AS `FK Empresas`,
    CONCAT('MICROVIX', '||', V.`Codigo do Produto`) AS `FK Produtos`, 
    'MICROVIX' AS `Sistema Origem`,
    'VENDA PRODUTOS' AS Operacao,
    0 AS `Desconto Total`
FROM
    (
        SELECT * FROM vendas_linx_principal_2021
        UNION ALL
        SELECT * FROM itens_vendas_2021
        UNION ALL
        SELECT * FROM itens_trocas_2021
        UNION ALL
        SELECT * FROM aux_vendas_linx_2020_2021 --csv
    ) V