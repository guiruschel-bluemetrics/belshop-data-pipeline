SELECT 
    CAST(L.`Codigo da Empresa` AS INTEGER) AS `Codigo da Empresa`,
    CAST(L.`Codigo da Venda` AS INTEGER) AS `Codigo da Venda`,
    L.`Serie da Venda`,
    CAST(L.`Codigo do Cliente` AS INTEGER) AS `Codigo do Cliente`,
    CAST(L.`Codigo do Vendedor` AS INTEGER) AS `Codigo do Vendedor`,
    CAST(L.`Data da Venda` AS DATE) AS `Data da Venda`,
    L.`Valor da Venda Bruta Total`,
    L.`Valor em Vale-Troca Total`,
    L.`Valor da Venda Liquida Total`,
    L.`Quantidade Venda Bruta Total`,
    L.`Quantidade Troca Total`,
    L.`Quantidade Venda Liquida Total`,
    L.`Custo da Venda Total`,
    L.`Custo de Troca Total`,
    L.`Frete`,
    CAST(L.`Codigo do Produto` AS INTEGER) AS `Codigo do Produto`,
    L.`Quantidade Venda Itens`,
    L.`Valor da Venda Itens`,
    L.`Custo da Venda Itens`,
    L.`Quantidade Troca Itens`,
    L.`Valor de Troca Itens`,
    L.`Custo de Troca Itens`,
    L.`Hora`,
    L.`Quantidade Venda Liquida Itens`,
    L.`Valor da Venda Liquida Itens`,
    L.`Custo da Venda Liquida Itens`,
    L.`UK Venda`,
    L.`Faixa de Valor`,
    L.`Mes de Referencia`,
    L.`FK Cliente`,
    L.`FK Vendedores`,
    L.`FK Empresas`,
    L.`FK Produtos`,
    L.`Sistema Origem`,
    L.`Operacao`,
    L.`Desconto Total`
FROM vendas_linx_principal AS L

UNION ALL

SELECT 
    T.`Codigo_da_Empresa` AS `Codigo da Empresa`,
    T.`Codigo_da_Venda` AS `Codigo da Venda`,
    T.`Serie` AS `Serie da Venda`,
    T.`Codigo_do_Cliente` AS `Codigo do Cliente`,
    T.`Codigo_do_Profissional` AS `Codigo do Vendedor`,
    CAST(T.`Data` AS DATE) AS `Data da Venda`,
    T.`Valor_Faturado_Bruto` AS `Valor da Venda Bruta Total`,
    0 AS `Valor em Vale-Troca Total`,
    T.`Valor_Faturado_Bruto` AS `Valor da Venda Liquida Total`,
    T.`Quantidade_Venda_Itens` AS `Quantidade Venda Bruta Total`,
    0 AS `Quantidade Troca Total`,
    T.`Quantidade_Venda_Itens` AS `Quantidade Venda Liquida Total`,
    T.`Comissao_Total` AS `Custo da Venda Total`,
    0 AS `Custo de Troca Total`,
    0 AS `Frete`,
    T.`Codigo_do_Servico` AS `Codigo do Produto`,
    T.`Quantidade_Venda_Itens` AS `Quantidade Venda Itens`,
    T.`Valor_Faturado_Bruto` AS `Valor da Venda Itens`,
    T.`Comissao_Total` AS `Custo da Venda Itens`,
    0 AS `Quantidade Troca Itens`,
    0 AS `Valor de Troca Itens`,
    0 AS `Custo de Troca Itens`,
    HOUR(`Data`) AS `Hora`,
    T.`Quantidade_Venda_Itens` AS `Quantidade Venda Liquida Itens`,
    T.`Valor_Faturado_Bruto` AS `Valor da Venda Liquida Itens`,
    T.`Comissao_Total` AS `Custo da Venda Liquida Itens`,
    CONCAT(`Codigo_da_Venda`, '-', `Codigo_da_Empresa`, '-', CAST(`Data` AS STRING)) AS `UK Venda`,
    CASE
        WHEN `Valor_Faturado_Bruto` >= 0   AND `Valor_Faturado_Bruto` < 25  THEN 'FAIXA 1 - ATÉ R$ 25'
        WHEN `Valor_Faturado_Bruto` >= 25  AND `Valor_Faturado_Bruto` < 50  THEN 'FAIXA 2 - ENTRE R$ 25 E R$ 50'
        WHEN `Valor_Faturado_Bruto` >= 50  AND `Valor_Faturado_Bruto` < 75  THEN 'FAIXA 3 - ENTRE R$ 50 E R$ 75'
        WHEN `Valor_Faturado_Bruto` >= 75  AND `Valor_Faturado_Bruto` < 100 THEN 'FAIXA 4 - ENTRE R$ 75 E R$ 100'
        WHEN `Valor_Faturado_Bruto` >= 100 AND `Valor_Faturado_Bruto` < 250 THEN 'FAIXA 5 - ENTRE R$ 100 E R$ 250'
        WHEN `Valor_Faturado_Bruto` >= 250 AND `Valor_Faturado_Bruto` < 500 THEN 'FAIXA 6 - ENTRE R$ 250 E R$ 500'
        WHEN `Valor_Faturado_Bruto` >= 500 THEN 'FAIXA 7 - ACIMA DE R$ 500'
    END AS `Faixa de Valor`,
    TRUNC(T.`Data`, 'MM') AS `Mes de Referencia`,
    CONCAT_WS('||', 'TRINKS', `Codigo_do_Cliente`) AS `FK Cliente`,
    CONCAT_WS('||', 'TRINKS', `Codigo_do_Profissional`) AS `FK Vendedores`,
    T.`Codigo_da_Empresa` AS `FK Empresas`,
    CONCAT_WS('||', 'TRINKS', `Codigo_do_Servico`) AS `FK Produtos`,
    'TRINKS' AS `Sistema Origem`,
    'PRESTAÇÃO SERVIÇOS' AS `Operacao`,
    T.`Valor_Total_Desconto` AS `Desconto Total`
FROM vendas_trinks AS T