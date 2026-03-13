/* Bloco 1: LINX */
SELECT 
    CAST('LINX' AS STRING) AS `origem`,
    CAST(L.`Codigo da Empresa` AS STRING) AS `Codigo da Empresa`,
    CAST(L.`Codigo da Venda` AS STRING) AS `Codigo da Venda`,
    CAST(L.`Serie da Venda` AS STRING) AS `Serie da Venda`,
    CAST(L.`Codigo do Cliente` AS STRING) AS `Codigo do Cliente`,
    CAST(L.`Codigo do Vendedor` AS STRING) AS `Codigo do Vendedor`,
    CAST(L.`Data da Venda` AS TIMESTAMP) AS `Data da Venda`,
    CAST(L.`Valor da Venda Bruta Total` AS DECIMAL(14,2)) AS `Valor da Venda Bruta Total`,
    CAST(L.`Valor em Vale-Troca Total` AS DECIMAL(14,2)) AS `Valor em Vale-Troca Total`,
    CAST(L.`Valor da Venda Liquida Total` AS DECIMAL(15,2)) AS `Valor da Venda Liquida Total`,
    CAST(L.`Quantidade Venda Bruta Total` AS INTEGER) AS `Quantidade Venda Bruta Total`,
    CAST(L.`Quantidade Troca Total` AS INTEGER) AS `Quantidade Troca Total`,
    CAST(L.`Quantidade Venda Liquida Total` AS INTEGER) AS `Quantidade Venda Liquida Total`,
    CAST(L.`Custo da Venda Total` AS DECIMAL(14,2)) AS `Custo da Venda Total`,
    CAST(L.`Custo de Troca Total` AS DECIMAL(14,2)) AS `Custo de Troca Total`,
    CAST(L.`Frete` AS DECIMAL(14,2)) AS `Frete`,
    CAST(L.`Codigo do Produto` AS STRING) AS `Codigo do Produto`,
    CAST(L.`Quantidade Venda Itens` AS DECIMAL(14,2)) AS `Quantidade Venda Itens`,
    CAST(L.`Valor da Venda Itens` AS DECIMAL(14,2)) AS `Valor da Venda Itens`,
    CAST(L.`Custo da Venda Itens` AS DECIMAL(14,2)) AS `Custo da Venda Itens`,
    CAST(L.`Quantidade Troca Itens` AS DECIMAL(14,2)) AS `Quantidade Troca Itens`,
    CAST(L.`Valor de Troca Itens` AS DECIMAL(14,2)) AS `Valor de Troca Itens`,
    CAST(L.`Custo de Troca Itens` AS DECIMAL(14,2)) AS `Custo de Troca Itens`,
    CAST(L.`Hora` AS INTEGER) AS `Hora`,
    CAST(L.`Quantidade Venda Liquida Itens` AS DECIMAL(15,2)) AS `Quantidade Venda Liquida Itens`,
    CAST(L.`Valor da Venda Liquida Itens` AS DECIMAL(15,2)) AS `Valor da Venda Liquida Itens`,
    CAST(L.`Custo da Venda Liquida Itens` AS DECIMAL(15,2)) AS `Custo da Venda Liquida Itens`,
    CAST(L.`UK Venda` AS STRING) AS `UK Venda`,
    CAST(L.`Faixa de Valor` AS STRING) AS `Faixa de Valor`,
    CAST(L.`Mes de Referencia` AS DATE) AS `Mes de Referencia`,
    CAST(L.`FK Cliente` AS STRING) AS `FK Cliente`,
    CAST(L.`FK Vendedores` AS STRING) AS `FK Vendedores`,
    CAST(L.`FK Empresas` AS STRING) AS `FK Empresas`,
    CAST(L.`FK Produtos` AS STRING) AS `FK Produtos`,
    CAST(L.`Sistema Origem` AS STRING) AS `Sistema Origem`,
    CAST(L.`Operacao` AS STRING) AS `Operacao`,
    CAST(L.`Desconto Total` AS INTEGER) AS `Desconto Total`
FROM vendas_linx_principal AS L

UNION ALL

/* Bloco 2: TRINKS */
SELECT 
    CAST('TRINKS' AS STRING) AS `origem`,
    --CAST(T.`Codigo_da_Empresa_Microvix` AS STRING) AS Codigo da Empresa,
    regexp_replace(CAL.codigo_microvix, '^#', '') AS `Codigo da Empresa`,
    CAST(T.`codigo_da_venda` AS STRING) AS `Codigo da Venda`,
    CAST(T.`serie` AS STRING) AS `Serie da Venda`,
    CAST(T.`codigo_do_cliente` AS STRING) AS `Codigo do Cliente`,
    CAST(T.`codigo_do_profissional` AS STRING) AS `Codigo do Vendedor`,
    CAST(T.`data` AS TIMESTAMP) AS `Data da Venda`,
    CAST(T.`valor_faturado_bruto` AS DECIMAL(38,2)) AS `Valor da Venda Bruta Total`,
    CAST(0 AS DECIMAL(38,2)) AS `Valor em Vale-Troca Total`,
    CAST(T.`valor_faturado_bruto` AS DECIMAL(38,2)) AS `Valor da Venda Liquida Total`,
    CAST(T.`quantidade_venda_itens` AS INTEGER) AS `Quantidade Venda Bruta Total`,
    CAST(0 AS INTEGER) AS `Quantidade Troca Total`,
    CAST(T.`quantidade_venda_itens` AS INTEGER) AS `Quantidade Venda Liquida Total`,
    CAST(T.`comissao_total` AS DECIMAL(38,2)) AS `Custo da Venda Total`,
    CAST(0 AS DECIMAL(38,2)) AS `Custo de Troca Total`,
    CAST(0 AS DECIMAL(38,2)) AS `Frete`,
    CAST(T.`codigo_do_servico` AS STRING) AS `Codigo do Produto`,
    CAST(T.`quantidade_venda_itens` AS DECIMAL(38,2)) AS `Quantidade Venda Itens`,
    CAST(T.`valor_faturado_bruto` AS DECIMAL(38,2)) AS `Valor da Venda Itens`,
    CAST(T.`comissao_total` AS DECIMAL(38,2)) AS `Custo da Venda Itens`,
    CAST(0 AS DECIMAL(38,2)) AS `Quantidade Troca Itens`,
    CAST(0 AS DECIMAL(38,2)) AS `Valor de Troca Itens`,
    CAST(0 AS DECIMAL(38,2)) AS `Custo de Troca Itens`,
    CAST(HOUR(CAST(T.`data` AS TIMESTAMP)) AS INTEGER) AS `Hora`,
    CAST(T.`quantidade_venda_itens` AS DECIMAL(38,2)) AS `Quantidade Venda Liquida Itens`,
    CAST(T.`valor_faturado_bruto` AS DECIMAL(38,2)) AS `Valor da Venda Liquida Itens`,
    CAST(T.`comissao_total` AS DECIMAL(38,2)) AS `Custo da Venda Liquida Itens`,
    CAST(CONCAT(CAST(T.`codigo_da_venda` AS STRING), '-', CAST(T.`codigo_da_empresa` AS STRING), '-', CAST(T.`data` AS STRING)) AS STRING) AS `UK Venda`,
    CAST(CASE
        WHEN T.`valor_faturado_bruto` >= 0   AND T.`valor_faturado_bruto` < 25  THEN 'FAIXA 1 - ATÉ R$ 25'
        WHEN T.`valor_faturado_bruto` >= 25  AND T.`valor_faturado_bruto` < 50  THEN 'FAIXA 2 - ENTRE R$ 25 E R$ 50'
        WHEN T.`valor_faturado_bruto` >= 50  AND T.`valor_faturado_bruto` < 75  THEN 'FAIXA 3 - ENTRE R$ 50 E R$ 75'
        WHEN T.`valor_faturado_bruto` >= 75  AND T.`valor_faturado_bruto` < 100 THEN 'FAIXA 4 - ENTRE R$ 75 E R$ 100'
        WHEN T.`valor_faturado_bruto` >= 100 AND T.`valor_faturado_bruto` < 250 THEN 'FAIXA 5 - ENTRE R$ 100 E R$ 250'
        WHEN T.`valor_faturado_bruto` >= 250 AND T.`valor_faturado_bruto` < 500 THEN 'FAIXA 6 - ENTRE R$ 250 E R$ 500'
        WHEN T.`valor_faturado_bruto` >= 500 THEN 'FAIXA 7 - ACIMA DE R$ 500'
    END AS STRING) AS `Faixa de Valor`,
    CAST(TRUNC(CAST(T.`data` AS DATE), 'MM') AS DATE) AS `Mes de Referencia`,
    CAST(CONCAT_WS('||', 'TRINKS', CAST(T.`codigo_do_cliente` AS STRING)) AS STRING) AS `FK Cliente`,
    CAST(CONCAT_WS('||', 'TRINKS', CAST(T.`codigo_do_profissional` AS STRING)) AS STRING) AS `FK Vendedores`,
    CAST(T.`codigo_da_empresa` AS STRING) AS `FK Empresas`,
    CAST(CONCAT_WS('||', 'TRINKS', CAST(T.`codigo_do_servico` AS STRING)) AS STRING) AS `FK Produtos`,
    CAST('TRINKS' AS STRING) AS `Sistema Origem`,
    CAST('PRESTAÇÃO SERVIÇOS' AS STRING) AS `Operacao`,
    CAST(T.`valor_total_desconto` AS INTEGER) AS `Desconto Total`
FROM belshop_bronze_db.vendas_trinks AS T
 LEFT JOIN cadastro_auxiliar_lojas_sheets AS CAL 
     ON CAST(T.codigo_da_empresa AS STRING) = regexp_replace(CAL.codigo_trinks, '^#', '')