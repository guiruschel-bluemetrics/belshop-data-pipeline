SELECT Upper(Rtrim(V.codigo_empresa))  AS `Codigo da Empresa`,
       Upper(Rtrim(V.codigo_venda))    AS `Codigo da Venda`,
       Upper(Rtrim(V.serie))           AS `Serie da Venda`,
       Upper(Rtrim(V.codigo_cliente))  AS `Codigo do Cliente`,
       Upper(Rtrim(V.codigo_vendedor)) AS `Codigo do Vendedor`,
       V.data_venda                    AS `Data da Venda`,
       0                               AS `Valor da Venda Bruta Total`,
       0                               AS `Valor de Troca Total`,
       0                               AS `Valor da Venda Liquida Total`,
       0                               AS `Quantidade Venda Bruta Total`,
       0                               AS `Quantidade Troca Total`,
       0                               AS `Quantidade Venda Liquida Total`,
       0                               AS `Custo da Venda Total`,
       0                               AS `Custo de Troca Total`,
       0                               AS `Frete`,
       Upper(Rtrim(IV.codigo_produto)) AS `Codigo do Produto`,
       IV.quantidade_venda_itens       AS `Quantidade Venda Itens`,
       IV.valor_venda_itens            AS `Valor da Venda Itens`,
       IV.custo_venda_itens            AS `Custo da Venda Itens`,
       0                               AS `Quantidade Troca Itens`,
       0                               AS `Valor de Troca Itens`,
       0                               AS `Custo de Troca Itens`
FROM   vendas V
       JOIN itens_vendas IV
         ON ( IV.codigo_empresa = V.codigo_empresa
              AND IV.codigo_venda = V.codigo_venda
              AND IV.data = V.data_venda)
WHERE  V.data_venda >= To_date('2019-01-01', 'yyyy-MM-dd') AND V.data_venda <= To_date('2019-12-31', 'yyyy-MM-dd')