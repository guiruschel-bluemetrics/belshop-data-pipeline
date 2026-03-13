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
       Upper(Rtrim(IT.codigo_produto)) AS `Codigo do Produto`,
       0                               AS `Quantidade Venda Itens`,
       0                               AS `Valor da Venda Itens`,
       0                               AS `Custo da Venda Itens`,
       IT.quantidade_troca_itens       AS `Quantidade Troca Itens`,
       IT.valor_troca_itens            AS `Valor de Troca Itens`,
       IT.custo_troca_itens            AS `Custo de Troca Itens`
FROM   vendas V
       JOIN itens_trocas IT
         ON ( IT.codigo_empresa = V.codigo_empresa
              AND IT.codigo_venda = V.codigo_venda
              AND IT.data = V.data_venda)
WHERE  V.data_venda >= To_date('2019-01-01', 'yyyy-MM-dd') and V.data_venda <= To_date('2019-12-31', 'yyyy-MM-dd')  