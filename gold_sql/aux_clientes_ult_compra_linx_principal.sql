SELECT V.`Codigo do Cliente` AS IDCLIENTE,
       MAX(V.`Data da Venda`) AS DATA_ULTIMA_COMPRA,
       E.Nome AS NOME_LOJA
  FROM vendas_linx_principal V
  LEFT JOIN empresas_linx_principal E ON E.`Codigo da Empresa` = V.`Codigo da Empresa`
WHERE V.`Codigo do Cliente` IS NOT NULL
GROUP BY V.`Codigo do Cliente`, E.Nome