SELECT E.`codigo do produto` AS IDPRODUTO,
       MAX(E.`data ultima entrada`) DATA_ULTIMA_ENTRADA
  FROM estoques_linx_principal E
GROUP BY E.`codigo do produto`