SELECT UPPER(RTRIM(P.CODIGO)) AS `Codigo`, 
UPPER(P.NOME) AS `Nome`, 
UPPER(P.GRUPO) AS `Grupo`,
UPPER(P.SUBGRUPO) AS `Subgrupo`,
UPPER(P.COLECAO) AS `Colecao`,
UPPER(P.FORNECEDOR) AS `Fabricante`,
UPPER(P.MARCA) AS `Marca`,
UPPER(P.LINHA) AS `Linha`,
P.codigo_comprador,
P.ULTIMO_CUSTO AS `Ultimo Custo`,
P.Codigo_barras 
FROM PRODUTOS P
left join usuarios on usuarios.`codigo` = p.codigo_comprador