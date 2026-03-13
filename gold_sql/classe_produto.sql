SELECT 
    codigo_produto, 
    classe
FROM (
    SELECT DISTINCT 
        vs.codigo_produto, 
        vs.classe, 
        vs.data_inventario, 
        DENSE_RANK() OVER (PARTITION BY vs.codigo_produto, vs.classe ORDER BY vs.data_inventario DESC NULLS LAST) AS rank
    FROM vendas_saldos vs
) UltClasse
WHERE rank = 1
  AND data_inventario IS NOT NULL