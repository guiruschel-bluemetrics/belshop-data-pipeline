SELECT
    codigo_produto,
    classe
FROM
    (
        SELECT
            codigo_produto,
            classe,
            ROW_NUMBER() OVER (PARTITION BY codigo_produto ORDER BY data_inventario DESC) as rank
        FROM
            vendas_saldos
    ) AS UltClasse
WHERE 
    rank = 1