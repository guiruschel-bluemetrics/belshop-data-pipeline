SELECT 
    CONCAT('MICROVIX', '||', V1.`Codigo`) AS `PK Vendedores`, 
    'MICROVIX' AS `Sistema Origem`, 
    V1.`Codigo`, 
    V1.`Nome`
FROM vendedores_linx_principal AS V1

UNION ALL

SELECT 
    CONCAT('TRINKS', '||', V1.`Codigo`), 
    'TRINKS', 
    V1.`Codigo`, 
    V1.`Nome`
FROM profissionais_trinks AS V1