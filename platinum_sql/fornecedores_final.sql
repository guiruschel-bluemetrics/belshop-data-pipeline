SELECT 
    CONCAT('MICROVIX', '||', F.`Codigo`) AS `PK Fabricante`, 
    'MICROVIX' AS `Sistema Origem`, 
    F.* FROM fornecedores_linx_principal AS F