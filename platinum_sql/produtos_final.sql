SELECT 
    CONCAT('MICROVIX', '||', P1.`Codigo`) AS `PK Produtos`, 
    CONCAT('MICROVIX', '||', P1.`Fabricante`) AS `FK Fabricante`, 
    'MICROVIX' AS `Sistema Origem`, 
    P1.`Codigo`, 
    P1.`Nome`, 
    P1.`Grupo`, 
    P1.`Subgrupo`, 
    P1.`Colecao`, 
    P1.`Fabricante`, 
    P1.`Marca`, 
    P1.`Linha`, 
    P1.`Ultimo Custo`, 
    P1.`Data Primeira Venda`, 
    P1.`Dias Desde Primeira Venda`, 
    CONCAT(P1.Codigo, ' - ', P1.Nome) AS `Codigo e Nome`, 
    CASE 
        WHEN TRIM(CP.`Classe`) IS NULL OR TRIM(CP.`Classe`) = '' THEN 'N/A'
        ELSE CP.`Classe` 
    END AS `Classe`, 
    CASE 
        WHEN TRIM(P1.`codigo_comprador`) IS NULL OR TRIM(P1.`codigo_comprador`) = '' THEN '0'
        ELSE P1.`codigo_comprador` 
    END AS `Codigo_Comprador`, 
    CASE 
        WHEN TRIM(U.`usuario`) IS NULL OR TRIM(U.`usuario`) = '' THEN 'N/A'
        ELSE U.`usuario` 
    END AS `Nome_Comprador`,
    P1.`codigo_barras`
FROM produtos_linx_principal AS P1 
LEFT JOIN classe_produto AS CP ON CP.`Codigo_Produto` = P1.`Codigo`
LEFT JOIN usuarios AS U ON U.`codigo` = P1.`codigo_comprador`

UNION ALL

SELECT 
    CONCAT('TRINKS', '||', T.`Codigo`), 
    CONCAT('TRINKS', '||', 'GENERICO'), 
    'TRINKS', 
    T.`Codigo`, 
    T.`Descricao`, 
    T.`Grupo`, 
    T.`Subgrupo`, 
    '', 
    '', 
    '', 
    '', 
    0, 
    CAST(NULL AS TIMESTAMP), 
    0, 
    '', 
    'N/A' AS `Classe`, 
    '0' AS `Codigo_Comprador`, 
    'N/A' AS `Nome_Comprador`, 
    '0' AS `codigo_barras`
FROM servicos_trinks AS T --produtos_trinks no sisense