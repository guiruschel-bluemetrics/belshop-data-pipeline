SELECT 
    CAST(L.`data do lancamento` AS DATE) AS data_do_lancamento,
    L.`codigo do terminal`,
    L.`codigo do lancamento`,
    L.`codigo da filial`,
    L.administradora,
    L.`tipo pagamento`,
    L.parcelas,
    L.`modalidade pagamento`,
    L.`nome da filial`,
    CAST(L.valor AS DECIMAL(15,2)) AS valor, 
    '' AS `SK Cluster Pagamento`,
    '' AS `Modalidade Pagamento CST`,
    '' AS `Nome da Filial CST`
FROM pagamentos_loja_linx_principal AS L

UNION ALL

SELECT 
    CAST(T.data_do_lancamento AS DATE) AS data_do_lancamento,
    T.codigo_do_terminal,
    T.codigo_do_lancamento,
    T.codigo_da_filial,
    T.administradora,
    T.tipo_pagamento,
    T.parcelas,
    T.modalidade_pagamento,
    '' AS `Nome da Filial`, 
    CAST(T.valor AS DECIMAL(15,2)) AS valor,
    '' AS `SK Cluster Pagamento`,
    '' AS `Modalidade Pagamento CST`,
    '' AS `Nome da Filial CST`
FROM pagamentos_trinks AS T