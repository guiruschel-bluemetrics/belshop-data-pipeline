SELECT 
    d.`codigo_deposito` AS `FK Depositos`, 
    d.`nome_deposito` AS `Deposito`
FROM depositos AS d

UNION

SELECT 
    '0' AS `FK Depositos`, 
    'N/A' AS `Deposito`