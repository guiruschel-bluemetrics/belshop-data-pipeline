SELECT DISTINCT 
    t.Data AS Data,
    date_add(t.Data, -364) AS `Data Ano Anterior`,
    CASE 
        WHEN month(t.Data) < month(current_date()) THEN 'Sim'
        WHEN month(t.Data) = month(current_date()) 
             AND day(t.Data) < day(current_date()) THEN 'Sim'
        ELSE 'Não' 
    END AS YTD,
    CASE 
        WHEN month(t.Data) < month(current_date()) THEN 'Sim'
        WHEN month(t.Data) = month(current_date()) 
             AND day(t.Data) <= day(current_date()) THEN 'Sim'
        ELSE 'Não' 
    END AS YTT,
    CASE 
        WHEN dayofweek(t.Data) = 1 THEN '01-Segunda-Feira'
        WHEN dayofweek(t.Data) = 2 THEN '02-Terça-Feira'
        WHEN dayofweek(t.Data) = 3 THEN '03-Quarta-Feira'
        WHEN dayofweek(t.Data) = 4 THEN '04-Quinta-Feira'
        WHEN dayofweek(t.Data) = 5 THEN '05-Sexta-Feira'
        WHEN dayofweek(t.Data) = 6 THEN '06-Sábado'
        WHEN dayofweek(t.Data) = 7 THEN '07-Domingo'
    END AS `Dia da Semana`
FROM (
    SELECT CAST(`Data da Venda` AS DATE) AS Data
    FROM vendas
    
    UNION
    
    SELECT CAST(`Data da Venda` AS DATE)
    FROM vendas_5_anos_linx_principal
    
    UNION
    
    SELECT current_date()
) AS t