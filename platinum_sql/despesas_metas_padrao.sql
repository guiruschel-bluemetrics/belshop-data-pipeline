SELECT 
    CONCAT(
        CAST(dr.codigo_empresa AS STRING), 
        '|', 
        CAST(date_format(add_months(current_date(), 1), 'yyyy-MM-01') AS STRING), 
        '|', 
        CAST(dr.cod_historico AS STRING)
    ) AS UID,
    dr.codigo_empresa AS `Codigo Loja`,
    CAST(date_format(add_months(current_date(), 1), 'yyyy-MM-01') AS DATE) AS Data,
    dr.cod_historico AS Codigo, 
    h.nome,
    h.grupo,
    h.codigo_grupo,
    ROUND(AVG(dr.valor_fatura), 2) AS Meta
FROM despesas_realizadas AS dr
INNER JOIN fin_historicos_sheets AS h ON dr.cod_historico = h.codigo
WHERE 
    dr.data_vencimento >= add_months(current_date(), -6)
    AND h.resp_gerente = 'S'
GROUP BY 
    dr.cod_historico, 
    h.codigo_grupo, 
    h.grupo, 
    h.nome, 
    dr.codigo_empresa