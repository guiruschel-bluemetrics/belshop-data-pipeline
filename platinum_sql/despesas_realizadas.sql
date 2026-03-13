SELECT 
    ca.*,
    CAST(date_format(ca.data_baixa, 'yyyy-MM-01') AS DATE) AS mes_referencia,
    h.codigo AS h_codigo, -- Opcional: incluído para conferência do JOIN
    h.nome AS nome_historico,
    h.grupo AS nome_grupo_historico,
    h.codigo_grupo AS cod_grupo_historico,
    h.resp_gerente
FROM contas_apagar AS ca
LEFT JOIN fin_historicos_sheets AS h 
    ON ca.cod_historico = h.codigo