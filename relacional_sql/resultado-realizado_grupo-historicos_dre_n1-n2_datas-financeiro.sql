SELECT
    -- Resultado_Realizado (Tabela Principal - Platinum)
    R.codigo_empresa,
    R.num_fatura,
    R.data_emissao,
    R.data_vencimento,
    R.data_baixa,
    R.valor,
    R.doc_excluido,
    R.doc_cancelado,
    R.cod_historico,
    R.mes_referencia,
    R.cod_grupo_historico,
    R.nome_grupo_historico,
    R.nome_historico,

    -- fin_grupos_historicos_sheets (Bronze)
    H.nome AS hist_nome_grupo,
    H.codigo AS hist_codigo_grupo,
    H.codigo_dre_nivel_superior AS hist_cod_dre_superior,

    -- fin_grupos_dre_n2_sheets (Bronze)
    N2.nome AS dre_n2_nome,
    N2.codigo_dre AS dre_n2_codigo,
    N2.codigo_dre_nivel_superior AS dre_n2_cod_dre_superior,

    -- fin_grupos_dre_n1_sheets (Bronze)
    N1.nome AS dre_n1_nome,
    N1.codigo_dre AS dre_n1_codigo,

-- datas_financeiro (Platinum)
    DF.data AS fin_data_calendario

FROM belshop_platinum_db.Resultado_Realizado AS R
LEFT JOIN belshop_bronze_db.fin_grupos_historicos_sheets AS H 
    ON CAST(R.cod_grupo_historico AS INT) = H.codigo
LEFT JOIN belshop_bronze_db.fin_grupos_dre_n2_sheets AS N2 
    ON H.codigo_dre_nivel_superior = N2.codigo_dre
LEFT JOIN belshop_bronze_db.fin_grupos_dre_n1_sheets AS N1 
    ON N2.codigo_dre_nivel_superior = N1.codigo_dre
LEFT JOIN belshop_platinum_db.datas_financeiro AS DF
    ON R.mes_referencia = DF.data