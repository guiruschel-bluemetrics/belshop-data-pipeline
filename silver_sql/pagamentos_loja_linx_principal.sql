SELECT 
    P.DATA_LANCAMENTO             AS `Data do Lancamento`,
    P.CODIGO_TERMINAL             AS `Codigo do Terminal`,
    P.CODIGOLANCAMENTO            AS `Codigo do Lancamento`,
    UPPER(RTRIM(P.CODIGO_FILIAL)) AS `Codigo da Filial`,
    P.ADMINISTRADORA              AS `Administradora`,
    P.TIPO_PAGAMENTO              AS `Tipo Pagamento`,
    P.PARCELAS                    AS `Parcelas`,
    P.MODALIDADE_PAGAMENTO        AS `Modalidade Pagamento`,
    P.NOME_FILIAL                 AS `Nome da Filial`,
    SUM(P.VALOR)                  AS `Valor`
FROM PAGAMENTOS P
WHERE CAST(P.DATA_LANCAMENTO AS DATE) BETWEEN TO_DATE('2022-03-01', 'yyyy-MM-dd')
                            AND CURRENT_DATE
GROUP BY 
    P.DATA_LANCAMENTO,
    P.CODIGO_TERMINAL,
    P.CODIGOLANCAMENTO,
    UPPER(RTRIM(P.CODIGO_FILIAL)),
    P.ADMINISTRADORA,
    P.TIPO_PAGAMENTO,
    P.PARCELAS,
    P.MODALIDADE_PAGAMENTO,
    P.NOME_FILIAL;
