WITH CLIENTE_ULTIMA_COMPRA AS (
    SELECT 
        UPPER(RTRIM(IDCLIENTE)) AS IDCLIENTE_KEY,
        DATA_ULTIMA_COMPRA,
        NOME_LOJA,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(RTRIM(IDCLIENTE)) 
            ORDER BY DATA_ULTIMA_COMPRA DESC
        ) as rn
    FROM aux_clientes_ult_compra_linx_principal
)
SELECT 
    UPPER(RTRIM(C.CODIGO)) AS `Codigo`,
    UPPER(C.NOME) AS `Nome`,
    UPPER(C.SEXO) AS `Sexo`,
    UPPER(C.CIDADE) AS `Cidade`,
    UPPER(C.UF) AS `UF`,
    CASE 
        WHEN UPPER(C.PAIS) = 'BRASIL' THEN 'BRAZIL'
        ELSE C.PAIS
    END AS `Pais`,
    C.DDD_FIXO AS `DDD`,
    C.FONE AS `Fone`,
    C.DDD_CELULAR AS `DDD Celular`,
    C.CELULAR AS `Celular`,
    UPPER(C.EMAIL) AS `Email`,
    C.DATA_CADASTRO AS `Data de Cadastro`,
    C.DATA_NASCIMENTO AS `Data de Nascimento`,
    UPPER(C.CODIGO_TIPO) AS `Codigo do Tipo`,
    UPPER(RTRIM(C.FILIAL_CADASTRO)) AS `Filial de Cadastro`,
    
    -- Idade calculada no Spark
    FLOOR(MONTHS_BETWEEN(CURRENT_DATE(), CAST(C.DATA_NASCIMENTO AS DATE)) / 12) AS `Idade`,
    
    -- Campos vindos do Join preparado (evita o erro de subquery)
    AUX_COMPRA.DATA_ULTIMA_COMPRA AS `Data Ultima Compra`,
    AUX_COMPRA.NOME_LOJA AS `Loja Ultima Compra`,

    COALESCE(AUX_FREQ.FREQUENCIA, 0) AS `Frequencia`,
    COALESCE(AUX_VALOR.valor_medio, 0) AS `Valor Medio`

FROM clientes C
LEFT JOIN CLIENTE_ULTIMA_COMPRA AS AUX_COMPRA
    ON UPPER(RTRIM(C.CODIGO)) = AUX_COMPRA.IDCLIENTE_KEY
    AND AUX_COMPRA.rn = 1

LEFT JOIN aux_clientes_frequencia_linx_principal AS AUX_FREQ
    ON UPPER(RTRIM(C.CODIGO)) = UPPER(RTRIM(AUX_FREQ.id_cliente))
    
LEFT JOIN aux_clientes_valor_linx_principal AS AUX_VALOR
    ON UPPER(RTRIM(C.CODIGO)) = UPPER(RTRIM(AUX_VALOR.id_cliente))