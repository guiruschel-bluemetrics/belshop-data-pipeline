SELECT 
    CONCAT('MICROVIX', '||', C1.Codigo) AS `PK Clientes`
    , 'MICROVIX' AS `Sistema Origem`
    , C1.Codigo
    , C1.Nome
    , C1.Sexo
    , C1.Cidade
    , C1.UF
    , C1.Pais
    , C1.DDD
    , C1.Fone
    , C1.`DDD Celular`
    , C1.Celular
    , C1.Email
    , C1.`Data de Cadastro`
    , C1.`Data de Nascimento`
    , C1.`Codigo do Tipo`
    , C1.`Filial de Cadastro`
    , INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) AS Idade
    , CASE 
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 0 AND 18 THEN '0-18 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 19 AND 25 THEN '19-25 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 26 AND 35 THEN '26-35 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 36 AND 45 THEN '36-45 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 46 AND 55 THEN '46-55 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) BETWEEN 56 AND 65 THEN '56-65 ANOS'
        WHEN INT(MONTHS_BETWEEN(CURRENT_DATE(), C1.`Data de Nascimento`) / 12) > 65 THEN '65+ ANOS'
        ELSE 'NÃO INFORMADO'
    END AS `Faixa Etaria`
    , C1.`Data Ultima Compra`
    , C1.`Loja Ultima Compra`
    , DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) AS Recencia
    , C1.Frequencia
    , CASE 
        WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 0 AND 30 THEN 'R1 (0-30 DIAS)'
        WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 31 AND 60 THEN 'R2 (31-60 DIAS)'
        WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 61 AND 120 THEN 'R3 (61-120 DIAS)'
        WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 121 AND 180 THEN 'R4 (121-180 DIAS)'
        WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 181 AND 365 THEN 'R5 (181-365 DIAS)'
        ELSE 'R6 (FORA DO PERÍODO)'
    END AS `Faixa de Recencia`
    , CASE 
        WHEN C1.Frequencia = 1 THEN 'F1 (1x/ANO)'
        WHEN C1.Frequencia = 2 THEN 'F2 (2x/ANO)'
        WHEN C1.Frequencia = 3 THEN 'F3 (3x/ANO)'
        WHEN C1.Frequencia = 4 THEN 'F4 (4x/ANO)'
        WHEN C1.Frequencia = 5 THEN 'F5 (5x/ANO)'
        WHEN C1.Frequencia = 6 THEN 'F6 (6x/ANO)'
        WHEN C1.Frequencia BETWEEN 7 AND 9 THEN 'F7 (7-9x/ANO)'
        WHEN C1.Frequencia BETWEEN 10 AND 12 THEN 'F8 (10-12x/ANO)'
        WHEN C1.Frequencia > 12 THEN 'F9 (>12x/ANO)'
        ELSE 'F0 (0x/ANO)'
    END AS `Faixa de Frequencia`
    , C1.`Valor Medio`
    , CASE WHEN C1.`Codigo do Tipo` = 'TRUE' THEN 'PESSOA JURIDICA' ELSE 'PESSOA FISICA' END AS Tipo
    , CASE WHEN C1.Sexo = 'M' THEN 'MASCULINO' WHEN C1.Sexo = 'F' THEN 'FEMININO' ELSE 'NÃO INFORMADO' END AS Genero
    , CASE WHEN DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) BETWEEN 0 AND 365 THEN 'ATIVO' ELSE 'INATIVO' END AS Atividade
    -- Cálculo de Score simplificado sem os COALESCE repetidos
    , (1.0 / (DATEDIFF(CURRENT_DATE(), C1.`Data Ultima Compra`) + 1.0)) 
        * CAST(C1.Frequencia AS DOUBLE) 
        * CAST(C1.`Valor Medio` AS DOUBLE) AS `Score RFV`
    , MONTH(C1.`Data de Nascimento`) AS `Mes de Aniversario`
    , DAY(C1.`Data de Nascimento`) AS `Dia de Aniversario`
    , CASE WHEN INSTR(UPPER(TRIM(C1.Email)), '@') > 0 AND INSTR(UPPER(TRIM(C1.Email)), '.') > 0 AND INSTR(UPPER(TRIM(C1.Email)), 'BELSHOP') = 0 THEN 'Sim' ELSE 'Não' END AS `Email Informado`
    , CASE WHEN (LENGTH(REGEXP_REPLACE(TRIM(C1.Fone), '-', '')) IN (8, 9)) OR (LENGTH(REGEXP_REPLACE(TRIM(C1.Celular), '-', '')) IN (8, 9)) THEN 'Sim' ELSE 'Não' END AS `Fone Informado`
    , CASE WHEN (LENGTH(REGEXP_REPLACE(TRIM(C1.Fone), '-', '')) IN (8, 9) AND LEFT(TRIM(C1.Fone), 1) IN ('9','8','7')) 
             OR (LENGTH(REGEXP_REPLACE(TRIM(C1.Celular), '-', '')) IN (8, 9) AND LEFT(TRIM(C1.Celular), 1) IN ('9','8','7')) THEN 'Sim' ELSE 'Não' END AS `Celular Informado`
    , CASE WHEN INSTR(TRIM(C1.Nome), ' ') > 0 AND LENGTH(SPLIT(TRIM(C1.Nome), ' ')[0]) > 1 AND LENGTH(SPLIT(TRIM(C1.Nome), ' ')[1]) > 1 THEN 'Sim' ELSE 'Não' END AS `Nome e Sobrenome Informado`
    , CASE WHEN LENGTH(TRIM(C1.Cidade)) >= 3 AND LENGTH(TRIM(C1.UF)) >= 2 THEN 'Sim' ELSE 'Não' END AS `Endereco Informado`

FROM clientes_linx_principal C1

UNION ALL

SELECT 
    CONCAT('TRINKS', '||', C2.Codigo) AS `PK Clientes`
    , 'TRINKS' AS `Sistema Origem`
    , C2.Codigo
    , C2.Nome
    , C2.Sexo
    , C2.Cidade
    , C2.UF
    , C2.Pais
    , C2.DDD
    , C2.Fone
    , CAST(C2.ddd_celular AS STRING)
    , CAST(C2.celulares AS STRING)
    , C2.Email
    , C2.Data_de_Cadastro
    , C2.Data_de_Nascimento
    , ' ' -- Codigo do Tipo
    , C2.filial_de_cadastro
    , 0   -- Idade
    , ' ' -- Faixa Etaria
    , CAST(NULL AS TIMESTAMP) -- Data Ultima Compra
    , ' ' -- Loja Ultima Compra
    , 0   -- Recencia
    , 0   -- Frequencia
    , ' ' -- Faixa de Recencia
    , ' ' -- Faixa de Frequencia
    , 0   -- Valor Medio
    , ' ' -- Tipo
    , ' ' -- Genero
    , ' ' -- Atividade
    , 0.0 -- Score RFV
    , 0   -- Mes de Aniversario
    , 0   -- Dia de Aniversario
    , ' ' -- Email Informado
    , ' ' -- Fone Informado
    , ' ' -- Celular Informado
    , ' ' -- Nome e Sobrenome Informado
    , ' ' -- Endereco Informado
FROM clientes_trinks C2