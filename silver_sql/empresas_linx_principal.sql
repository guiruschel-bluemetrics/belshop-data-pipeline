SELECT UPPER(RTRIM(E.CODIGO_FILIAL)) AS `Codigo da Empresa`,
       UPPER(RTRIM(E.NOME)) AS `Nome`,
       E.DATA_ABERTURA AS `Data de Abertura`,
       E.DATA_FECHAMENTO AS `Data de Fechamento` 
  FROM EMPRESAS E
