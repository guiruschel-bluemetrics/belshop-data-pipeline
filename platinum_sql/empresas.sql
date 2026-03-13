SELECT 
    regexp_replace(E1.codigo_microvix, '^#', '') AS `PK Empresas`, 
    regexp_replace(E1.codigo_microvix, '^#', '') AS `Codigo da Filial`, 
    E1.codigo_microvix AS `Codigo da Loja`, 
    E1.nome_comum AS `Nome`, 
    E1.nome_comum AS `Nome Reduzido`, 
    E1.cidade AS `Cidade`, 
    E1.`UF` AS `UF`, 
    'BRAZIL' AS `Pais`, 
    E1.`Rede` AS `Rede da Loja`, 
    E1.`Latitude` AS `Latitude`, 
    E1.`Longitude` AS `Longitude`, 
    E1.`Agrupamento` AS `Agrupamento`, 
    E1.supervisor_loja AS `Supervisor Loja`, 
    E1.supervisor_salao AS `Supervisor Salao`, 
    E1.area_da_loja AS `Area da Loja`
FROM cadastro_auxiliar_lojas_sheets AS E1