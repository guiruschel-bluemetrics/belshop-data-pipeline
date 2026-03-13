SELECT 
    V.`Codigo da Empresa` AS id_filial, 
    MAX(V.`Data da Venda`) AS data_ultima_venda
FROM 
    vendas_linx_principal V
GROUP BY 
    V.`Codigo da Empresa`
