SELECT
    -- Vendas (Tabela Principal)
    V."codigo da empresa",
    V."codigo da venda",
    V."serie da venda",
    V."codigo do cliente",
    V."codigo do vendedor",
    V."data da venda",
    V."valor da venda bruta total",
    V."valor em vale-troca total",
    V."valor da venda liquida total",
    V."quantidade venda bruta total",
    V."quantidade troca total",
    V."quantidade venda liquida total",
    V."custo da venda total",
    V."custo de troca total",
    V.frete,
    V."codigo do produto",
    V."quantidade venda itens",
    V."valor da venda itens",
    V."custo da venda itens",
    V."quantidade troca itens",
    V."valor de troca itens",
    V."custo de troca itens",
    V.hora,
    V."quantidade venda liquida itens",
    V."valor da venda liquida itens",
    V."custo da venda liquida itens",
    V."uk venda",
    V."faixa de valor",
    V."mes de referencia",
    V."fk cliente",
    V."fk vendedores",
    V."fk empresas",
    V."fk produtos",
    V."sistema origem" AS vda_sistema_origem,
    V.operacao,
    V."desconto total",

    -- Vendedores (Dimensão)
    VEN."pk vendedores",
    VEN."sistema origem" AS vend_sistema_origem,
    VEN.codigo AS vend_codigo,
    VEN.nome AS vend_nome,

    -- Empresas (Adicionado)
    E."pk empresas",
    E."codigo da filial",
    E."codigo da loja",
    E.nome AS emp_nome,
    E."nome reduzido" AS emp_nome_reduzido,
    E.cidade AS emp_cidade,
    E.uf AS emp_uf,
    E.pais AS emp_pais,
    E."rede da loja",
    E.latitude,
    E.longitude,
    E.agrupamento,
    E."supervisor loja",
    E."supervisor salao",
    E."area da loja"

FROM belshop_platinum_db.Vendas AS V
LEFT JOIN belshop_platinum_db.Vendedores AS VEN 
    ON V."fk vendedores" = VEN."pk vendedores"
LEFT JOIN belshop_platinum_db.Empresas AS E 
    ON V."fk empresas" = E."pk empresas"